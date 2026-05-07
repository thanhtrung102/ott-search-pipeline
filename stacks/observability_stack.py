"""
Iteration 7 — CloudWatch observability hardening.

Custom metrics (namespace OTT/SearchPipeline) — emitted by Lambda handlers:
  SearchEnterEventsPerMinute  dims: Genre, PlatformGroup
  SearchQuitEventsPerMinute   dims: Genre, PlatformGroup
  AnomalyScore                dims: Genre, Hour
  GlueJobDurationSeconds      dims: JobName
  DailyPipelineSuccess        dims: Environment
  BaselineUpdateLatency       (no dims)

Alarms (→ SNS ott-pipeline-ops):
  anomaly-score-breach    AnomalyScore > 3.0 × 1 period (5 min)
  glue-job-failure        DailyPipelineSuccess < 1 in 26 h window
  lambda-error-rate       Lambda Errors / Invocations > 0.05 over 15 min
  kinesis-iterator-age    GetRecords.IteratorAgeMilliseconds > 60 000 ms

Composite alarm:
  pipeline-health  = OR of the four above

Log Groups (30-day retention unless noted):
  /aws/lambda/ott-anomaly-detector-{env}
  /aws/lambda/ott-baseline-updater-{env}
  /aws/glue/jobs/search-enrichment-job-{env}
  /aws/states/ott-daily-pipeline-{env}
  /ott/vpc-flow-logs/{env}           (90-day retention)

CloudWatch Dashboard: ott-pipeline-health

Cross-stack inputs (all optional — alarms degrade gracefully if absent):
  anomaly_fn_name   ← ComputeStack
  stream_name       ← IngestionStack
"""
from aws_cdk import (
    Duration,
    Stack,
    Tags,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
from constructs import Construct
from stacks import tag_stack

_CW_NS = "OTT/SearchPipeline"


class ObservabilityStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        alert_email: str = "",
        anomaly_fn_name: str | None = None,
        stream_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Ops SNS topic ─────────────────────────────────────────────────────
        ops_topic = sns.Topic(
            self, "OpsTopic",
            topic_name=f"ott-pipeline-ops-{env_name}",
            display_name="OTT Pipeline Operations Alerts",
        )
        if alert_email:
            ops_topic.add_subscription(subscriptions.EmailSubscription(alert_email))

        sns_action = cw_actions.SnsAction(ops_topic)

        # ── Alarms ────────────────────────────────────────────────────────────

        # 1. AnomalyScore > 3.0 (custom metric, 5-min period)
        anomaly_alarm = cloudwatch.Alarm(
            self, "AnomalyScoreBreach",
            alarm_name=f"ott-anomaly-score-breach-{env_name}",
            alarm_description="AnomalyScore exceeded z=3.0 threshold",
            metric=cloudwatch.Metric(
                namespace=_CW_NS,
                metric_name="AnomalyScore",
                statistic="Maximum",
                period=Duration.minutes(5),
            ),
            threshold=3.0,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        anomaly_alarm.add_alarm_action(sns_action)

        # 2. DailyPipelineSuccess < 1 within 26 h window (misses a scheduled run)
        glue_failure_alarm = cloudwatch.Alarm(
            self, "GlueJobFailure",
            alarm_name=f"ott-glue-job-failure-{env_name}",
            alarm_description="Daily pipeline did not report success in the last 26 hours",
            metric=cloudwatch.Metric(
                namespace=_CW_NS,
                metric_name="DailyPipelineSuccess",
                dimensions_map={"Environment": env_name},
                statistic="Sum",
                period=Duration.hours(26),
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
        )
        glue_failure_alarm.add_alarm_action(sns_action)

        # 3. Lambda error rate > 5 % over 15 min (anomaly-detector function)
        fn_name = anomaly_fn_name or f"ott-anomaly-detector-{env_name}"
        errors_metric = cloudwatch.Metric(
            namespace="AWS/Lambda",
            metric_name="Errors",
            dimensions_map={"FunctionName": fn_name},
            statistic="Sum",
            period=Duration.minutes(15),
        )
        invocations_metric = cloudwatch.Metric(
            namespace="AWS/Lambda",
            metric_name="Invocations",
            dimensions_map={"FunctionName": fn_name},
            statistic="Sum",
            period=Duration.minutes(15),
        )
        error_rate_metric = cloudwatch.MathExpression(
            expression="errors / MAX([errors, invocations])",
            using_metrics={"errors": errors_metric, "invocations": invocations_metric},
            period=Duration.minutes(15),
        )
        lambda_error_alarm = cloudwatch.Alarm(
            self, "LambdaErrorRate",
            alarm_name=f"ott-lambda-error-rate-{env_name}",
            alarm_description="Anomaly-detector Lambda error rate > 5 % over 15 min",
            metric=error_rate_metric,
            threshold=0.05,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        lambda_error_alarm.add_alarm_action(sns_action)

        # 4. Kinesis iterator age > 60 s (consumer lagging behind)
        stream = stream_name or f"ott-search-stream-{env_name}"
        kinesis_alarm = cloudwatch.Alarm(
            self, "KinesisIteratorAge",
            alarm_name=f"ott-kinesis-iterator-age-{env_name}",
            alarm_description="Kinesis consumer iterator age > 60 000 ms",
            metric=cloudwatch.Metric(
                namespace="AWS/Kinesis",
                metric_name="GetRecords.IteratorAgeMilliseconds",
                dimensions_map={"StreamName": stream},
                statistic="Maximum",
                period=Duration.minutes(5),
            ),
            threshold=60_000,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        kinesis_alarm.add_alarm_action(sns_action)

        # ── Composite alarm ───────────────────────────────────────────────────
        composite = cloudwatch.CompositeAlarm(
            self, "PipelineHealth",
            composite_alarm_name=f"ott-pipeline-health-{env_name}",
            alarm_description="OR of all four OTT pipeline health alarms",
            alarm_rule=cloudwatch.AlarmRule.any_of(
                anomaly_alarm,
                glue_failure_alarm,
                lambda_error_alarm,
                kinesis_alarm,
            ),
        )
        composite.add_alarm_action(sns_action)

        # ── CloudWatch Dashboard ──────────────────────────────────────────────
        dashboard = cloudwatch.Dashboard(
            self, "PipelineDashboard",
            dashboard_name=f"ott-pipeline-health-{env_name}",
        )
        dashboard.add_widgets(
            cloudwatch.TextWidget(
                markdown=f"## OTT Search Pipeline — {env_name}",
                width=24,
            ),
        )
        dashboard.add_widgets(
            cloudwatch.AlarmWidget(
                alarm=anomaly_alarm,
                title="Anomaly Score Breach",
            ),
            cloudwatch.AlarmWidget(
                alarm=glue_failure_alarm,
                title="Daily Pipeline Success",
            ),
            cloudwatch.AlarmWidget(
                alarm=lambda_error_alarm,
                title="Lambda Error Rate",
            ),
            cloudwatch.AlarmWidget(
                alarm=kinesis_alarm,
                title="Kinesis Iterator Age",
            ),
        )
        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Search Enter Events per Minute",
                left=[
                    cloudwatch.Metric(
                        namespace=_CW_NS,
                        metric_name="SearchEnterEventsPerMinute",
                        statistic="Sum",
                        period=Duration.minutes(5),
                        color=cloudwatch.Color.GREEN,
                    ),
                    cloudwatch.Metric(
                        namespace=_CW_NS,
                        metric_name="SearchQuitEventsPerMinute",
                        statistic="Sum",
                        period=Duration.minutes(5),
                        color=cloudwatch.Color.RED,
                    ),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="AnomalyScore (max)",
                left=[cloudwatch.Metric(
                    namespace=_CW_NS,
                    metric_name="AnomalyScore",
                    statistic="Maximum",
                    period=Duration.minutes(5),
                    color=cloudwatch.Color.ORANGE,
                )],
                left_y_axis=cloudwatch.YAxisProps(min=0, label="z-score"),
                width=12,
            ),
        )

        # ── Log Groups with retention ─────────────────────────────────────────
        _30d = logs.RetentionDays.ONE_MONTH
        _90d = logs.RetentionDays.THREE_MONTHS

        for name in [
            f"/aws/lambda/ott-anomaly-detector-{env_name}",
            f"/aws/lambda/ott-baseline-updater-{env_name}",
            f"/aws/glue/jobs/search-enrichment-job-{env_name}",
            f"/aws/states/ott-daily-pipeline-{env_name}",
        ]:
            logs.LogGroup(
                self, f"LG{name.replace('/', '').replace('-', '').replace('.', '')}",
                log_group_name=name,
                retention=_30d,
            )

        logs.LogGroup(
            self, "VpcFlowLogGroup",
            log_group_name=f"/ott/vpc-flow-logs/{env_name}",
            retention=_90d,
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def ops_topic(self) -> sns.Topic:
        return self.node.find_child("OpsTopic")  # type: ignore[return-value]
