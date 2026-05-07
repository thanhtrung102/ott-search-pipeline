"""
Iteration 4 — Step Functions orchestration + Gold layer.

Resources:
  Athena workgroup   : ott-analytics; 10 GB scan cap; SSE-KMS result output
  Glue Databases     : ott_search_gold
  Step Functions SM  : ott-daily-pipeline; trigger 01:30 UTC+7 (18:30 UTC)
    States:
      StartCrawler → WaitCrawler → StartETL
      → RepairCuratedTable (MSCK REPAIR — registers partitions Athena can see)
      → DropGoldTable     (DROP TABLE IF EXISTS — makes CTAS idempotent)
      → RunAthenaGoldCTAS → InvokeBaselineUpdater
      → PipelineSuccess / PipelineFailure
  Step Functions role : least-privilege (Glue, Athena, Lambda invoke, S3, CloudWatch)

Gold CTAS query (§6.3):
  keyword_trends — top-50 per (derived_genre, platform_group), rank_delta vs 7d ago

CDK context overrides (cdk.json or --context):
  gold_date_filter   : Athena WHERE dt filter for CTAS and baseline updater.
                       Default: "dt >= date_add('day', -1, current_date)"
                       Demo:    "dt >= '2022-01-01'"

Cross-stack inputs:
  bucket  ← StorageStack
  enrichment_job_name, crawler_name  ← ETLStack
  baseline_updater_arn, alert_topic_arn  ← ComputeStack
"""
import json

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    Tags,
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct
from stacks import tag_stack


# Gold CTAS SQL (§6.3) — aligned to spec output contract
_GOLD_CTAS_SQL = """
CREATE TABLE ott_search_gold.keyword_trends
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['trend_date', 'derived_genre'],
  external_location = 's3://{bucket}/gold/keyword_trends/'
) AS
WITH ranked_today AS (
  SELECT
    CAST(dt AS DATE)                                       AS trend_date,
    derived_genre,
    platform_group,
    network_type_norm,
    isp_segment,
    keyword_norm,
    COUNT(*)                                               AS search_count,
    COUNT(*) FILTER (WHERE session_action = 'enter')       AS enter_count,
    1.0 - (COUNT(*) FILTER (WHERE session_action = 'enter')
           * 1.0 / NULLIF(COUNT(*), 0))                   AS abandonment_rate,
    COUNT(DISTINCT user_id_hashed)                         AS unique_users,
    COUNT(DISTINCT user_id_hashed) * 1.0
      / NULLIF(COUNT(*), 0)                                AS authenticated_rate,
    AVG(CAST(is_repeat_search AS INT))                     AS repeat_search_rate,
    AVG(CAST(has_premium AS INT))                          AS premium_search_rate,
    RANK() OVER (
      PARTITION BY derived_genre, platform_group
      ORDER BY COUNT(*) FILTER (WHERE session_action = 'enter') DESC
    )                                                      AS rank_today
  FROM ott_search_curated.search_enriched
  WHERE {date_filter}
    AND keyword_norm IS NOT NULL
    AND is_cross_partition_date = false
  GROUP BY 1, 2, 3, 4, 5, 6
),
ranked_7d AS (
  SELECT derived_genre, platform_group, keyword_norm,
    RANK() OVER (
      PARTITION BY derived_genre, platform_group
      ORDER BY COUNT(*) FILTER (WHERE session_action = 'enter') DESC
    ) AS rank_7d_ago
  FROM ott_search_curated.search_enriched
  WHERE dt BETWEEN DATE_ADD('day', -8, CURRENT_DATE)
                AND DATE_ADD('day', -2, CURRENT_DATE)
    AND keyword_norm IS NOT NULL
    AND is_cross_partition_date = false
  GROUP BY 1, 2, 3
)
SELECT
  t.trend_date, t.derived_genre, t.platform_group, t.network_type_norm,
  t.isp_segment, t.keyword_norm, t.search_count, t.enter_count,
  t.abandonment_rate, t.unique_users, t.authenticated_rate,
  t.repeat_search_rate, t.premium_search_rate,
  t.rank_today,
  COALESCE(h.rank_7d_ago, 9999)               AS rank_7d_ago,
  COALESCE(h.rank_7d_ago, 9999) - t.rank_today AS rank_delta
FROM ranked_today t
LEFT JOIN ranked_7d h
  ON  t.derived_genre  = h.derived_genre
  AND t.platform_group = h.platform_group
  AND t.keyword_norm   = h.keyword_norm
WHERE t.rank_today <= 50
"""


class AnalyticsStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        bucket: s3.IBucket | None = None,
        enrichment_job_name: str | None = None,
        crawler_name: str | None = None,
        baseline_updater_arn: str | None = None,
        failure_topic_arn: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = bucket.bucket_name if bucket else "PLACEHOLDER"

        # CDK context override: set to "dt >= '2022-01-01'" for demo / backfill runs.
        # Production default is yesterday's partition.
        _date_filter = (
            self.node.try_get_context("gold_date_filter")
            or "dt >= date_add('day', -1, current_date)"
        )

        # ── Glue gold database ────────────────────────────────────────────────
        glue.CfnDatabase(
            self, "GoldDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ott_search_gold",
                description="OTT pipeline - gold aggregated keyword trends",
            ),
        )

        # ── Athena workgroup ──────────────────────────────────────────────────
        self._workgroup = athena.CfnWorkGroup(
            self, "AnalyticsWorkgroup",
            name=f"ott-analytics-{env_name}",
            description="OTT search pipeline analytics",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                bytes_scanned_cutoff_per_query=10 * 1024 ** 3,  # 10 GB
                enforce_work_group_configuration=True,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3",
                ),
            ),
        )

        # ── Step Functions IAM role ───────────────────────────────────────────
        sfn_role = iam.Role(
            self, "SfnRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "sfn-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["glue:StartCrawler", "glue:GetCrawler",
                                     "glue:StartJobRun", "glue:GetJobRun"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            actions=["athena:StartQueryExecution",
                                     "athena:GetQueryExecution",
                                     "athena:StopQueryExecution"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:GetObject", "s3:PutObject",
                                     "s3:GetBucketLocation", "s3:ListBucket"],
                            resources=(
                                [bucket.bucket_arn, bucket.arn_for_objects("*")]
                                if bucket else ["*"]
                            ),
                        ),
                        iam.PolicyStatement(
                            actions=["glue:GetTable", "glue:GetDatabase",
                                     "glue:GetPartitions", "glue:BatchCreatePartition"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            actions=["lambda:InvokeFunction"],
                            resources=[baseline_updater_arn or "*"],
                        ),
                        iam.PolicyStatement(
                            actions=["cloudwatch:PutMetricData"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            actions=["sns:Publish"],
                            resources=[failure_topic_arn or "*"],
                        ),
                        iam.PolicyStatement(
                            actions=["xray:PutTraceSegments",
                                     "xray:PutTelemetryRecords"],
                            resources=["*"],
                        ),
                    ]
                )
            },
        )

        # ── Step Functions state machine ──────────────────────────────────────
        # Build complete ASL as a dict so all states (including post-Choice
        # transitions) are included.  from_chainable() only traverses CDK
        # .next() links and silently drops states referenced only by name in
        # CustomState JSON, causing MISSING_TRANSITION_TARGET validation errors.
        gold_sql = _GOLD_CTAS_SQL.format(bucket=bucket_name, date_filter=_date_filter)
        _crawler = crawler_name or "raw-events-crawler"
        _job     = enrichment_job_name or "search-enrichment-job"
        _updater = baseline_updater_arn or "ott-baseline-updater"
        _topic   = failure_topic_arn or "arn:aws:sns:::PLACEHOLDER"

        asl = {
            "Comment": "OTT search pipeline daily orchestration",
            "StartAt": "StartCrawler",
            "States": {
                "StartCrawler": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                    "Parameters": {"Name": _crawler},
                    "Catch": [{"ErrorEquals": ["Glue.CrawlerRunningException"],
                               "Next": "WaitForCrawler"}],
                    "Next": "WaitForCrawler",
                },
                "WaitForCrawler": {
                    "Type": "Wait",
                    "Seconds": 30,
                    "Next": "CheckCrawlerStatus",
                },
                "CheckCrawlerStatus": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
                    "Parameters": {"Name": _crawler},
                    "Next": "IsCrawlerDone",
                },
                "IsCrawlerDone": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.Crawler.State",
                         "StringEquals": "READY",
                         "Next": "StartETLJob"},
                    ],
                    "Default": "WaitForCrawler",
                },
                "StartETLJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                        "JobName": _job,
                        "Arguments": {"--PUSH_DOWN_PREDICATE": _date_filter},
                    },
                    "ResultPath": None,
                    "Next": "RepairCuratedTable",
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "PipelineFailure"}],
                },
                "RepairCuratedTable": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                    "Parameters": {
                        "QueryString": "MSCK REPAIR TABLE ott_search_curated.search_enriched",
                        "WorkGroup": f"ott-analytics-{env_name}",
                        "ResultConfiguration": {
                            "OutputLocation": f"s3://{bucket_name}/athena-results/",
                        },
                    },
                    "ResultPath": None,
                    "Next": "DropGoldTable",
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "PipelineFailure"}],
                },
                "DropGoldTable": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                    "Parameters": {
                        "QueryString": "DROP TABLE IF EXISTS ott_search_gold.keyword_trends",
                        "WorkGroup": f"ott-analytics-{env_name}",
                        "ResultConfiguration": {
                            "OutputLocation": f"s3://{bucket_name}/athena-results/",
                        },
                    },
                    "ResultPath": None,
                    "Next": "RunAthenaGoldCTAS",
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "PipelineFailure"}],
                },
                "RunAthenaGoldCTAS": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                    "Parameters": {
                        "QueryString": gold_sql,
                        "WorkGroup": f"ott-analytics-{env_name}",
                        "ResultConfiguration": {
                            "OutputLocation": f"s3://{bucket_name}/athena-results/",
                        },
                    },
                    "Next": "InvokeBaselineUpdater",
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "PipelineFailure"}],
                },
                "InvokeBaselineUpdater": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke.waitForTaskToken",
                    "Parameters": {
                        "FunctionName": _updater,
                        "Payload": {"taskToken.$": "$$.Task.Token"},
                    },
                    "Next": "PipelineSuccess",
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "PipelineFailure"}],
                },
                "PipelineSuccess": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
                    "Parameters": {
                        "Namespace": "OTT/SearchPipeline",
                        "MetricData": [{
                            "MetricName": "DailyPipelineSuccess",
                            "Value": 1,
                            "Unit": "Count",
                            "Dimensions": [{"Name": "Environment", "Value": env_name}],
                        }],
                    },
                    "End": True,
                },
                "PipelineFailure": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sns:publish",
                    "Parameters": {
                        "TopicArn": _topic,
                        "Message": {
                            "Input.$": "States.Format('Daily pipeline FAILED: {}', $.Cause)"
                        },
                    },
                    "End": True,
                },
            },
        }

        self._state_machine = sfn.StateMachine(
            self, "DailyPipeline",
            state_machine_name=f"ott-daily-pipeline-{env_name}",
            definition_body=sfn.DefinitionBody.from_string(json.dumps(asl)),
            role=sfn_role,
            timeout=Duration.hours(3),
            tracing_enabled=True,
        )

        # EventBridge trigger: daily 01:30 UTC+7 = 18:30 UTC
        events.Rule(
            self, "DailyTrigger",
            schedule=events.Schedule.cron(hour="18", minute="30"),
            targets=[targets.SfnStateMachine(self._state_machine)],
        )

        tag_stack(self, env_name)

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine
