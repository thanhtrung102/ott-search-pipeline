"""
Iteration 3 — Anomaly detection compute layer.

Resources:
  DynamoDB ott-baseline-stats  : PK genre_hour (derived_genre#hour_of_day_vn)
                                  192 slots (8 genres × 24 h), on-demand, KMS
  DynamoDB ott-anomaly-events  : PK anomaly_id, SK score_ts, TTL 30d
                                  GSI: genre-ts-index (derived_genre, score_ts)
  Lambda anomaly-detector      : Kinesis ESM (batch 100, bisect-on-error),
                                  256 MB, 3 min, Python 3.11
  Lambda baseline-updater      : EventBridge schedule daily 01:00 UTC+7 (18:00 UTC),
                                  256 MB, 5 min, Python 3.11
  EventBridge bus              : ott-search-events (custom bus)
  EventBridge rule             : SearchAnomalyDetected → SNS (+ SQS DLQ)
  SNS topic                    : ott-search-anomaly-alerts (KMS-encrypted)

Cross-stack inputs:
  vpc, lambda_sg  ← NetworkStack
  bucket, dynamodb_key, sns_key  ← StorageStack
  stream  ← IngestionStack
"""
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_sqs as sqs,
    aws_s3 as s3,
)
from constructs import Construct
from stacks import tag_stack


class ComputeStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        alert_email: str = "",
        # Cross-stack inputs
        bucket: s3.IBucket | None = None,
        stream: kinesis.IStream | None = None,
        dynamodb_key=None,
        sns_key=None,
        lambda_sg=None,
        vpc=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name  = bucket.bucket_name if bucket else "PLACEHOLDER"
        bucket_arn   = bucket.bucket_arn  if bucket else "arn:aws:s3:::PLACEHOLDER"

        # ── DynamoDB: ott-baseline-stats ─────────────────────────────────────
        self._baseline_table = dynamodb.Table(
            self, "BaselineTable",
            table_name=f"ott-baseline-stats-{env_name}",
            partition_key=dynamodb.Attribute(
                name="genre_hour",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED
            if dynamodb_key else dynamodb.TableEncryption.AWS_MANAGED,
            encryption_key=dynamodb_key,
            removal_policy=RemovalPolicy.DESTROY,  # data regenerated nightly
        )

        # ── DynamoDB: ott-anomaly-events ─────────────────────────────────────
        self._anomaly_table = dynamodb.Table(
            self, "AnomalyTable",
            table_name=f"ott-anomaly-events-{env_name}",
            partition_key=dynamodb.Attribute(
                name="anomaly_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="score_ts",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED
            if dynamodb_key else dynamodb.TableEncryption.AWS_MANAGED,
            encryption_key=dynamodb_key,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
        )
        # GSI: genre-ts-index for querying anomalies by genre + time
        self._anomaly_table.add_global_secondary_index(
            index_name="genre-ts-index",
            partition_key=dynamodb.Attribute(
                name="derived_genre", type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="score_ts", type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ── SNS alert topic ───────────────────────────────────────────────────
        self._alert_topic = sns.Topic(
            self, "AlertTopic",
            topic_name=f"ott-search-anomaly-alerts-{env_name}",
            master_key=sns_key,
        )
        if alert_email:
            self._alert_topic.add_subscription(
                subscriptions.EmailSubscription(alert_email)
            )

        # ── SQS DLQ for EventBridge target ───────────────────────────────────
        dlq = sqs.Queue(
            self, "AnomalyDlq",
            queue_name=f"ott-anomaly-dlq-{env_name}",
            retention_period=Duration.days(14),
        )

        # ── EventBridge custom bus ────────────────────────────────────────────
        event_bus = events.EventBus(
            self, "SearchEventBus",
            event_bus_name=f"ott-search-events-{env_name}",
        )

        # Rule: SearchAnomalyDetected → SNS topic
        events.Rule(
            self, "AnomalyRule",
            event_bus=event_bus,
            event_pattern=events.EventPattern(
                source=["ott.anomaly-detector"],
                detail_type=["SearchAnomalyDetected"],
            ),
            targets=[
                targets.SnsTopic(
                    self._alert_topic,
                    dead_letter_queue=dlq,
                )
            ],
        )

        # ── IAM role for anomaly-detector Lambda ──────────────────────────────
        anomaly_role = iam.Role(
            self, "AnomalyRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
            inline_policies={
                "anomaly-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["dynamodb:GetItem", "dynamodb:BatchGetItem"],
                            resources=[self._baseline_table.table_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["dynamodb:PutItem"],
                            resources=[self._anomaly_table.table_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["events:PutEvents"],
                            resources=[event_bus.event_bus_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["cloudwatch:PutMetricData"],
                            resources=["*"],
                            conditions={
                                "StringEquals": {
                                    "cloudwatch:namespace": "OTT/SearchPipeline"
                                }
                            },
                        ),
                    ]
                )
            },
        )
        if stream:
            stream.grant_read(anomaly_role)
        if dynamodb_key:
            dynamodb_key.grant_decrypt(anomaly_role)

        # ── Lambda: anomaly-detector ──────────────────────────────────────────
        fn_kwargs: dict = {}
        if vpc and lambda_sg:
            fn_kwargs["vpc"] = vpc
            fn_kwargs["security_groups"] = [lambda_sg]

        self._anomaly_fn = _lambda.Function(
            self, "AnomalyDetector",
            function_name=f"ott-anomaly-detector-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/anomaly_detector"),
            memory_size=256,
            timeout=Duration.minutes(3),
            role=anomaly_role,
            environment={
                "BASELINE_TABLE": self._baseline_table.table_name,
                "ANOMALY_TABLE":  self._anomaly_table.table_name,
                "EVENT_BUS_NAME": event_bus.event_bus_name,
                "CW_NAMESPACE":   "OTT/SearchPipeline",
            },
            **fn_kwargs,
        )

        # Kinesis event-source mapping (bisect on error for safe retries)
        if stream:
            self._anomaly_fn.add_event_source(
                event_sources.KinesisEventSource(
                    stream,
                    starting_position=_lambda.StartingPosition.TRIM_HORIZON,
                    batch_size=100,
                    bisect_batch_on_error=True,
                    retry_attempts=3,
                )
            )

        # ── IAM role for baseline-updater Lambda ──────────────────────────────
        updater_role = iam.Role(
            self, "UpdaterRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
            inline_policies={
                "updater-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "athena:StartQueryExecution",
                                "athena:GetQueryExecution",
                                "athena:GetQueryResults",
                            ],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket",
                                     "s3:GetBucketLocation"],
                            resources=[bucket_arn, f"{bucket_arn}/*"],
                        ),
                        iam.PolicyStatement(
                            actions=["glue:GetTable", "glue:GetDatabase",
                                     "glue:GetPartitions"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            actions=["dynamodb:BatchWriteItem"],
                            resources=[self._baseline_table.table_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["cloudwatch:PutMetricData"],
                            resources=["*"],
                        ),
                        # Required when invoked via Step Functions waitForTaskToken
                        iam.PolicyStatement(
                            actions=[
                                "states:SendTaskSuccess",
                                "states:SendTaskFailure",
                            ],
                            resources=["*"],
                        ),
                    ]
                )
            },
        )
        if dynamodb_key:
            dynamodb_key.grant_encrypt_decrypt(updater_role)

        # ── Lambda: baseline-updater ──────────────────────────────────────────
        self._updater_fn = _lambda.Function(
            self, "BaselineUpdater",
            function_name=f"ott-baseline-updater-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/baseline_updater"),
            memory_size=256,
            timeout=Duration.minutes(5),
            role=updater_role,
            environment={
                "BASELINE_TABLE":   self._baseline_table.table_name,
                "ATHENA_WORKGROUP": f"ott-analytics-{env_name}",
                "ATHENA_OUTPUT":    f"s3://{bucket_name}/athena-results/",
                "CURATED_DB":       "ott_search_curated",
                "CURATED_TABLE":    "search_enriched",
            },
        )

        # EventBridge schedule: daily 01:00 UTC+7 = 18:00 UTC
        events.Rule(
            self, "BaselineSchedule",
            schedule=events.Schedule.cron(hour="18", minute="0"),
            targets=[targets.LambdaFunction(self._updater_fn)],
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def baseline_table(self) -> dynamodb.Table:
        return self._baseline_table

    @property
    def anomaly_table(self) -> dynamodb.Table:
        return self._anomaly_table

    @property
    def alert_topic(self) -> sns.Topic:
        return self._alert_topic

    @property
    def alert_topic_arn(self) -> str:
        return self._alert_topic.topic_arn

    @property
    def anomaly_detector_name(self) -> str:
        return self._anomaly_fn.function_name

    @property
    def baseline_updater_arn(self) -> str:
        return self._updater_fn.function_arn
