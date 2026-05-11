"""
Iteration 1 — Ingest layer.

Resources:
  Kinesis Data Streams  : 2 shards; partition key = category_norm (8 values);
                          SSE with aws/kinesis managed key; 24h retention
  Kinesis Firehose      : source=KDS; dest=S3; JSON→Parquet conversion;
                          dynamic partitioning on dt/hour/category_norm/platform;
                          64 MB / 60 s buffer; SNAPPY compression; SSE-KMS (ott-kinesis-key)
  Glue Database         : ott_search_raw
  Glue Table            : raw_events — schema includes category_norm + platform_group
                          (enriched fields added by replay-producer before publish)
  replay-producer Lambda: Python 3.11, 512 MB, 15 min; reads Parquet from S3,
                          enriches each row (datetime fix, genre classify, platform bucket),
                          batch-publishes to Kinesis (500 rec/call, ≤1500 rec/s)

Cross-stack inputs wired in app.py Iteration 1 update:
  vpc, lambda_sg  ← NetworkStack
  bucket, s3_key, kinesis_key  ← StorageStack
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct
from stacks import tag_stack


class IngestionStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        # Cross-stack inputs — wired from app.py after Iteration 0
        bucket: s3.IBucket | None = None,
        kinesis_key=None,        # kms.IKey — optional at skeleton stage
        s3_key=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._env_name = env_name

        # ── Kinesis Data Streams ──────────────────────────────────────────────
        encryption = (
            kinesis.StreamEncryption.KMS
            if kinesis_key
            else kinesis.StreamEncryption.MANAGED
        )
        self._stream = kinesis.Stream(
            self, "SearchStream",
            stream_name=f"ott-search-stream-{env_name}",
            shard_count=2,
            retention_period=Duration.hours(24),
            encryption=encryption,
            encryption_key=kinesis_key,
        )

        # ── Glue Database + Table (raw_events) ───────────────────────────────
        raw_db = glue.CfnDatabase(
            self, "RawDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ott_search_raw",
                description="OTT pipeline - raw Parquet events from Firehose",
            ),
        )

        # Schema for the raw Parquet table as produced by replay-producer.
        # Firehose converts JSON→Parquet using this table definition.
        _str = glue.CfnTable.ColumnProperty
        _raw_columns = [
            _str(name="eventid",        type="string"),
            _str(name="datetime",       type="string"),
            _str(name="user_id",        type="string"),
            _str(name="keyword",        type="string"),
            _str(name="category",       type="string"),
            _str(name="proxy_isp",      type="string"),
            _str(name="platform",       type="string"),
            _str(name="networktype",    type="string"),
            _str(name="action",         type="string"),
            _str(name="userplansmap",   type="array<string>"),
            # Enriched by replay-producer before publishing to stream
            _str(name="derived_genre",  type="string"),
            _str(name="platform_group", type="string"),
        ]
        _raw_partitions = [
            _str(name="dt",             type="string"),
            _str(name="hour",           type="string"),
            _str(name="derived_genre",  type="string"),
            _str(name="platform_group", type="string"),
        ]

        raw_table = glue.CfnTable(
            self, "RawEventsTable",
            catalog_id=self.account,
            database_name="ott_search_raw",
            table_input=glue.CfnTable.TableInputProperty(
                name="events",
                description="Raw search events - Parquet, SNAPPY",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=_raw_columns,
                    location=(
                        f"s3://{bucket.bucket_name}/raw/events/"
                        if bucket else "s3://PLACEHOLDER/raw/events/"
                    ),
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=(
                            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        ),
                        parameters={"serialization.format": "1"},
                    ),
                    compressed=True,
                ),
                partition_keys=_raw_partitions,
                parameters={
                    "classification": "parquet",
                    "compressionType": "snappy",
                },
            ),
        )
        raw_table.add_dependency(raw_db)

        # ── Kinesis Firehose ──────────────────────────────────────────────────
        # IAM role for Firehose → S3 + Glue + Kinesis read
        firehose_role = iam.Role(
            self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            inline_policies={
                "firehose-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "kinesis:GetRecords", "kinesis:GetShardIterator",
                                "kinesis:DescribeStream", "kinesis:ListShards",
                                "kinesis:SubscribeToShard",
                            ],
                            resources=[self._stream.stream_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload",
                                     "s3:ListBucketMultipartUploads", "s3:ListBucket",
                                     "s3:GetBucketLocation"],
                            resources=(
                                [bucket.bucket_arn, bucket.arn_for_objects("*")]
                                if bucket
                                else ["arn:aws:s3:::PLACEHOLDER", "arn:aws:s3:::PLACEHOLDER/*"]
                            ),
                        ),
                        iam.PolicyStatement(
                            actions=["glue:GetTable", "glue:GetTableVersion",
                                     "glue:GetTableVersions"],
                            resources=["*"],
                        ),
                    ]
                )
            },
        )
        if kinesis_key:
            kinesis_key.grant_decrypt(firehose_role)
        if s3_key:
            s3_key.grant_encrypt_decrypt(firehose_role)

        bucket_arn = bucket.bucket_arn if bucket else "arn:aws:s3:::PLACEHOLDER"
        bucket_name = bucket.bucket_name if bucket else "PLACEHOLDER"

        # Firehose with dynamic partitioning — JSON→Parquet via Glue schema
        self._delivery_stream = firehose.CfnDeliveryStream(
            self, "FirehoseStream",
            delivery_stream_name=f"ott-search-firehose-{env_name}",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self._stream.stream_arn,
                role_arn=firehose_role.role_arn,
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix=(
                    "raw/events/"
                    "dt=!{partitionKeyFromQuery:dt}/"
                    "hour=!{partitionKeyFromQuery:hour}/"
                    "derived_genre=!{partitionKeyFromQuery:derived_genre}/"
                    "platform_group=!{partitionKeyFromQuery:platform_group}/"
                ),
                error_output_prefix=(
                    "raw/errors/!{firehose:error-output-type}/"
                    "dt=!{timestamp:yyyy-MM-dd}/"
                ),
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    size_in_m_bs=64,
                    interval_in_seconds=60,
                ),
                compression_format="UNCOMPRESSED",  # Parquet conversion handles compression
                data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    enabled=True,
                    input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                        deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                            open_x_json_ser_de=firehose.CfnDeliveryStream.OpenXJsonSerDeProperty(),
                        )
                    ),
                    output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                        serializer=firehose.CfnDeliveryStream.SerializerProperty(
                            parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                                compression="SNAPPY",
                            )
                        )
                    ),
                    schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                        catalog_id=self.account,
                        database_name="ott_search_raw",
                        table_name="events",
                        region=self.region,
                        version_id="LATEST",
                        role_arn=firehose_role.role_arn,
                    ),
                ),
                dynamic_partitioning_configuration=firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                    enabled=True,
                    retry_options=firehose.CfnDeliveryStream.RetryOptionsProperty(
                        duration_in_seconds=300,
                    ),
                ),
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[
                        firehose.CfnDeliveryStream.ProcessorProperty(
                            type="MetadataExtraction",
                            parameters=[
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="MetadataExtractionQuery",
                                    parameter_value=(
                                        "{dt:.datetime[:10]"
                                        ",hour:.datetime[11:13]"
                                        ",derived_genre:.derived_genre"
                                        ",platform_group:.platform_group}"
                                    ),
                                ),
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="JsonParsingEngine",
                                    parameter_value="JQ-1.6",
                                ),
                            ],
                        )
                    ],
                ),
                s3_backup_mode="Disabled",
                encryption_configuration=firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                    kms_encryption_config=firehose.CfnDeliveryStream.KMSEncryptionConfigProperty(
                        awskms_key_arn=(
                            kinesis_key.key_arn if kinesis_key
                            else cdk.Aws.NO_VALUE  # type: ignore[arg-type]
                        )
                    ) if kinesis_key else firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                        no_encryption_config="NoEncryption"
                    )
                ) if kinesis_key else None,
            ),
        )

        # ── replay-producer Lambda ────────────────────────────────────────────
        # genre_classifier module is in the repo root; packaged as a Lambda layer
        # or included via the function's bundling step in etl_stack / compute_stack.
        replay_role = iam.Role(
            self, "ReplayRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
            inline_policies={
                "replay-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["kinesis:PutRecords"],
                            resources=[self._stream.stream_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:GetObject", "s3:ListBucket"],
                            resources=(
                                [bucket.bucket_arn, bucket.arn_for_objects("raw-source/*")]
                                if bucket
                                else ["arn:aws:s3:::PLACEHOLDER",
                                      "arn:aws:s3:::PLACEHOLDER/*"]
                            ),
                        ),
                        # Required when NOVA_FALLBACK_ENABLED=1; harmless otherwise.
                        iam.PolicyStatement(
                            actions=["s3:GetObject", "s3:PutObject"],
                            resources=(
                                [bucket.arn_for_objects("glue-scripts/lut_extended.json")]
                                if bucket
                                else ["arn:aws:s3:::PLACEHOLDER/glue-scripts/lut_extended.json"]
                            ),
                        ),
                        iam.PolicyStatement(
                            actions=["bedrock:InvokeModel"],
                            resources=["arn:aws:bedrock:ap-southeast-1::foundation-model/amazon.nova-micro-v1:0"],
                        ),
                    ]
                )
            },
        )

        self._replay_fn = _lambda.Function(
            self, "ReplayProducer",
            function_name=f"ott-replay-producer-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/replay_producer"),
            memory_size=512,
            timeout=Duration.minutes(15),
            role=replay_role,
            environment={
                "STREAM_NAME": self._stream.stream_name,
                "SPEEDUP_FACTOR": "336",
                "PARQUET_S3_PREFIX": (
                    f"s3://{bucket_name}/raw-source/log_search/"
                ),
            },
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def stream(self) -> kinesis.Stream:
        return self._stream

    @property
    def stream_arn(self) -> str:
        return self._stream.stream_arn

    @property
    def stream_name(self) -> str:
        return self._stream.stream_name

    @property
    def replay_fn(self) -> _lambda.Function:
        return self._replay_fn
