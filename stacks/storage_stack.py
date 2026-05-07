"""
Iteration 0 — Storage + security baseline.

Resources:
  S3          : ott-search-{account}-{env}; versioning, SSE-KMS, lifecycle rules
  KMS CMKs    : ott-s3-key, ott-kinesis-key, ott-sns-key, ott-dynamodb-key
                annual rotation, account-identity access control
  CloudTrail  : multi-region, file-validation, KMS-encrypted → same S3 bucket
  GuardDuty   : enabled, 15-min finding publish
  AWS Config  : skipped for demo (CfnConfigurationRecorder blocks indefinitely in ap-southeast-1)
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
    aws_cloudtrail as cloudtrail,
    aws_guardduty as guardduty,
    aws_iam as iam,
    aws_kms as kms,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct
from stacks import tag_stack


class StorageStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, env_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── KMS CMKs ──────────────────────────────────────────────────────────
        def _key(logical_id: str, alias: str, description: str) -> kms.Key:
            return kms.Key(
                self, logical_id,
                alias=f"alias/{alias}",
                description=description,
                enable_key_rotation=True,
                pending_window=Duration.days(7),
                removal_policy=RemovalPolicy.RETAIN,
            )

        self._s3_key = _key(
            "S3Key", "ott-s3-key",
            "OTT pipeline — S3 bucket encryption",
        )
        self._kinesis_key = _key(
            "KinesisKey", "ott-kinesis-key",
            "OTT pipeline — Kinesis Streams / Firehose encryption",
        )
        self._sns_key = _key(
            "SnsKey", "ott-sns-key",
            "OTT pipeline — SNS topic encryption",
        )
        self._dynamodb_key = _key(
            "DynamoDbKey", "ott-dynamodb-key",
            "OTT pipeline — DynamoDB table encryption",
        )

        # ── S3 Bucket ─────────────────────────────────────────────────────────
        # Name: ott-search-{accountId}-{env}  (CloudFormation resolves accountId)
        bucket_name = cdk.Fn.sub(
            f"ott-search-${{AWS::AccountId}}-{env_name}",
        )
        self._bucket = s3.Bucket(
            self, "DataBucket",
            bucket_name=bucket_name,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self._s3_key,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                # raw/ → Intelligent-Tiering after 30 days
                s3.LifecycleRule(
                    id="raw-intelligent-tiering",
                    prefix="raw/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30),
                        )
                    ],
                ),
                # curated/ → Standard-IA after 60 days
                s3.LifecycleRule(
                    id="curated-standard-ia",
                    prefix="curated/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(60),
                        )
                    ],
                ),
                # athena-results/ → expire after 7 days
                s3.LifecycleRule(
                    id="athena-results-expire",
                    prefix="athena-results/",
                    expiration=Duration.days(7),
                ),
                # glue-temp/ → expire after 3 days
                s3.LifecycleRule(
                    id="glue-temp-expire",
                    prefix="glue-temp/",
                    expiration=Duration.days(3),
                ),
            ],
        )

        # ── CloudTrail ────────────────────────────────────────────────────────
        # CDK Python Trail does not auto-grant CloudTrail use of the KMS key;
        # add the required key policy statements explicitly.
        self._s3_key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCloudTrailEncrypt",
                principals=[iam.ServicePrincipal("cloudtrail.amazonaws.com")],
                actions=["kms:GenerateDataKey*"],
                resources=["*"],
                conditions={
                    "StringLike": {
                        "kms:EncryptionContext:aws:cloudtrail:arn": f"arn:aws:cloudtrail:*:{self.account}:trail/*",
                    }
                },
            )
        )
        self._s3_key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCloudTrailDescribeKey",
                principals=[iam.ServicePrincipal("cloudtrail.amazonaws.com")],
                actions=["kms:DescribeKey"],
                resources=["*"],
            )
        )

        cloudtrail.Trail(
            self, "AuditTrail",
            trail_name="ott-search-trail",
            bucket=self._bucket,
            s3_key_prefix="logs/cloudtrail",
            encryption_key=self._s3_key,
            enable_file_validation=True,
            is_multi_region_trail=True,
            include_global_service_events=True,
            send_to_cloud_watch_logs=True,
            cloud_watch_logs_retention=logs.RetentionDays.ONE_MONTH,
        )

        # ── GuardDuty ─────────────────────────────────────────────────────────
        guardduty.CfnDetector(
            self, "GuardDuty",
            enable=True,
            finding_publishing_frequency="FIFTEEN_MINUTES",
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def bucket(self) -> s3.Bucket:
        return self._bucket

    @property
    def s3_key(self) -> kms.Key:
        return self._s3_key

    @property
    def kinesis_key(self) -> kms.Key:
        return self._kinesis_key

    @property
    def sns_key(self) -> kms.Key:
        return self._sns_key

    @property
    def dynamodb_key(self) -> kms.Key:
        return self._dynamodb_key
