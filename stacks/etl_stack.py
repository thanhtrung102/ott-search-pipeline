"""
Iteration 2 — Glue ETL layer.

Resources:
  Glue Database   : ott_search_curated
  Glue Crawler    : raw-events-crawler; target raw/events/; on-demand
  Glue ETL Job    : search-enrichment-job; G.1X, 2 DPUs, Glue 4.0;
                    job bookmark enabled; reads last 2 days
  IAM roles       : ott-glue-crawler-role, ott-glue-etl-role (least-privilege)

Cross-stack inputs:
  vpc, glue_sg  ← NetworkStack
  bucket, s3_key  ← StorageStack
"""
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
from stacks import tag_stack


class ETLStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        bucket: s3.IBucket | None = None,
        s3_key=None,
        glue_sg=None,
        vpc=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = bucket.bucket_name if bucket else "PLACEHOLDER"
        bucket_arn  = bucket.bucket_arn  if bucket else "arn:aws:s3:::PLACEHOLDER"

        # ── Glue curated database ─────────────────────────────────────────────
        curated_db = glue.CfnDatabase(
            self, "CuratedDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ott_search_curated",
                description="OTT pipeline - enriched curated events",
            ),
        )

        # ── IAM role for Glue crawler ─────────────────────────────────────────
        crawler_role = iam.Role(
            self, "CrawlerRole",
            role_name=f"ott-glue-crawler-{env_name}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[bucket_arn, f"{bucket_arn}/*"],
            )
        )
        if s3_key:
            s3_key.grant_decrypt(crawler_role)

        # ── Glue crawler (raw-events-crawler) ─────────────────────────────────
        self._crawler = glue.CfnCrawler(
            self, "RawEventsCrawler",
            name=f"raw-events-crawler-{env_name}",
            role=crawler_role.role_arn,
            database_name="ott_search_raw",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket_name}/raw/events/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            configuration=('{"Version":1.0,'
                           '"CrawlerOutput":{'
                           '"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"},'
                           '"Tables":{"AddOrUpdateBehavior":"MergeNewColumns"}}}'),
        )

        # ── IAM role for Glue ETL job ─────────────────────────────────────────
        etl_role = iam.Role(
            self, "EtlRole",
            role_name=f"ott-glue-etl-{env_name}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        etl_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                         "s3:ListBucket", "s3:GetBucketLocation",
                         "s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload"],
                resources=[bucket_arn, f"{bucket_arn}/*"],
            )
        )
        etl_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetTable", "glue:GetDatabase", "glue:GetPartition",
                         "glue:GetPartitions", "glue:CreateTable", "glue:UpdateTable",
                         "glue:BatchCreatePartition"],
                resources=["*"],
            )
        )
        if s3_key:
            s3_key.grant_encrypt_decrypt(etl_role)
        etl_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=["*"],
            )
        )

        # Upload Glue script to S3 (uses CDK asset deployment approach via S3 ref)
        glue_script_location = (
            f"s3://{bucket_name}/glue-scripts/search_enrichment_job.py"
        )

        # ── Glue ETL job (search-enrichment-job) ─────────────────────────────
        vpc_conn_args: dict = {}
        if glue_sg and vpc:
            # Glue connection allows job to run inside the VPC
            conn = glue.CfnConnection(
                self, "GlueVpcConnection",
                catalog_id=self.account,
                connection_input=glue.CfnConnection.ConnectionInputProperty(
                    name=f"ott-glue-vpc-{env_name}",
                    connection_type="NETWORK",
                    physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        subnet_id=vpc.private_subnets[0].subnet_id,
                        security_group_id_list=[glue_sg.security_group_id],
                        availability_zone=vpc.private_subnets[0].availability_zone,
                    ),
                ),
            )
            vpc_conn_args = {"connections": glue.CfnJob.ConnectionsListProperty(
                connections=[conn.ref]
            )}

        self._enrichment_job = glue.CfnJob(
            self, "EnrichmentJob",
            name=f"search-enrichment-job-{env_name}",
            role=etl_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=glue_script_location,
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{bucket_name}/glue-temp/spark-ui/",
                "--TempDir": f"s3://{bucket_name}/glue-temp/",
                "--S3_BUCKET": bucket_name,
                "--CURATED_DB": "ott_search_curated",
                "--RAW_DB": "ott_search_raw",
                "--LLM_ENABLED": "true",
                "--extra-py-files": (
                    f"s3://{bucket_name}/glue-scripts/genre_classifier.zip"
                ),
                "--additional-python-modules": "rapidfuzz>=3.0.0",
            },
            max_retries=1,
            timeout=30,  # minutes
            **vpc_conn_args,
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def enrichment_job_name(self) -> str:
        return self._enrichment_job.ref

    @property
    def crawler_name(self) -> str:
        return self._crawler.ref
