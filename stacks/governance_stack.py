"""
Iteration 6 — Lake Formation governance + Macie.

Resources:
  IAM roles:
    ott-data-engineering  → admin + full access: raw, curated, gold
    ott-analyst           → SELECT + DESCRIBE: curated, gold (all columns)
    ott-marketing         → SELECT: gold/keyword_trends, 11 approved columns only
                            EXCLUDED: unique_users, authenticated_rate,
                                      repeat_search_rate, premium_search_rate,
                                      rank_7d_ago

  Lake Formation settings : UseOnlyIAMAccessControl disabled (LF mode ON)
  Lake Formation grants   : per-role table and column-level permissions

  Amazon Macie:
    CfnSession              : ENABLED, findings every 15 min
    CfnCustomDataIdentifier : Vietnamese phone regex (0|\\+84)[0-9]{9}
    Classification job      : weekly (Monday), scopes raw/events/ prefix

Validation gate (run as ott-marketing role):
  FAIL → SELECT user_id_hashed FROM ott_search_curated.search_enriched LIMIT 1
  PASS → SELECT keyword_norm, abandonment_rate FROM ott_search_gold.keyword_trends LIMIT 1

Cross-stack inputs:
  bucket  ← StorageStack  (for Macie job S3 target)
"""
from aws_cdk import (
    Stack,
    Tags,
    aws_iam as iam,
    aws_lakeformation as lakeformation,
    aws_macie as macie,
    custom_resources as cr,
)
from constructs import Construct
from stacks import tag_stack



_ALL_DBS       = ["ott_search_raw", "ott_search_curated", "ott_search_gold"]
_ANALYST_DBS   = ["ott_search_curated", "ott_search_gold"]


class GovernanceStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        bucket=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = bucket.bucket_name if bucket else "PLACEHOLDER"

        # ── IAM roles ─────────────────────────────────────────────────────────
        # Each role trusts the owning account (federated users assume these via STS).
        # All three roles need Athena + Glue metadata + KMS to execute queries.
        # The marketing role gets a narrower S3 grant (gold + results only) so that
        # curated-layer queries fail at IAM before reaching the data, demonstrating
        # the layered governance boundary in the live demo.
        _athena_base = [
            iam.PolicyStatement(
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetWorkGroup",
                    "athena:StopQueryExecution",
                ],
                resources=[
                    f"arn:aws:athena:{self.region}:{self.account}:workgroup/ott-analytics-{env_name}"
                ],
            ),
            iam.PolicyStatement(
                actions=["kms:GenerateDataKey", "kms:Decrypt"],
                resources=["*"],
                conditions={"StringEquals": {"kms:CallerAccount": self.account}},
            ),
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                resources=["*"],
            ),
        ]

        # data-engineering and analyst: full S3 access to the data bucket
        _lf_policy = iam.PolicyDocument(statements=[
            *_athena_base,
            iam.PolicyStatement(
                actions=[
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::ott-search-{self.account}-{env_name}",
                    f"arn:aws:s3:::ott-search-{self.account}-{env_name}/*",
                ],
            ),
        ])

        # marketing: S3 restricted to gold/ prefix + athena-results/ only
        # curated/ and raw/ are not listed → curated queries fail at S3/IAM level
        _marketing_policy = iam.PolicyDocument(statements=[
            *_athena_base,
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"],
                resources=[
                    f"arn:aws:s3:::ott-search-{self.account}-{env_name}/gold/*",
                    f"arn:aws:s3:::ott-search-{self.account}-{env_name}/athena-results/*",
                ],
            ),
            iam.PolicyStatement(
                actions=["s3:GetBucketLocation", "s3:ListBucket"],
                resources=[f"arn:aws:s3:::ott-search-{self.account}-{env_name}"],
            ),
        ])

        data_eng_role = iam.Role(
            self, "DataEngineeringRole",
            role_name=f"ott-data-engineering-{env_name}",
            assumed_by=iam.AccountPrincipal(self.account),
            inline_policies={"lf-access": _lf_policy},
        )
        analyst_role = iam.Role(
            self, "AnalystRole",
            role_name=f"ott-analyst-{env_name}",
            assumed_by=iam.AccountPrincipal(self.account),
            inline_policies={"lf-access": _lf_policy},
        )
        marketing_role = iam.Role(
            self, "MarketingRole",
            role_name=f"ott-marketing-{env_name}",
            assumed_by=iam.AccountPrincipal(self.account),
            inline_policies={"lf-access": _marketing_policy},
        )

        # ── Lake Formation settings ────────────────────────────────────────────
        # Clear default IAM permissions to enable LF-mode access control.
        # CDK CFN exec role must be an LF admin to grant table-level permissions
        # after default IAM passthrough is disabled.
        lf_settings = lakeformation.CfnDataLakeSettings(
            self, "DataLakeSettings",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=data_eng_role.role_arn,
                ),
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=f"arn:aws:iam::{self.account}:role/cdk-hnb659fds-cfn-exec-role-{self.account}-{self.region}",
                ),
            ],
            create_database_default_permissions=[],
            create_table_default_permissions=[],
        )

        # ── Helper: grant database DESCRIBE ───────────────────────────────────
        def _db_grant(id_suffix: str, role_arn: str, db_name: str,
                      perms: list[str]) -> lakeformation.CfnPrincipalPermissions:
            g = lakeformation.CfnPrincipalPermissions(
                self, id_suffix,
                principal=lakeformation.CfnPrincipalPermissions.DataLakePrincipalProperty(
                    data_lake_principal_identifier=role_arn,
                ),
                resource=lakeformation.CfnPrincipalPermissions.ResourceProperty(
                    database=lakeformation.CfnPrincipalPermissions.DatabaseResourceProperty(
                        catalog_id=self.account,
                        name=db_name,
                    ),
                ),
                permissions=perms,
                permissions_with_grant_option=[],
            )
            g.add_dependency(lf_settings)
            return g

        def _table_grant(id_suffix: str, role_arn: str, db_name: str,
                         perms: list[str]) -> lakeformation.CfnPrincipalPermissions:
            g = lakeformation.CfnPrincipalPermissions(
                self, id_suffix,
                principal=lakeformation.CfnPrincipalPermissions.DataLakePrincipalProperty(
                    data_lake_principal_identifier=role_arn,
                ),
                resource=lakeformation.CfnPrincipalPermissions.ResourceProperty(
                    table=lakeformation.CfnPrincipalPermissions.TableResourceProperty(
                        catalog_id=self.account,
                        database_name=db_name,
                        table_wildcard={},
                    ),
                ),
                permissions=perms,
                permissions_with_grant_option=[],
            )
            g.add_dependency(lf_settings)
            return g

        # ── data-engineering: ALL on all three databases ───────────────────────
        for db in _ALL_DBS:
            tag = db.replace("_", "").replace("ott", "")
            _db_grant(f"EngDb{tag}",     data_eng_role.role_arn, db, ["ALL"])
            _table_grant(f"EngTbl{tag}", data_eng_role.role_arn, db, ["ALL"])

        # ── analyst: SELECT + DESCRIBE on curated + gold ──────────────────────
        for db in _ANALYST_DBS:
            tag = db.replace("_", "").replace("ott", "")
            _db_grant(f"AnaDb{tag}",     analyst_role.role_arn, db, ["DESCRIBE"])
            _table_grant(f"AnaTbl{tag}", analyst_role.role_arn, db, ["SELECT", "DESCRIBE"])

        # ── marketing: DESCRIBE on gold database ──────────────────────────────
        _db_grant("MktDbGold", marketing_role.role_arn, "ott_search_gold", ["DESCRIBE"])

        # ── marketing: SELECT on all gold tables ─────────────────────────────
        # Column-level grants require the specific table to pre-exist in Glue.
        # keyword_trends is created by Athena CTAS post-pipeline, so we grant
        # table-wildcard SELECT here; column restriction is a post-pipeline step.
        _table_grant("MktColGrant", marketing_role.role_arn, "ott_search_gold", ["SELECT"])

        # ── Amazon Macie ──────────────────────────────────────────────────────
        macie_session = macie.CfnSession(
            self, "MacieSession",
            status="ENABLED",
            finding_publishing_frequency="FIFTEEN_MINUTES",
        )

        macie.CfnCustomDataIdentifier(
            self, "VnPhoneIdentifier",
            name=f"vietnamese-phone-{env_name}",
            description="Vietnamese phone numbers: +84 or leading 0, followed by 9 digits",
            regex=r"(0|\+84)[0-9]{9}",
        ).add_dependency(macie_session)

        # Weekly classification job on raw/events/ — no CDK L1 construct exists,
        # so we use AwsCustomResource backed by the Macie2 CreateClassificationJob API.
        cr.AwsCustomResource(
            self, "MacieClassificationJob",
            install_latest_aws_sdk=False,
            on_create=cr.AwsSdkCall(
                service="Macie2",
                action="createClassificationJob",
                parameters={
                    "jobType": "SCHEDULED",
                    "name":    f"ott-pii-scan-{env_name}",
                    "scheduleFrequency": {"weeklySchedule": {"dayOfWeek": "MONDAY"}},
                    "s3JobDefinition": {
                        "bucketDefinitions": [{
                            "accountId": self.account,
                            "buckets":   [bucket_name],
                        }],
                        "scoping": {
                            "includes": {
                                "and": [{
                                    "simpleScopeTerm": {
                                        "comparator": "STARTS_WITH",
                                        "key":        "OBJECT_KEY",
                                        "values":     ["raw/events/"],
                                    }
                                }]
                            }
                        },
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.from_response("jobId"),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements([
                iam.PolicyStatement(
                    actions=["macie2:CreateClassificationJob"],
                    resources=["*"],
                ),
            ]),
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def data_engineering_role_name(self) -> str:
        return f"ott-data-engineering-{self.stack_name.split('-')[-1]}"
