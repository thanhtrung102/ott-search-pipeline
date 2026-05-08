---
title: "6. Clean Up"
weight: 60
chapter: true
pre: "<b>6. </b>"
---

# Clean Up

Destroy all resources in reverse dependency order. **Run every command — skipping steps will leave orphaned resources that continue to incur charges.**

---

## Step 1 — Disable Macie (if deployed)

Macie cannot be disabled by CloudFormation. Run this first:

```bash
DETECTOR=$(aws macie2 get-macie-session \
  --query 'status' --output text 2>/dev/null || echo "PAUSED")

if [ "${DETECTOR}" = "ENABLED" ]; then
  aws macie2 disable-macie
  echo "Macie disabled"
fi
```

---

## Step 2 — Empty the S3 bucket

CloudFormation cannot delete a non-empty S3 bucket. Empty it first:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

echo "Deleting all objects in ${BUCKET}..."
aws s3 rm "s3://${BUCKET}/" --recursive

# Delete versioned objects and delete markers
aws s3api delete-objects \
  --bucket ${BUCKET} \
  --delete "$(aws s3api list-object-versions \
    --bucket ${BUCKET} \
    --output json \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
    2>/dev/null)" 2>/dev/null || true

aws s3api delete-objects \
  --bucket ${BUCKET} \
  --delete "$(aws s3api list-object-versions \
    --bucket ${BUCKET} \
    --output json \
    --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
    2>/dev/null)" 2>/dev/null || true

echo "Bucket emptied"
```

---

## Step 3 — Destroy all CDK stacks

Destroy in reverse dependency order (pipeline and observability first, foundation last):

```bash
ACCT=${CDK_ACCOUNT}
ENV=${CDK_ENV}

# Iteration 7 (CI/CD + observability)
cdk destroy OttPipeline-${ENV} OttObservability-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force

# Iteration 6 (governance)
cdk destroy OttGovernance-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force

# Iteration 4 (analytics / Step Functions)
cdk destroy OttAnalytics-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force

# Iteration 3 (compute / anomaly detection)
cdk destroy OttCompute-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force

# Iteration 2 (ETL)
cdk destroy OttETL-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force

# Iteration 1 (ingestion)
cdk destroy OttIngestion-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force

# Iteration 0 (storage + network — last)
cdk destroy OttStorage-${ENV} OttNetwork-${ENV} \
  --context env=${ENV} --context account=${ACCT} --force
```

Each `cdk destroy` takes approximately 1–3 minutes.

{{% notice warning %}}
KMS keys are created with `RETAIN` removal policy — they are **not** deleted by `cdk destroy` to prevent accidental data loss (data encrypted with a deleted key cannot be recovered). After destroying all stacks, schedule the keys for deletion manually:

```bash
for ALIAS in ott-s3-key ott-kinesis-key ott-sns-key ott-dynamodb-key; do
  KEY_ID=$(aws kms describe-key --key-id "alias/${ALIAS}" \
    --query 'KeyMetadata.KeyId' --output text 2>/dev/null)
  if [ -n "${KEY_ID}" ]; then
    aws kms schedule-key-deletion \
      --key-id ${KEY_ID} \
      --pending-window-in-days 7
    echo "Scheduled deletion: ${ALIAS} (${KEY_ID}) in 7 days"
  fi
done
```
{{% /notice %}}

---

## Step 4 — Delete GuardDuty detector

GuardDuty is not deleted by CloudFormation (deletion would be destructive if the account has other active workloads).

```bash
DETECTOR_ID=$(aws guardduty list-detectors \
  --query 'DetectorIds[0]' --output text)

if [ "${DETECTOR_ID}" != "None" ] && [ -n "${DETECTOR_ID}" ]; then
  aws guardduty delete-detector --detector-id ${DETECTOR_ID}
  echo "GuardDuty detector deleted: ${DETECTOR_ID}"
fi
```

---

## Step 5 — Remove Lake Formation admin (if needed)

If the Lake Formation settings are causing issues with subsequent CDK deployments in this account, reset the default permissions:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

aws lakeformation put-data-lake-settings \
  --data-lake-settings '{
    "DataLakeAdmins": [],
    "CreateDatabaseDefaultPermissions": [
      {"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
       "Permissions": ["ALL"]}
    ],
    "CreateTableDefaultPermissions": [
      {"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
       "Permissions": ["ALL"]}
    ]
  }'
echo "Lake Formation default permissions restored"
```

---

## Step 6 — Cancel QuickSight subscription (if activated)

1. Open **QuickSight Console → top-right menu → Manage QuickSight**
2. **Account settings → Delete account**
3. Type the account name to confirm
4. Click **Delete account**

---

## Step 7 — Verify no orphaned resources

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ENV=${CDK_ENV}

# Check for remaining CloudFormation stacks
echo "=== CloudFormation stacks ==="
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
  --query "StackSummaries[?contains(StackName,'Ott')].StackName" \
  --output text
# Expected: empty

# Check for remaining S3 buckets
echo "=== S3 buckets ==="
aws s3api list-buckets \
  --query "Buckets[?contains(Name,'ott-search')].Name" \
  --output text
# Expected: empty (or only the artifacts bucket if CodePipeline was deployed)

# Check for DynamoDB tables
echo "=== DynamoDB tables ==="
aws dynamodb list-tables \
  --query "TableNames[?contains(@,'ott')]" \
  --output text
# Expected: empty

# Check for Lambda functions
echo "=== Lambda functions ==="
aws lambda list-functions \
  --query "Functions[?contains(FunctionName,'ott')].FunctionName" \
  --output text
# Expected: empty
```

---

## Cost summary

The table below shows the expected cost for a complete pipeline build and demo run using the June 2022 dataset.

| Service | Estimated cost |
|---|---|
| Kinesis Data Streams | $0.015 (2 shards × ~15 min active) |
| Kinesis Firehose | $0.028 (~1.4 GB data processed) |
| Glue ETL | $1.27 (10 DPU × ~260 sec = 0.72 DPU-hours × $0.44/DPU-hr × 2 runs; reduce to 2 DPU for ~$0.13 at 3× longer runtime) |
| Athena | $0.06 (CTAS + validation queries, <3 GB scanned) |
| Lambda | $0.00 (within free tier) |
| DynamoDB | $0.00 (within free tier for on-demand) |
| S3 | $0.02 (~1 GB stored for demo period) |
| KMS | $0.04 (4 keys × $0.01/month, API calls) |
| CloudTrail | $0.00 (first trail in account is free) |
| GuardDuty | $0.00 (30-day free trial) |
| QuickSight | $9.00 (Standard, 1 author, 1 month — cancel after demo) |
| **Total** | **~$9.60** |

{{% notice info %}}
The NAT Gateway (`$0.045/hour × ~1 hour setup`) is the largest non-QuickSight cost if left running. The stack deploys 1 NAT Gateway in AZ-a only. If you leave the stacks deployed for testing, the NAT Gateway accumulates at ~$1.08/day. Destroy promptly after the demo.
{{% /notice %}}

{{% notice tip %}}
**📸 Screenshot:** Navigate to **AWS Cost Explorer → Group by: Service → Date range: [your project start to today]**. Capture the bar chart with Glue, QuickSight, KMS, CloudWatch, and S3 as visible line items. The total cost label should be visible. Save as `workshop/static/images/6-cost-explorer.png`.
{{% /notice %}}
