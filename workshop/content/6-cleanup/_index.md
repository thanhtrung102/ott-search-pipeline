---
title: "6. Clean Up"
weight: 60
chapter: true
pre: "<b>6. </b>"
---

# Clean Up

Tear down all resources in **reverse dependency order**. Do not skip steps — CDK destroy will fail if dependent resources still reference a stack being deleted.

{{% notice warning %}}
**NAT Gateway cost:** If you leave the VPC deployed, the NAT Gateway accrues ~$0.045/hour (~$1.08/day). Complete cleanup within the same day as the demo to stay within the $9.60 demo budget.
{{% /notice %}}

---

## Step 1 — Disable Macie

Macie sessions are not deleted by CloudFormation. Disable first:

```bash
aws macie2 disable-macie
# No output = success
```

---

## Step 2 — Empty the S3 bucket

CDK destroy fails if the bucket contains versioned objects.

```bash
# Delete all versions and delete markers
aws s3api list-object-versions --bucket ${BUCKET} \
  --query 'Versions[*].{Key:Key,VersionId:VersionId}' \
  --output json | \
  jq -r '.[] | "--key \(.Key) --version-id \(.VersionId)"' | \
  while read args; do
    aws s3api delete-object --bucket ${BUCKET} $args
  done

aws s3api list-object-versions --bucket ${BUCKET} \
  --query 'DeleteMarkers[*].{Key:Key,VersionId:VersionId}' \
  --output json | \
  jq -r '.[] | "--key \(.Key) --version-id \(.VersionId)"' | \
  while read args; do
    aws s3api delete-object --bucket ${BUCKET} $args
  done

echo "Bucket emptied"
```

---

## Step 3 — Destroy CDK stacks (reverse order)

```bash
# Iteration 7
cdk destroy OttPipeline-${CDK_ENV} OttObservability-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force

# Iteration 6
cdk destroy OttGovernance-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force

# Iteration 4
cdk destroy OttAnalytics-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force

# Iteration 3
cdk destroy OttCompute-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force

# Iteration 2
cdk destroy OttETL-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force

# Iteration 1
cdk destroy OttIngestion-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force

# Iteration 0 (VPC — stops NAT Gateway charge)
cdk destroy OttStorage-${CDK_ENV} OttNetwork-${CDK_ENV} \
  --context env=${CDK_ENV} --context account=${CDK_ACCOUNT} --force
```

---

## Step 4 — Delete GuardDuty detector

```bash
DETECTOR=$(aws guardduty list-detectors --query 'DetectorIds[0]' --output text)
aws guardduty delete-detector --detector-id ${DETECTOR}
echo "GuardDuty deleted"
```

---

## Step 5 — Schedule KMS key deletion (7-day window)

```bash
for alias in ott-s3-key ott-kinesis-key ott-sns-key ott-dynamodb-key; do
  KEY_ID=$(aws kms describe-key \
    --key-id "alias/${alias}" \
    --query 'KeyMetadata.KeyId' --output text)
  aws kms schedule-key-deletion \
    --key-id ${KEY_ID} \
    --pending-window-in-days 7
  echo "Scheduled: ${alias}"
done
```

---

## Step 6 — Cancel QuickSight

**QuickSight Console → Account settings → Account termination → Delete account**

This removes the $9/month charge.

---

## Step 7 — Verify cleanup

```bash
# No Ott* stacks remain
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
  --query 'StackSummaries[?contains(StackName,`Ott`)].StackName' \
  --output table
# Expected: empty

# S3 bucket gone
aws s3api head-bucket --bucket ${BUCKET} 2>&1
# Expected: 404 Not Found

# Kinesis stream gone
aws kinesis describe-stream-summary \
  --stream-name ott-search-stream-${CDK_ENV} 2>&1
# Expected: ResourceNotFoundException
```

---

## Demo cost summary

| Component | Cost |
|---|---|
| Kinesis Data Streams (2 shards, ~18 min replay) | ~$0.05 |
| Kinesis Firehose (~1.1M records, JSON→Parquet) | ~$0.05 |
| Glue ETL (10 DPU × G.1X × ~200 s) | ~$0.32 |
| Athena (CTAS + validation, ~2 GB scanned) | ~$0.01 |
| Step Functions (1 execution) | ~$0.01 |
| KMS (4 CMKs × API calls) | ~$0.02 |
| Lambda (3 functions) | ~$0.00 |
| S3 (~10 GB raw + curated + gold) | ~$0.23 |
| QuickSight Standard (1 month) | $9.00 |
| **Total** | **~$9.60** |

QuickSight ($9.00) is 93.7% of the total cost. All AWS data pipeline services combined cost ~$0.60 for the full June 2022 dataset run.
