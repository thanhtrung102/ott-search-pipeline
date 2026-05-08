---
title: "4.1 Foundation (Iteration 0)"
weight: 41
pre: "<b>4.1 </b>"
---

## Context

Before any data enters the system, the security and networking foundation must exist. All AWS services in this pipeline run inside a private VPC with no public internet access — they communicate exclusively through VPC endpoints. This prevents data from transiting the public internet and satisfies the CIS AWS Foundations Benchmark requirement for network isolation.

The StorageStack creates four KMS customer-managed keys (one per AWS service: S3, Kinesis, SNS, DynamoDB), the S3 bucket, CloudTrail trail, and GuardDuty detector. NetworkStack creates the VPC, security groups, and 10 interface VPC endpoints.

---

## Deploy

```bash
cdk deploy OttNetwork-${CDK_ENV} OttStorage-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --context alert_email=${ALERT_EMAIL} \
  --require-approval never
```

Expected output (both stacks):
```
✅  OttNetwork-dev (no changes)
✅  OttStorage-dev (no changes)

Outputs:
OttNetwork-dev.VpcId = vpc-xxxxxxxxxxxxxxxxx
OttStorage-dev.BucketName = ott-search-123456789012-dev
```

**Deploy time:** approximately 8 minutes. The KMS keys and VPC endpoints take the longest.

---

## What was deployed

### VPC layout

| Subnet | CIDR | Purpose |
|---|---|---|
| Public-a | 10.0.101.0/24 | NAT Gateway (1 AZ only) |
| Public-b | 10.0.102.0/24 | Spare |
| Private-a | 10.0.1.0/24 | Glue, Lambda |
| Private-b | 10.0.2.0/24 | Glue, Lambda (second AZ) |

### VPC endpoints

| Type | Service | Notes |
|---|---|---|
| Gateway | S3 | Free; routes private subnet S3 traffic through prefix list |
| Interface | Glue | ETL workers read/write S3 without NAT |
| Interface | Athena | Step Functions queries run without NAT |
| Interface | CloudWatch Logs | Lambda structured logs |
| Interface | CloudWatch | Custom metrics |
| Interface | SNS | Anomaly alert publish |
| Interface | Kinesis Streams | replay-producer and anomaly-detector |
| Interface | Secrets Manager | LLM API key (Iteration 2, LLM path) |
| Interface | STS | Role assumption |
| Interface | KMS | Envelope key operations |
| Interface | Kinesis Firehose | Firehose endpoint (no CDK enum — `kinesis-firehose`) |
| Gateway | DynamoDB | Routes private subnet DynamoDB traffic without NAT |
| Interface | EventBridge | Anomaly-detector PutEvents call |

### KMS keys

| Alias | Used by |
|---|---|
| `alias/ott-s3-key` | S3 bucket SSE-KMS |
| `alias/ott-kinesis-key` | Kinesis Data Streams + Firehose |
| `alias/ott-sns-key` | SNS anomaly alert topic |
| `alias/ott-dynamodb-key` | DynamoDB baseline and anomaly tables |

All keys have annual rotation enabled and a 7-day pending deletion window.

### Security groups

Three security groups are created with **no inbound rules and HTTPS-only egress to the VPC CIDR**:

- `ott-glue-dev` — Glue ETL workers: egress port 443 to `10.0.0.0/16`
- `ott-lambda-dev` — Lambda functions: egress port 443 to `10.0.0.0/16`
- `ott-endpoints-dev` — Interface endpoints: ingress port 443 from Glue and Lambda SGs only

---

## Screenshot 1 — VPC Endpoints

> **Note:** Screenshots were captured from the `demo` environment. Your resources will show `-dev` instead of `-demo`.

Navigate to: **VPC Console → Your VPCs → [select your VPC] → Endpoints tab**

You should see 13 endpoints in `available` state: 2 Gateway (S3, DynamoDB) and 11 Interface endpoints.

{{% notice tip %}}
**📸 Screenshot 1:** Navigate to **VPC Console → Your VPCs → [select VPC tagged `ott-search`] → Endpoints tab**. Capture the full list — all 11 endpoints must show **State: available**. Save as `workshop/static/images/4.1-vpc-endpoints.png`.
{{% /notice %}}

---

## Screenshot 2 — CloudTrail trail

Navigate to: **CloudTrail Console → Trails → `ott-search-trail`**

Verify these settings are visible:
- Multi-region trail: **Yes**
- Log file validation: **Enabled**
- S3 bucket: `ott-search-{account}-dev`
- KMS key: `ott-s3-key`

{{% notice tip %}}
**📸 Screenshot 2:** Open the trail detail page. Make sure **Multi-region trail: Yes** and **Log file validation: Enabled** are both visible in the same frame. Save as `workshop/static/images/4.1-cloudtrail.png`.
{{% /notice %}}

---

## Screenshot 3 — GuardDuty active

Navigate to: **GuardDuty Console → Summary**

Status should show **Active** with your detector ID visible.

{{% notice tip %}}
**📸 Screenshot 3:** Capture the GuardDuty summary page showing **Status: Active** and the detector ID. Save as `workshop/static/images/4.1-guardduty.png`.
{{% /notice %}}

---

## Verification

```bash
# Confirm VPC endpoints are available
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=tag:Project,Values=ott-search-pipeline" \
  --query "Vpcs[0].VpcId" --output text)

aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
  --query 'VpcEndpoints[*].{Service:ServiceName,State:State}' \
  --output table
```

Expected output (13 rows, all `available`):
```
-------------------------------------------------------------------
|                     DescribeVpcEndpoints                        |
+-------------------------------------------------+---------------+
|                     Service                     |     State     |
+-------------------------------------------------+---------------+
|  com.amazonaws.ap-southeast-1.s3                |  available    |
|  com.amazonaws.ap-southeast-1.athena            |  available    |
|  com.amazonaws.ap-southeast-1.sns               |  available    |
|  com.amazonaws.ap-southeast-1.kinesis-firehose  |  available    |
|  com.amazonaws.ap-southeast-1.secretsmanager    |  available    |
|  com.amazonaws.ap-southeast-1.sts               |  available    |
|  com.amazonaws.ap-southeast-1.monitoring        |  available    |
|  com.amazonaws.ap-southeast-1.kinesis-streams   |  available    |
|  com.amazonaws.ap-southeast-1.glue              |  available    |
|  com.amazonaws.ap-southeast-1.kms               |  available    |
|  com.amazonaws.ap-southeast-1.logs              |  available    |
|  com.amazonaws.ap-southeast-1.dynamodb          |  available    |
|  com.amazonaws.ap-southeast-1.events            |  available    |
+-------------------------------------------------+---------------+
```

```bash
# Verify S3 bucket exists with versioning and KMS
aws s3api get-bucket-versioning --bucket ${BUCKET}
# Expected: {"Status": "Enabled"}

aws s3api get-bucket-encryption --bucket ${BUCKET} \
  --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault'
# Expected: {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": "arn:aws:kms:..."}
```

---

## Explanation

The VPC endpoint for S3 appears in the private subnet route table as a prefix list entry (`pl-xxxxxxxx`). All traffic from private subnets to S3 is routed through this endpoint rather than the NAT Gateway. This means Glue ETL workers and Lambda functions can read and write S3 without any data leaving the AWS network boundary — and without incurring NAT Gateway data-processing charges (~$0.045 per GB).

The 10 interface endpoints resolve to private IP addresses in your private subnets via Route 53 private DNS. When Glue calls `glue.ap-southeast-1.amazonaws.com`, DNS resolves to a private IP inside the VPC rather than the public endpoint.
