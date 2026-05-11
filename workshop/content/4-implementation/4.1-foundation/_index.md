---
title: "4.1 Foundation (Iteration 0)"
weight: 41
pre: "<b>4.1 </b>"
---

## What this deploys

Two stacks that all other stacks depend on:

- **OttNetwork** — VPC with 4 subnets (2 public, 2 private across 2 AZs), 13 VPC endpoints, 3 security groups
- **OttStorage** — 4 KMS CMKs, S3 bucket (versioned, SSE-KMS), CloudTrail multi-region trail, GuardDuty detector

All data-path services run in private subnets communicating exclusively through VPC endpoints — no public internet transit.

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

**Deploy time:** approximately 8 minutes (KMS key creation and VPC interface endpoints take the longest).

Expected terminal output:
```
✅  OttNetwork-demo

Outputs:
OttNetwork-demo.VpcId = vpc-xxxxxxxxxxxxxxxxx

✅  OttStorage-demo

Outputs:
OttStorage-demo.BucketName = ott-search-703668403514-demo
```

---

## Proof of deployment

### VPC and subnets

```bash
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=tag:aws:cloudformation:stack-name,Values=OttNetwork-${CDK_ENV}" \
  --query 'Vpcs[0].VpcId' --output text)
echo "VPC: ${VPC_ID}"

aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
  --query 'Subnets[*].{Name:Tags[?Key==`Name`]|[0].Value,CIDR:CidrBlock}' \
  --output table
```

Expected:
```
-------------------------------------------------------------
|                     DescribeSubnets                       |
+--------------------------------------+-------------------+
|  Name                                |  CIDR             |
+--------------------------------------+-------------------+
|  OttNetwork-demo/Vpc/PublicSubnet1   |  10.0.0.0/24      |
|  OttNetwork-demo/Vpc/PublicSubnet2   |  10.0.1.0/24      |
|  OttNetwork-demo/Vpc/PrivateSubnet1  |  10.0.2.0/24      |
|  OttNetwork-demo/Vpc/PrivateSubnet2  |  10.0.3.0/24      |
+--------------------------------------+-------------------+
```

### VPC endpoints (13 total)

```bash
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
  --query 'VpcEndpoints[*].{Service:ServiceName,State:State}' \
  --output table
```

Expected — 13 rows, all `available`:

```
-----------------------------------------------------------------
|  com.amazonaws.ap-southeast-1.s3                | available  |
|  com.amazonaws.ap-southeast-1.dynamodb          | available  |
|  com.amazonaws.ap-southeast-1.glue              | available  |
|  com.amazonaws.ap-southeast-1.athena            | available  |
|  com.amazonaws.ap-southeast-1.logs              | available  |
|  com.amazonaws.ap-southeast-1.monitoring        | available  |
|  com.amazonaws.ap-southeast-1.sns               | available  |
|  com.amazonaws.ap-southeast-1.kinesis-streams   | available  |
|  com.amazonaws.ap-southeast-1.secretsmanager    | available  |
|  com.amazonaws.ap-southeast-1.sts               | available  |
|  com.amazonaws.ap-southeast-1.kms               | available  |
|  com.amazonaws.ap-southeast-1.kinesis-firehose  | available  |
|  com.amazonaws.ap-southeast-1.events            | available  |
-----------------------------------------------------------------
```

### KMS CMKs (4 keys)

```bash
aws kms list-aliases \
  --query 'Aliases[?starts_with(AliasName,`alias/ott`)].AliasName' \
  --output json
```

Expected:
```json
[
  "alias/ott-dynamodb-key",
  "alias/ott-kinesis-key",
  "alias/ott-s3-key",
  "alias/ott-sns-key"
]
```

### S3 bucket — versioning and encryption

```bash
aws s3api get-bucket-versioning --bucket ${BUCKET}
# Expected: {"Status": "Enabled"}

aws s3api get-bucket-encryption --bucket ${BUCKET} \
  --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm' \
  --output text
# Expected: aws:kms
```

### CloudTrail

```bash
aws cloudtrail describe-trails \
  --query 'trailList[?contains(Name,`ott`)].{Name:Name,MultiRegion:IsMultiRegionTrail,Validation:LogFileValidationEnabled}' \
  --output json
```

Expected:
```json
[{"Name": "ott-search-trail", "MultiRegion": true, "Validation": true}]
```

### GuardDuty

```bash
DETECTOR=$(aws guardduty list-detectors --query 'DetectorIds[0]' --output text)
aws guardduty get-detector --detector-id ${DETECTOR} \
  --query '{Status:Status}' --output json
```

Expected:
```json
{"Status": "ENABLED"}
```

---

## Resources deployed

| Stack | Resource | Name | Key config |
|---|---|---|---|
| OttNetwork | VPC | `OttNetwork-demo/Vpc` | CIDR 10.0.0.0/16 |
| OttNetwork | Subnets | PublicSubnet1/2, PrivateSubnet1/2 | Across 2 AZs |
| OttNetwork | VPC Endpoints | 13 endpoints | 2 Gateway, 11 Interface |
| OttNetwork | Security Groups | glue, lambda, endpoints | No inbound; HTTPS egress to VPC CIDR only |
| OttStorage | KMS CMK | `ott-s3-key` | Annual rotation, 7-day deletion window |
| OttStorage | KMS CMK | `ott-kinesis-key` | Kinesis + Firehose |
| OttStorage | KMS CMK | `ott-sns-key` | SNS anomaly topic |
| OttStorage | KMS CMK | `ott-dynamodb-key` | DynamoDB tables |
| OttStorage | S3 Bucket | `ott-search-{account}-demo` | SSE-KMS, versioning, CloudTrail logging |
| OttStorage | CloudTrail | `ott-search-trail` | Multi-region, log file validation |
| OttStorage | GuardDuty | detector | ENABLED, 6-hour finding frequency |

---

## Screenshot guidance

**Screenshot 1 — VPC endpoints**
Navigate to: **VPC Console → Endpoints** — filter by VPC `OttNetwork-demo/Vpc`.
Capture all 13 endpoints with State: available.
Save as `workshop/static/images/4.1-vpc-endpoints.png`.

**Screenshot 2 — S3 bucket properties**
Navigate to: **S3 Console → `ott-search-{account}-demo` → Properties tab**.
Capture the Default encryption row showing `aws:kms` and Versioning row showing `Enabled`.
Save as `workshop/static/images/4.1-s3.png`.
