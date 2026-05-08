---
title: "4. Implementation"
weight: 40
chapter: true
pre: "<b>4. </b>"
---

# Implementation

Eight iterations, each adding one CDK stack (or stack pair). Deploy in order — each iteration depends on resources created by the previous one.

## Environment setup

Set these once. All commands in sections 4.1–4.8 reference them:

```bash
export CDK_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_ENV=dev
export ALERT_EMAIL=your@email.com
export BUCKET="ott-search-${CDK_ACCOUNT}-${CDK_ENV}"
```

## Iteration overview

| Section | Stack(s) | What it deploys | Approx. time |
|---|---|---|---|
| [4.1 Foundation]({{% relref "4.1-foundation" %}}) | OttNetwork + OttStorage | VPC, KMS keys, S3, CloudTrail, GuardDuty | ~8 min |
| [4.2 Ingest]({{% relref "4.2-ingest" %}}) | OttIngestion | Kinesis, Firehose, replay-producer Lambda | ~6 min |
| [4.3 ETL]({{% relref "4.3-etl" %}}) | OttETL | Glue ETL job (18 steps), crawler, curated database | ~3 min |
| [4.4 Anomaly Detection]({{% relref "4.4-anomaly" %}}) | OttCompute | DynamoDB, anomaly-detector, baseline-updater, SNS | ~5 min |
| [4.5 Orchestration + Gold]({{% relref "4.5-orchestration" %}}) | OttAnalytics | Step Functions, Athena workgroup, gold database | ~3 min |
| [4.6 QuickSight]({{% relref "4.6-quicksight" %}}) | _(manual)_ | SPICE datasets, 4 dashboards | ~20 min |
| [4.7 Governance]({{% relref "4.7-governance" %}}) | OttGovernance | Lake Formation grants, Macie | ~5 min |
| [4.8 CI/CD + Observability]({{% relref "4.8-cicd" %}}) | OttObservability + OttPipeline | CloudWatch alarms + dashboard, CodePipeline | ~10 min |

## Verified full-pipeline run — June 2022 dataset (2026-05-08)

| Step | Duration | Verified output |
|---|---|---|
| Dataset upload | — | 14 S3 prefixes, 1,146,996 source rows |
| Kinesis replay (14 concurrent Lambdas) | ~5 min | ~331,000 records published (throttled: 2 shards × 14 concurrent) |
| Firehose → S3 raw | parallel with replay | 14 date × 24 hour × 8 genre × 5 platform partitions |
| Glue crawler | ~90 sec | 14+ partitions registered in `ott_search_raw.events` |
| Glue ETL job | **260 seconds** (G.1X, 10 DPU) | **1,334,620** curated records; **1,333,242** valid |
| Step Functions nightly pipeline | **610 seconds** | **4,761** gold keyword_trends rows |
| DynamoDB baseline seeding | ~10 sec | **192** slots (8 genres × 24 hours) |
| DynamoDB anomaly events | — | **65,148** total (65,051 DROP, 97 SPIKE) |
| CloudWatch DailyPipelineSuccess | — | **1** (SUCCEEDED) |
