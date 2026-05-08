---
title: "4. Implementation"
weight: 40
chapter: true
pre: "<b>4. </b>"
---

# Implementation

The pipeline is built in 8 iterations, each deploying a self-contained CDK stack. Deploy them in order — each iteration depends on the outputs of the previous one.

| Iteration | CDK Stacks deployed | Time |
|---|---|---|
| [4.1 Foundation]({{% relref "4.1-foundation" %}}) | OttNetwork + OttStorage | ~8 min |
| [4.2 Ingest Layer]({{% relref "4.2-ingest" %}}) | OttIngestion | ~4 min |
| [4.3 ETL Layer]({{% relref "4.3-etl" %}}) | OttETL | ~3 min |
| [4.4 Anomaly Detection]({{% relref "4.4-anomaly" %}}) | OttCompute | ~4 min |
| [4.5 Orchestration + Gold]({{% relref "4.5-orchestration" %}}) | OttAnalytics | ~3 min |
| [4.6 QuickSight]({{% relref "4.6-quicksight" %}}) | Manual console setup | ~15 min |
| [4.7 Governance]({{% relref "4.7-governance" %}}) | OttGovernance | ~5 min |
| [4.8 CI/CD]({{% relref "4.8-cicd" %}}) | OttObservability + OttPipeline | ~10 min |

**Total infrastructure deploy time:** approximately 52 minutes for the full stack. The replay producer run (Section 4.2) adds ~10 minutes of Lambda execution time.

---

## Environment variables

Set these once before starting. They are used in every `cdk deploy` command:

```bash
export AWS_DEFAULT_REGION=ap-southeast-1
export CDK_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_ENV=dev
export ALERT_EMAIL=your@email.com

# Shorthand used throughout this workshop:
BUCKET="ott-search-${CDK_ACCOUNT}-${CDK_ENV}"
```

## CDK bootstrap (one-time)

Run this once per AWS account/region combination:

```bash
cdk bootstrap aws://${CDK_ACCOUNT}/ap-southeast-1
```

Expected output ends with: `✅  Environment aws://{account}/ap-southeast-1 bootstrapped.`
