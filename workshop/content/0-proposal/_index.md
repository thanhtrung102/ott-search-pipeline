---
title: "Project Proposal"
weight: 1
pre: "<b>0. </b>"
---

# Project Proposal — OTT Search Analytics Pipeline

## Overview

FPT Play processes approximately 82,350 search events per day from Vietnamese OTT viewers. The raw event log exists in a data warehouse but refreshes once per day in batch — meaning a prime-time search failure at 20:00 is not visible until 08:00 the next morning. This proposal describes a dual-path AWS analytics pipeline that closes that gap: surfacing anomalies within 5 minutes and delivering keyword trend rankings by 02:00 each morning.

The pipeline is built entirely as CDK Python, reproducible from a single `cdk deploy --all` command.

---

## Problem Statement

The Product Analytics team at FPT Play has three unanswered questions about their search experience:

| Question | Current answer | Target answer |
|---|---|---|
| What is trending? | Unknown until the next day's batch | Top-50 keywords per genre and platform, with 7-day rank deltas, by 02:00 each morning |
| Is something wrong right now? | Unknown until user complaints arrive | Z-score alert within 5 minutes of a genre's search rate deviating ≥ 3σ |
| Where is the search experience failing? | Cannot be answered from raw logs without manual querying | Abandonment rate heatmap by genre × platform, refreshed daily |

The June 2022 dataset confirms this matters: SmartTV users searching for content that matches no known genre pattern are abandoning at **30.95%** — nearly one in three. The search index likely has the content; the classifier cannot map the free-text query to it. Without the pipeline, this signal is buried in 82,000 daily event rows that no business user can query.

---

## User Stories

> *As a Marketing analyst, I want to see which keywords are trending in music searches on SmartTV this week versus last week, so that I can recommend content acquisition targets to the programming team.*

> *As an on-call engineer, I want to receive an alert within 5 minutes when search volume for any genre deviates abnormally, so that I can investigate a CDN failure, search index outage, or viral event before user complaints arrive.*

> *As a Data Governance officer, I want Marketing analysts to query keyword trend data without accessing individual user activity or authentication status, so that the platform complies with its data privacy commitments.*

---

## Architecture

The pipeline uses a **dual-path design** deployed across 9 CDK stacks in ap-southeast-1:

```
log_search Parquet files (S3)
         │
         ▼
  replay-producer Lambda            ← enriches: genre, platform
         │
         ▼
  Kinesis Data Streams (2 shards, KMS, 24h retention)
         │
    ┌────┴──────────────────────────┐
    │  Real-time path               │  Batch path
    ▼                               ▼
anomaly-detector Lambda         Kinesis Firehose → S3 raw/events/
(z-score vs DynamoDB baseline)  (JSON→Parquet, dynamic partitioning)
    │                               │
    ▼                               ▼
DynamoDB ott-anomaly-events     Glue Crawler + Glue ETL (18 steps)
EventBridge → SNS email         S3 curated/search_enriched/
≤ 5 min alert latency               │
                                    ▼
                             Step Functions nightly orchestrator
                             (01:30 UTC+7 daily)
                                    │
                                    ▼
                             Athena CTAS → S3 gold/keyword_trends/
                             QuickSight SPICE (4 dashboards)
```

**CDK stacks (9 total):**

| Iteration | Stack | Key resources |
|---|---|---|
| 0 | OttNetwork, OttStorage | VPC + subnets, KMS CMKs (4 keys), S3 bucket, CloudTrail, GuardDuty |
| 1 | OttIngestion | Kinesis Data Streams, Kinesis Firehose, replay-producer Lambda, Glue raw database |
| 2 | OttETL | Glue ETL job (G.1X, 10 DPU), Glue crawler, Glue curated database |
| 3 | OttCompute | DynamoDB baseline + anomaly tables, anomaly-detector Lambda (Kinesis ESM), baseline-updater Lambda, EventBridge, SNS |
| 4 | OttAnalytics | Step Functions state machine, Athena workgroup, Glue gold database |
| 5 | _(QuickSight — manual setup)_ | 4 SPICE dashboards |
| 6 | OttGovernance | Lake Formation column-level grants (3 IAM roles), Amazon Macie |
| 7 | OttObservability, OttPipeline | CloudWatch alarms + composite alarm + dashboard, CodePipeline CI/CD |

**Design rationale — why not Glue-only?**

The anomaly detection requirement demands ≤5-minute alert latency. Glue job startup alone is 5–10 minutes. Kinesis delivers records to the Lambda consumer within seconds of ingestion. The dual-path design uses Kinesis for the latency-sensitive path and Glue for the throughput-optimized batch path — each service in the role it was designed for.

---

## Implementation Timeline

The pipeline was designed and built in 8 iterations over 8 weeks:

| Week | Iteration | Deliverable |
|---|---|---|
| 1 | Foundation (Iter 0) | VPC, KMS keys, S3 bucket, CloudTrail, GuardDuty baseline. `cdk deploy OttNetwork OttStorage` |
| 2 | Ingest (Iter 1) | Kinesis stream + Firehose, replay-producer Lambda, raw Glue database. First records land in S3 |
| 3 | ETL (Iter 2) | Glue ETL job — 18-step enrichment: datetime repair, SHA-256 user hashing, repeat-search detection, genre classification |
| 4 | Anomaly detection (Iter 3) | DynamoDB baseline, anomaly-detector Lambda, EventBridge → SNS. First z-score alerts |
| 5 | Orchestration + Gold (Iter 4) | Step Functions nightly orchestrator, Athena CTAS, keyword_trends gold table |
| 6 | Dashboards (Iter 5) | QuickSight SPICE: Search Quality, Anomaly Timeline, Keyword Leaderboard, ISP × Platform |
| 7 | Governance (Iter 6) | Lake Formation column-level grants, Macie PII scanning, governance demo |
| 8 | Observability + CI/CD (Iter 7) | CloudWatch composite alarm, CodePipeline with cfn-nag security gate |

---

## Budget

**Demo run (June 2022 dataset, 1.13M source records, verified 2026-05-08):**

| Component | Cost |
|---|---|
| Kinesis Data Streams (2 shards, ~5 min replay) | ~$0.05 |
| Kinesis Firehose (JSON→Parquet, ~331K records) | ~$0.05 |
| Glue ETL job (10 DPU × G.1X × 260 sec) | $0.32 |
| Athena queries (CTAS + validation, ~2 GB scanned) | ~$0.01 |
| Step Functions (1 execution, standard workflow) | ~$0.01 |
| KMS (4 CMKs × API calls) | ~$0.02 |
| Lambda (3 functions, minimal invocations) | ~$0.00 |
| S3 storage (raw + curated + gold, ~10 GB) | ~$0.23 |
| QuickSight Standard (1 month) | $9.00 |
| **Demo total** | **~$9.60** |

**Estimated monthly production cost (production scale, 30 days):**

| Service | Monthly estimate |
|---|---|
| Kinesis Data Streams (2 shards) | ~$15 |
| Kinesis Firehose | ~$3 |
| Glue ETL (daily, 10 DPU × 260 sec) | ~$3 |
| Step Functions (30 executions) | ~$1 |
| DynamoDB (on-demand) | ~$1 |
| Athena (~30 CTAS + queries) | ~$2 |
| Lambda | ~$1 |
| S3 (~50 GB/month growth) | ~$5 |
| CloudWatch (logs, metrics, alarms) | ~$5 |
| KMS (4 CMKs) | ~$4 |
| GuardDuty | ~$10 |
| Macie (weekly scan) | ~$5 |
| CloudTrail | ~$2 |
| QuickSight Standard | ~$9 |
| **Monthly total** | **~$66** |

---

## Verified Results (June 2022 dataset)

All metrics verified against the deployed `demo` environment on 2026-05-08:

| Metric | Value |
|---|---|
| Source events | 1,146,996 (14 daily Parquet folders) |
| Kinesis records published | ~331,000 (throttled: 2 shards × 14 concurrent Lambdas) |
| S3 raw partitions | 14 date × 24 hour × 8 genre × 5 platform |
| Curated records after ETL | **1,334,620** total; **1,333,242** valid (cross-partition excluded) |
| Gold keyword_trends rows | **4,761** (top-50 per genre × platform) |
| DynamoDB baseline slots | **192** (8 genres × 24 hours) |
| DynamoDB anomaly events | **65,148** (65,051 DROP, 97 SPIKE) |
| Step Functions pipeline duration | **10 minutes 10 seconds** |
| Glue ETL duration | **260 seconds** (G.1X, 10 DPU) |
| Anomaly alert latency | **≤5 minutes** (z-score >3 → EventBridge → SNS) |

**Key findings from the data:**

| Finding | Detail |
|---|---|
| Highest abandonment | UNKNOWN × SmartTV: **30.95%** (362,253 events) |
| Highest genre by volume | UNKNOWN: 1,239,982 events (93.3% of curated) |
| Music keyword diversity | NHAC: 668 distinct keyword slots in gold layer |
| Classifier coverage | 44.5% of keyword volume classified; 55.5% falls to UNKNOWN |
| Sports abandonment | THE_THAO overall: **7.16%**; OTTBox: **8.01%** |
| Pipeline reliability | DailyPipelineSuccess metric: 1 hit on 2026-05-08 |

---

## Success Metrics

| Metric | Target | Verified result |
|---|---|---|
| Anomaly detection latency | ≤ 5 minutes | ✓ (EventBridge → SNS in seconds after z>3) |
| Daily pipeline completion | By 02:00 UTC+7 | ✓ (10 min 10 sec total, scheduled 01:30) |
| Curated enrichment coverage | All 18 columns populated | ✓ (verified via Athena validation gate) |
| Governance boundary | Marketing role cannot access curated layer | ✓ (IAM S3 scope + Lake Formation enforced) |
| Infrastructure reproducibility | Single `cdk deploy --all` | ✓ (9 stacks, ~15 minutes, verified) |
| Glue ETL runtime | < 10 minutes | ✓ (260 seconds) |
| Gold layer keyword slots | ≥ 4,000 rows (top-50 × 8 genres × N platforms) | ✓ (4,761 rows) |

---

## Team

| Role | Responsibility |
|---|---|
| Builder / Author | Pipeline architecture, CDK implementation, Glue ETL, workshop documentation |
| Mentor (Solutions Architect) | Architecture review, AWS Well-Architected guidance, cost optimization |

---

## Scope Boundaries

**In scope:** Real-time anomaly detection, batch keyword trend ranking, curated data layer with governance, CI/CD pipeline, full workshop documentation.

**Out of scope (future work):**
- LLM fallback genre classifier (code present, disabled in demo — requires Secrets Manager + Gemini API key)
- Lucie 7B or larger foundation model for keyword classification
- QuickSight ML Insights (AutoML forecasting on keyword trends)
- Multi-region replication
