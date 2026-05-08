---
title: "3. Architecture Overview"
weight: 30
chapter: true
pre: "<b>3. </b>"
---

# Architecture Overview

## Full pipeline

```
log_search Parquet files (S3)
         │
         ▼
  replay-producer Lambda            ← enriches: derived_genre, platform_group
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
DynamoDB ott-anomaly-events     Glue Crawler + Glue ETL (18 steps, 260s)
EventBridge → SNS email         S3 curated/search_enriched/ (1,334,620 rows)
≤ 5 min alert latency               │
                                    ▼
                             Step Functions (01:30 UTC+7 daily, 10m10s)
                                    │
                                    ▼
                             Athena CTAS → S3 gold/keyword_trends/ (4,761 rows)
                             QuickSight SPICE (4 dashboards, refresh 03:00)
```

## CDK stacks (9 total)

| Iteration | Stack | Key resources |
|---|---|---|
| 0 | OttNetwork, OttStorage | VPC (4 subnets, 13 VPC endpoints), KMS CMKs (4), S3 bucket, CloudTrail, GuardDuty |
| 1 | OttIngestion | Kinesis Data Streams, Kinesis Firehose, replay-producer Lambda |
| 2 | OttETL | Glue ETL job (G.1X, 10 DPU), Glue crawler, curated database |
| 3 | OttCompute | DynamoDB (baseline + anomaly), anomaly-detector Lambda, baseline-updater Lambda, EventBridge, SNS |
| 4 | OttAnalytics | Step Functions state machine, Athena workgroup, gold database |
| 5 | _(QuickSight — manual)_ | 4 SPICE dashboards |
| 6 | OttGovernance | Lake Formation column-level grants (3 IAM roles), Amazon Macie |
| 7 | OttObservability, OttPipeline | CloudWatch composite alarm + dashboard, CodePipeline CI/CD |

## Dual-path design rationale

The anomaly detection requirement demands ≤ 5-minute latency. Glue job startup alone takes 5–10 minutes before processing begins. Kinesis delivers records to the Lambda consumer within seconds of ingestion.

The dual-path design puts each service in the role it was built for:
- **Kinesis + Lambda** — latency-sensitive real-time path
- **Firehose + Glue** — throughput-optimized batch path with full historical context (session windows, repeat-search detection)

Both paths read from the **same** Kinesis stream. An anomaly alert is never delayed by a slow Glue job.

---

## Data-flow schema

### Raw layer — 12 columns

`s3://ott-search-{account}-{env}/raw/events/dt=.../hour=.../derived_genre=.../platform_group=.../`

| Column | Type | Notes |
|---|---|---|
| `eventid` | string | Original |
| `datetime` | string | May contain Arabic-Indic numerals or Buddhist Era year |
| `user_id` | string | Null for ~25.3% of events (unauthenticated) |
| `keyword` | string | Free-text search query |
| `category` | string | `enter` or `quit` |
| `proxy_isp` | string | ISP name from proxy |
| `platform` | string | 35 distinct device strings |
| `networktype` | string | 9 dirty values |
| `action` | string | Event action type |
| `userplansmap` | array | Subscription plan list |
| `derived_genre` | string | **Added by replay-producer** (8 genres + UNKNOWN) |
| `platform_group` | string | **Added by replay-producer** (OTTBox/SmartTV/Android/iOS/Web/Other) |

### Curated layer — 18 columns

`s3://ott-search-{account}-{env}/curated/search_enriched/dt=.../derived_genre=.../`

| Column | Step | Description |
|---|---|---|
| `event_id` | 1 | Renamed eventid |
| `event_ts` | 2 | Parsed timestamp (UTC) after datetime repair |
| `hour_of_day_vn` | 4 | Vietnam-timezone hour (0–23) |
| `user_id_hashed` | 8 | SHA-256(user_id), null-safe |
| `user_is_authenticated` | 7 | user_id IS NOT NULL |
| `session_action` | 5 | `enter` or `quit` |
| `is_search_abandoned` | 6 | category == 'quit' |
| `keyword_norm` | 9 | lower(trim(keyword)) |
| `derived_genre` | 10 | Rule-based classifier + optional LLM fallback |
| `platform_group` | 11 | 35 strings → 6 buckets |
| `network_type_norm` | 12 | 9 values → 5 canonical |
| `isp_segment` | 13 | upper(proxy_isp) |
| `has_premium` | 14 | Any of VIP/HBO GO+/K+/MAX in userplansmap |
| `subscription_count` | 15 | len(userplansmap) |
| `search_session_id` | 16 | SHA-256(user_id\|30min_bucket) |
| `is_repeat_search` | 17 | Same keyword as previous in session (LAG window) |
| `is_cross_partition_date` | 18 | Event year ≠ dt partition year (corruption flag) |

### Gold layer — 16 columns

`s3://ott-search-{account}-{env}/gold/keyword_trends/trend_date=.../derived_genre=.../`

Top-50 keywords per (genre × platform_group) combination, with 7-day rank delta.

| Column | Description |
|---|---|
| `keyword_norm` | Search term |
| `platform_group` | Device class |
| `search_count` | Total searches (enter + quit) |
| `enter_count` | Searches with result selection |
| `abandonment_rate` | 1 − enter_count/search_count |
| `unique_users` | COUNT(DISTINCT user_id_hashed) |
| `rank_today` | Rank within (genre, platform_group) by enter_count |
| `rank_7d_ago` | Same rank 7 days prior (COALESCE 9999 if new entrant) |
| `rank_delta` | rank_7d_ago − rank_today (positive = rising) |
| `trend_date` | Partition key |
| `derived_genre` | Partition key |
