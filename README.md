# OTT Search Trend & Anomaly Pipeline

AWS CDK v2 (Python) pipeline for FPT Play search-log analytics. Ingests 14 days of `log_search`
Parquet data, enriches events, detects keyword-trend anomalies in real time, and surfaces
aggregated Gold-layer trends via Athena + QuickSight.

---

## Architecture overview

```
S3 (raw Parquet)
   │
   ▼
Lambda replay-producer ──► Kinesis Data Streams (2 shards)
                                │                │
                          Firehose (dyn. part.)  └── Lambda anomaly-detector
                                │                        │          │
                           S3 raw/events/         DynamoDB      EventBridge
                                │               anomaly-events  ──► SNS alerts
                           Glue Crawler                │
                                │                  DynamoDB
                           Glue ETL (18 steps)  baseline-stats
                                │                      ▲
                           S3 curated/          Lambda baseline-updater (daily)
                                │
                           Step Functions (daily 01:30 UTC+7)
                          StartCrawler → ETL → Athena CTAS → BaselineUpdater
                                │
                           S3 gold/keyword_trends/ (Parquet, partitioned)
                                │
                           QuickSight (4 dashboards)
```

### Stack dependency chain

```
NetworkStack ──► StorageStack ──► IngestionStack ──► ETLStack ──► ComputeStack ──► AnalyticsStack
                                                                                        │
                                                              GovernanceStack ◄──────────┤
                                                              ObservabilityStack ◄───────┤
                                                              PipelineStack ◄────────────┘
```

---

## Iterations

| # | Stack(s) | Key resources |
|---|----------|---------------|
| 0 | Network, Storage | VPC + 11 endpoints, 4 KMS CMKs, S3 bucket, CloudTrail, GuardDuty, Config |
| 1 | Ingestion | Kinesis stream, Firehose (dynamic partitioning), replay-producer Lambda, Glue raw DB |
| 2 | ETL | Glue crawler, Glue 4.0 ETL job (18-step enrichment), curated DB |
| 3 | Compute | DynamoDB baseline + anomaly tables, anomaly-detector Lambda (Kinesis ESM), baseline-updater Lambda, EventBridge + SNS |
| 4 | Analytics | Step Functions daily pipeline (7 states), Athena workgroup, Gold CTAS, EventBridge schedule |
| 5 | — | QuickSight dashboards (console — no CDK resources) |
| 6 | Governance | Lake Formation column-level grants (3 roles), Amazon Macie weekly PII scan |
| 7 | Observability, Pipeline | 4 CloudWatch alarms + composite, Dashboard, log groups, CodePipeline CI/CD |
| 8 | — | Documentation (this file) |

---

## Deployment

### Prerequisites

- Python ≥ 3.11, Node.js ≥ 18
- `npm install -g aws-cdk`
- AWS credentials configured (`aws configure` or environment variables)

### Bootstrap (once per account/region)

```bash
cdk bootstrap aws://<ACCOUNT_ID>/ap-southeast-1
```

### Synthesise

```bash
python -m venv .venv && .venv/Scripts/activate   # Windows
pip install -r requirements.txt
cdk synth --all --context env=dev --context account=<ACCOUNT_ID>
```

### Deploy (iterative)

```bash
# Iteration 0 — Foundation
cdk deploy OttNetwork-dev OttStorage-dev --context env=dev --context account=<ACCOUNT> --context alert_email=ops@example.com

# Iteration 1 — Ingest
cdk deploy OttIngestion-dev

# Iterations 2–4
cdk deploy OttETL-dev OttCompute-dev OttAnalytics-dev

# Iterations 6–7
cdk deploy OttGovernance-dev OttObservability-dev OttPipeline-dev
```

### Deploy all at once

```bash
cdk deploy --all \
  --context env=dev \
  --context account=<ACCOUNT_ID> \
  --context region=ap-southeast-1 \
  --context alert_email=ops@example.com
```

---

## Configuration (CDK context)

| Key | Default | Description |
|-----|---------|-------------|
| `env` | `dev` | Environment name suffix on all resource names |
| `account` | (required) | AWS account ID |
| `region` | `ap-southeast-1` | AWS region |
| `alert_email` | `""` | SNS subscription email for ops alerts |
| `github_owner` | — | GitHub org/user for CI/CD source |
| `github_repo` | — | GitHub repository name |
| `github_connection` | — | CodeStar Connections ARN |

---

## Replay producer

The `replay-producer` Lambda reads the 28 raw Parquet files from
`s3://<bucket>/raw-source/log_search/`, sorts events by timestamp, and
replays them to Kinesis at 336× speedup (14 days → ~60 min).

Invoke manually after deployment:

```bash
aws lambda invoke \
  --function-name ott-replay-producer-dev \
  --payload '{}' \
  response.json
```

Environment variables (set in CDK):

| Variable | Description |
|----------|-------------|
| `STREAM_NAME` | Kinesis stream name |
| `SPEEDUP_FACTOR` | Default 336 |
| `PARQUET_S3_PREFIX` | `s3://<bucket>/raw-source/log_search/` |

---

## Genre taxonomy

| Code | Description |
|------|-------------|
| `NHAC` | Music / karaoke |
| `THE_THAO` | Sports |
| `ANIME` | Anime / animation |
| `PHIM_TRUNG` | Chinese / Korean drama |
| `PHIM_VIET` | Vietnamese drama |
| `PHIM_AU_MY` | Western / Hollywood |
| `TRUYEN_HINH` | Live TV / news channels |
| `UNKNOWN` | Unclassified (LLM fallback optional) |

Classification priority: THE_THAO > ANIME > NHAC > TRUYEN_HINH > PHIM_AU_MY > PHIM_TRUNG > PHIM_VIET.

---

## Data quality notes

Raw `log_search` data (28 Parquet files, June–July 2022, 82 350-row sample) contains:

| Issue | Field | Fix |
|-------|-------|-----|
| Arabic-Indic numerals | `datetime` | `str.translate()` in clean_datetime UDF |
| Buddhist calendar year (2565 → 2022) | `datetime` | Prefix substitution |
| Corrupt year 0004 | `datetime` | Drop rows with `year < 2015` |
| `category` is session state, not genre | — | Genre derived from `keyword_norm` |

---

## Lake Formation access matrix

| Role | Databases | Tables | Columns |
|------|-----------|--------|---------|
| `ott-data-engineering` | raw, curated, gold | ALL | ALL |
| `ott-analyst` | curated, gold | ALL | ALL |
| `ott-marketing` | gold | `keyword_trends` only | `trend_date, derived_genre, platform_group, network_type_norm, isp_segment, keyword_norm, search_count, enter_count, abandonment_rate, rank_today, rank_delta` |

Excluded from marketing: `user_id_hashed`, `unique_users`, `authenticated_rate`.

---

## Anomaly detection

The `anomaly-detector` Lambda processes each Kinesis batch:

1. Groups `enter` events by `(derived_genre, hour_of_day_vn)`
2. Fetches 7-day rolling `(mean, std)` from DynamoDB `ott-baseline-stats`
3. Computes z-score = `(observed − mean) / max(std, 1)`
4. If `|z| > 3.0`: publishes `SearchAnomalyDetected` to EventBridge → SNS alert

Baseline is refreshed nightly at 01:00 UTC+7 by the `baseline-updater` Lambda.

---

## CloudWatch alarms

| Alarm | Metric | Threshold | Action |
|-------|--------|-----------|--------|
| `anomaly-score-breach` | `OTT/SearchPipeline / AnomalyScore` (max) | > 3.0 | SNS ops |
| `glue-job-failure` | `OTT/SearchPipeline / DailyPipelineSuccess` (sum 26 h) | < 1 | SNS ops |
| `lambda-error-rate` | Lambda Errors/Invocations (15 min) | > 5 % | SNS ops |
| `kinesis-iterator-age` | Kinesis IteratorAgeMilliseconds (max) | > 60 000 ms | SNS ops |
| `pipeline-health` | Composite OR of above four | any | SNS ops |

---

## Cost estimate (dev, ap-southeast-1, ~60-min replay)

| Service | Estimate |
|---------|----------|
| Kinesis Data Streams (2 shards, 24 h) | ~$0.03 |
| Kinesis Firehose | ~$0.01 |
| Glue ETL (G.1X × 2 DPUs, 30 min) | ~$0.15 |
| Lambda (replay + anomaly + updater) | < $0.01 |
| Athena (gold CTAS, ~1 GB scan) | ~$0.005 |
| DynamoDB (on-demand, < 1M writes) | ~$0.02 |
| Step Functions (< 1000 state transitions) | < $0.01 |
| S3 (< 1 GB) | < $0.03 |
| **Total** | **< $0.30** |

---

## Destroy

```bash
cdk destroy --all --context env=dev --context account=<ACCOUNT_ID>
```

> Note: KMS keys have a 7-day pending deletion window (`RemovalPolicy.RETAIN`).
> S3 bucket must be emptied manually before stack deletion if it contains data.
