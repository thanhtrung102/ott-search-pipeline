---
title: "5. Results"
weight: 50
chapter: true
pre: "<b>5. </b>"
---

# Results

## What was built

The pipeline ingests Vietnamese OTT search behavior data through a dual-path architecture deployed entirely from CDK. The real-time path detects anomalies in genre search volume within 5 minutes of occurrence. The batch path produces daily keyword trend rankings with 7-day rank deltas and search abandonment rates by genre and platform, completed by 02:00 each morning.

All infrastructure is defined as CDK Python and is reproducible from `cdk deploy --all` in under 15 minutes (excluding data upload and Glue ETL runtime).

---

## Pipeline run metrics — June 2022 dataset

| Metric | Value |
|---|---|
| Source parquet files | 14 daily folders, 1,146,996 source rows |
| Replay duration | ~5 minutes (14 concurrent Lambda invocations) |
| Kinesis records published | ~331,000 (throttled — 2 shards × 14 concurrent Lambdas) |
| Firehose S3 partitions | 14 date partitions × 24 hours × 8 genres × 5 platforms |
| Curated records after ETL | **1,334,620** across 14 days (includes anomaly injection data from Section 4.4) |
| Gold keyword_trends rows | **4,761** (top-50 per genre × platform combination) |
| DynamoDB baseline slots | **192** (8 genres × 24 hours) |
| DynamoDB anomaly events | **65,148** total (65,051 DROP, 97 SPIKE) |
| Step Functions pipeline duration | **10 minutes 11 seconds** |

---

## What the data revealed

**Free-text search abandonment:** The UNKNOWN category on SmartTV exhibits the highest search abandonment rate at approximately **30.95%** — nearly one in three SmartTV users who typed a free-text search matching no known genre got no useful result and quit. Android × UNKNOWN is **13.83%**. THE_THAO (sports) × OTTBox is **8.01%**, mid-range relative to other genre × platform pairs. The primary product signal is classifier coverage: the rule-based LUT covers ~44.5% of keyword volume, and the high SmartTV abandonment on UNKNOWN reflects unresolvable long-tail searches on the living-room device class, not a content gap on a specific device class.

**Music dominates by normalized volume:** NHAC (music) has 668 keyword slots and 14,371 total searches in the gold layer — the most distinct keyword vocabulary of any genre. Bolero keywords appear consistently across all platform groups, suggesting a broad, cross-demographic audience rather than a specific device-class preference.

**ISP × platform alignment:** The ISP × platform distribution is consistent with known Vietnamese telecom market structure. VNPT subscribers — who are predominantly on fixed broadband — show disproportionate SmartTV engagement. Viettel subscribers — predominantly mobile network customers — show higher Android proportions.

**Classifier coverage:** The UNKNOWN bucket contains 97,451 search events (55.5% of classified searches in the gold layer). This is the honest measure of classifier coverage. The rule-based classifier + LUT covers ~44.5% of keyword volume. The LLM fallback path (disabled in this demo) handles the long tail in production.

**Anomaly detection sensitivity:** With the baseline seeded from June 2022 data, the anomaly detector flagged 65,051 DROP anomalies during the replay. These are expected — the initial baseline was computed from a subset of the data (before re-running the full month), so the detector correctly identified that observed counts were lower than the seeded baseline for most (genre, hour) slots. The 97 SPIKE anomalies represent genuine high-volume events in the dataset. After the full June baseline is established via the nightly updater, future replay runs would produce calibrated z-scores.

---

## What the architecture enables

Marketing can query keyword trends without accessing individual user data — demonstrated by a Lake Formation `Access denied on table: ott_search_curated.search_enriched` on the curated layer and a successful query returning keyword_norm, abandonment_rate, and rank_delta on the gold layer — using the same IAM role.

The pipeline answers the three questions from the Introduction:

| Question | Answer |
|---|---|
| What is trending? | Bolero keywords rising in NHAC/SmartTV; sports search volume concentrated at hours 20–22 |
| Is something wrong right now? | Z-score alert delivered within 5 minutes of anomaly injection — before any user complaint |
| Where is the search experience failing? | UNKNOWN × SmartTV has 30.95% abandonment — highest by volume-weighted impact; Android × UNKNOWN is 13.83%; fix is improving keyword classifier coverage, not device-class content |

---

## Three questions a technical interviewer will ask

**"How do you know the genre classification is accurate?"**

The UNKNOWN bucket is the honest answer. 55.5% of keyword volume is unclassified by the rule-based system. The LUT (from `key_search_by_category.csv`) covers known high-frequency terms with high precision. The regex patterns cover transliterations and partial matches. The LLM fallback handles the long tail in production. The UNKNOWN bucket is not a failure — it is an explicit acknowledgment of classification uncertainty, which is more informative than a classifier that silently misclassifies.

**"What happens if the Glue job fails?"**

The Step Functions state machine catches any `States.ALL` error in `StartETLJob` and routes to `PipelineFailure`, which publishes to the ops SNS topic. The CloudWatch alarm `glue-job-failure-dev` fires if `DailyPipelineSuccess < 1` in a 26-hour window. The next day's run reprocesses any missed partitions because the job reads with a pushdown predicate (not a bookmark), and the CTAS is idempotent (drop + recreate).

**"Why Kinesis and not just Glue reading from S3 directly?"**

The anomaly detection requirement demands ≤5-minute alert latency. Glue reads from S3 in batch — minimum practical latency is 5–10 minutes for job startup alone, before any processing. Kinesis delivers records to the Lambda consumer within seconds of ingestion. The dual-path architecture uses Kinesis for the latency-sensitive path and S3+Glue for the throughput-optimized batch path — each service in the role it is designed for.
