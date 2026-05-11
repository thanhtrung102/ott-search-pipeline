---
title: "5. Results"
weight: 50
chapter: true
pre: "<b>5. </b>"
---

# Results

All metrics verified from the deployed `demo` environment on **2026-05-10**, June 2022 dataset.

---

## Pipeline run metrics

| Metric | Verified value |
|---|---|
| Source Parquet files | 14 daily folders, **1,146,996** source rows |
| Replay duration | ~18 minutes (14 sequential Lambda invocations) |
| Kinesis records published | **1,146,996** (sequential replay — no throttling) |
| Firehose S3 partitions | 14 date × 24 hour × 8 genre × 5 platform paths |
| Curated records after ETL | **992,650** valid (0 cross-partition date rows) |
| Gold keyword_trends rows | **6,908** (top-50 per genre × platform combination, 14 dates) |
| DynamoDB baseline slots | **192** (8 genres × 24 hours) |
| DynamoDB anomaly events | **69,306** total (69,208 DROP, 98 SPIKE) |
| Step Functions pipeline duration | **~730 seconds (~12 minutes)** |
| Glue ETL duration | **~200 seconds** (G.1X, 10 DPU) |
| Anomaly alert latency | **≤ 5 minutes** (z-score > 3 → EventBridge → SNS) |
| CloudWatch DailyPipelineSuccess | **1** (per successful nightly run; verified 2026-05-10) |

---

## What the data revealed

### Search abandonment by genre × platform

Abandonment rate = fraction of search events where the user typed a query, received results, and left without clicking anything.

| Genre | Platform | Abandonment rate |
|---|---|---|
| **UNKNOWN** | **SmartTV** | **30.87%** ← highest by volume × abandonment impact |
| UNKNOWN | Android | 13.91% |
| UNKNOWN | OTTBox | ~13.1% |
| UNKNOWN | iOS | ~3.8% |
| THE_THAO | OTTBox | ~6.2% |
| THE_THAO | Android | ~5.5% |
| THE_THAO (overall) | all platforms | 7.02% |

**Primary finding:** UNKNOWN × SmartTV at 30.87% — nearly 1-in-3 SmartTV free-text searches produce no useful result. This is a **classifier coverage problem**. The search index has the content; the rule-based LUT + regex covers only ~43.5% of keyword volume. The 56.5% falling to UNKNOWN on SmartTV see no genre match and abandon at 3× the rate of classified searches.

The fix is expanding the keyword classifier, not adding content to the platform — the content already exists.

### Music search diversity

NHAC (music) has the most distinct keyword vocabulary: **392 unique keyword slots** across 14 days and 5,899 total searches in the gold layer. Bolero keywords consistently rank at the top across all platform groups, suggesting broad cross-demographic appeal rather than a device-class preference.

### Classifier coverage

| Category | Keyword volume | % of total |
|---|---|---|
| Classified (8 known genres) | ~33,856 | **43.5%** |
| UNKNOWN | 43,884 | **56.5%** |

The UNKNOWN bucket is an explicit acknowledgment of classification uncertainty — not a misclassification. The LLM fallback path (`LLM_ENABLED=true`) handles the long tail in production but requires a Secrets Manager API key and is disabled in this demo.

### Anomaly detection calibration

With the baseline seeded from June 2022 data before the full replay, the anomaly detector correctly flagged:
- **69,208 DROP** anomalies — expected: observed counts were lower than the pre-seeded baseline for most (genre, hour) slots during replay
- **98 SPIKE** anomalies — genuine high-volume events including the injected THE_THAO × hour 20 spike

After the full June baseline is established via the nightly baseline-updater, future replays produce calibrated z-scores.

### ISP × platform alignment

VNPT subscribers show disproportionate SmartTV engagement — consistent with VNPT's fixed broadband market position. Viettel subscribers show higher Android proportion — consistent with Viettel's mobile network dominance. This distribution is consistent with known Vietnamese telecom market structure.

---

## The pipeline answers the three questions

| Question | Answer |
|---|---|
| **What is trending?** | Bolero keywords rising in NHAC/SmartTV; sports search volume concentrated at hours 20–22 |
| **Is something wrong right now?** | Z-score alert delivered within 5 minutes of anomaly injection — before any user complaint could arrive |
| **Where is the search experience failing?** | UNKNOWN × SmartTV has 30.87% abandonment — highest by volume-weighted impact. Fix: improve keyword classifier coverage, not device-class content |

---

## Three questions a technical interviewer will ask

**"How do you know the genre classification is accurate?"**

The UNKNOWN bucket at 56.5% is the honest answer. The LUT covers known high-frequency terms with high precision. The regex patterns cover transliterations and partial matches. The LLM fallback handles the long tail in production. UNKNOWN is not a silent misclassification — it is an explicit "I don't know" that is more informative than a classifier that silently assigns wrong genres.

**"What happens if the Glue job fails?"**

Step Functions catches `States.ALL` errors in `StartETLJob` and routes to `PipelineFailure`, which publishes to the ops SNS topic. The `ott-glue-job-failure-demo` CloudWatch alarm fires if `DailyPipelineSuccess < 1` in a 26-hour window. The next day's run reprocesses missed partitions via the pushdown predicate (not a bookmark), and the CTAS is idempotent (drop + recreate).

**"Why Kinesis instead of just Glue reading from S3 directly?"**

Anomaly detection needs ≤5-minute latency. Glue job startup alone is 5–10 minutes, before any processing begins. Kinesis delivers records to the Lambda consumer within seconds of ingestion. The dual-path design uses Kinesis for the latency-sensitive path and Glue for the throughput-optimized batch path — each service in the role it was built for.
