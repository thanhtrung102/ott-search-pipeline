---
title: "4.5 Orchestration + Gold Layer (Iteration 4)"
weight: 45
pre: "<b>4.5 </b>"
---

## What this deploys

- **Step Functions state machine** `ott-daily-pipeline-dev` — nightly orchestrator, scheduled 01:30 UTC+7 (18:30 UTC), 3-hour timeout
- **Athena Workgroup** `ott-analytics-dev` — 10 GB scan cap, SSE-S3 query results
- **Glue Database** `ott_search_gold` — catalog for keyword_trends gold table
- **EventBridge Rule** — daily 18:30 UTC trigger

---

## Deploy

```bash
cdk deploy OttAnalytics-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --context gold_date_filter="dt >= '2022-06-01'" \
  --require-approval never
```

{{% notice warning %}}
The `gold_date_filter` is baked into the Step Functions ASL at CDK synth time. For the June 2022 historical demo, pass `--context gold_date_filter="dt >= '2022-06-01'"`. The production default is `dt >= date_add('day', -1, current_date)` — omitting this override produces an empty gold table when running against historical data.
{{% /notice %}}

**Deploy time:** approximately 3 minutes.

---

## State machine flow

```
StartCrawler
    │
    ▼
WaitForCrawler (30 sec)
    │
    ▼
CheckCrawlerStatus (GetCrawler)
    │
    ▼
IsCrawlerDone? ──No──► WaitForCrawler (loop)
    │ Yes
    ▼
StartETLJob (.sync — blocks until SUCCEEDED/FAILED)
    │
    ▼
RepairCuratedTable (MSCK REPAIR TABLE)
    │
    ▼
DropGoldTable (DROP TABLE IF EXISTS ott_search_gold.keyword_trends)
    │
    ▼
RunAthenaGoldCTAS (CREATE TABLE ... AS SELECT ...)
    │
    ▼
InvokeBaselineUpdater (.waitForTaskToken)
    │
    ▼
PipelineSuccess (PutMetricData DailyPipelineSuccess=1)
```

Any state (except `StartCrawler`) routes to `PipelineFailure` on error, which publishes to the ops SNS topic.

**Why `DropGoldTable` before CTAS?** Athena CTAS fails if the target table exists. Dropping first makes the pipeline idempotent — re-runnable on the same day without manual cleanup.

**Why `MSCK REPAIR TABLE` after ETL?** The Glue PySpark writer writes partitions directly to S3, bypassing the Glue catalog. `MSCK REPAIR TABLE` registers the new S3 partitions so Athena can read them in the CTAS.

---

## Run the pipeline manually

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
SM_ARN="arn:aws:states:ap-southeast-1:${ACCOUNT}:stateMachine:ott-daily-pipeline-${CDK_ENV}"

EXEC_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn ${SM_ARN} \
  --name "manual-$(date +%Y%m%dT%H%M%S)" \
  --query executionArn \
  --output text)

echo "Execution: ${EXEC_ARN}"
```

Poll until complete:
```bash
until [[ "$(aws stepfunctions describe-execution \
  --execution-arn ${EXEC_ARN} \
  --query status --output text)" != "RUNNING" ]]; do
  CURRENT=$(aws stepfunctions get-execution-history \
    --execution-arn ${EXEC_ARN} \
    --reverse-order --max-results 1 \
    --query "events[0].stateEnteredEventDetails.name" \
    --output text 2>/dev/null)
  echo "Current state: ${CURRENT}"
  sleep 30
done

aws stepfunctions describe-execution \
  --execution-arn ${EXEC_ARN} \
  --query '{Status:status,Start:startDate,Stop:stopDate}' \
  --output json
```

**Verified output (2026-05-08):**
```json
{
  "Status": "SUCCEEDED",
  "Start": "2026-05-08T...",
  "Stop": "2026-05-08T..."
}
```

**Verified pipeline duration: 610 seconds (10 minutes 10 seconds)**

---

## Gold CTAS SQL

```sql
CREATE TABLE ott_search_gold.keyword_trends
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['trend_date', 'derived_genre'],
  external_location = 's3://{bucket}/gold/keyword_trends/'
) AS
WITH ranked_today AS (
  SELECT
    CAST(dt AS DATE)                                           AS trend_date,
    derived_genre, platform_group, network_type_norm,
    isp_segment, keyword_norm,
    COUNT(*)                                                   AS search_count,
    COUNT(*) FILTER (WHERE session_action = 'enter')           AS enter_count,
    1.0 - (COUNT(*) FILTER (WHERE session_action = 'enter')
           * 1.0 / NULLIF(COUNT(*), 0))                       AS abandonment_rate,
    COUNT(DISTINCT user_id_hashed)                             AS unique_users,
    COUNT(DISTINCT user_id_hashed) * 1.0 / NULLIF(COUNT(*),0) AS authenticated_rate,
    AVG(CAST(is_repeat_search AS INT))                         AS repeat_search_rate,
    AVG(CAST(has_premium AS INT))                              AS premium_search_rate,
    RANK() OVER (
      PARTITION BY derived_genre, platform_group
      ORDER BY COUNT(*) FILTER (WHERE session_action = 'enter') DESC
    ) AS rank_today
  FROM ott_search_curated.search_enriched
  WHERE {date_filter}
    AND keyword_norm IS NOT NULL
    AND is_cross_partition_date = false
  GROUP BY 1, 2, 3, 4, 5, 6
),
ranked_7d AS (
  SELECT derived_genre, platform_group, keyword_norm,
    RANK() OVER (
      PARTITION BY derived_genre, platform_group
      ORDER BY COUNT(*) FILTER (WHERE session_action = 'enter') DESC
    ) AS rank_7d_ago
  FROM ott_search_curated.search_enriched
  WHERE dt BETWEEN DATE_ADD('day', -8, CURRENT_DATE)
                AND DATE_ADD('day', -2, CURRENT_DATE)
    AND keyword_norm IS NOT NULL
    AND is_cross_partition_date = false
  GROUP BY 1, 2, 3
)
SELECT
  t.*, 
  COALESCE(h.rank_7d_ago, 9999)               AS rank_7d_ago,
  COALESCE(h.rank_7d_ago, 9999) - t.rank_today AS rank_delta
FROM ranked_today t
LEFT JOIN ranked_7d h
  ON  t.derived_genre  = h.derived_genre
  AND t.platform_group = h.platform_group
  AND t.keyword_norm   = h.keyword_norm
WHERE t.rank_today <= 50
```

`COALESCE(rank_7d_ago, 9999)` — keywords with no 7-day history get rank 9999. `rank_delta = 9999 − rank_today` marks them as new entrants, making emerging trends visible.

---

## Validate the gold output

```bash
QID=$(aws athena start-query-execution \
  --query-string "
    SELECT derived_genre, platform_group,
           COUNT(*) AS keyword_slots,
           SUM(search_count) AS total_searches,
           ROUND(AVG(abandonment_rate), 4) AS avg_abandonment_rate,
           COUNT(CASE WHEN rank_delta > 0 THEN 1 END) AS rising_keywords
    FROM ott_search_gold.keyword_trends
    GROUP BY derived_genre, platform_group
    ORDER BY total_searches DESC
    LIMIT 20" \
  --work-group ott-analytics-${CDK_ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query QueryExecutionId --output text)

until [ "$(aws athena get-query-execution \
  --query-execution-id $QID \
  --query 'QueryExecution.Status.State' --output text)" != "RUNNING" ]; do
  sleep 5
done

aws athena get-query-results --query-execution-id $QID --output table
```

**Verified top rows (2026-05-08):**

| derived_genre | platform_group | keyword_slots | total_searches | avg_abandonment |
|---|---|---|---|---|
| UNKNOWN | SmartTV | ~102 | ~27,700 | **~0.310** |
| UNKNOWN | Android | ~101 | ~20,900 | **~0.138** |
| UNKNOWN | iOS | ~101 | ~15,400 | ~0.090 |
| UNKNOWN | OTTBox | ~100 | ~12,500 | ~0.085 |
| THE_THAO | Android | ~100 | ~4,200 | ~0.046 |
| THE_THAO | OTTBox | ~150 | ~979 | **~0.080** |

Key findings:
- **UNKNOWN × SmartTV: 30.95% abandonment** — highest by volume × abandonment impact. Nearly 1-in-3 SmartTV free-text searches produce no useful result.
- **UNKNOWN × Android: 13.83%** — second-highest.
- **THE_THAO × OTTBox: ~8.0%** — sports on set-top boxes; higher than sports on Android (~4.6%).
- The failure is **classifier coverage**, not a device-specific content gap.

```bash
# Total gold row count
QID=$(aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM ott_search_gold.keyword_trends" \
  --work-group ott-analytics-${CDK_ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query QueryExecutionId --output text)

until [ "$(aws athena get-query-execution \
  --query-execution-id $QID \
  --query 'QueryExecution.Status.State' --output text)" != "RUNNING" ]; do
  sleep 5
done

aws athena get-query-results \
  --query-execution-id $QID \
  --query "ResultSet.Rows[1].Data[0].VarCharValue" \
  --output text
```

**Verified: 4,761 rows**

---

## Screenshot guidance

**Screenshot 1 — Step Functions execution graph (all green)**
Navigate to: **Step Functions Console → State machines → `ott-daily-pipeline-dev` → Executions → [your execution] → Graph view**.
All state nodes must be green. Hover over `StartETLJob` to show its duration.
Save as `workshop/static/images/4.5-sfn-graph.png`.

**Screenshot 2 — Step Functions execution timeline**
On the same execution, click **Events** tab or **Timeline view**.
Capture the total duration (~610 seconds) with each state's start/end time visible.
Save as `workshop/static/images/4.5-sfn-timeline.png`.

**Screenshot 3 — Athena gold validation query results**
Run the validation query above in Athena Console (workgroup `ott-analytics-dev`).
Capture the result table with UNKNOWN × SmartTV showing avg_abandonment_rate ≈ 0.310.
Save as `workshop/static/images/4.5-gold-validation.png`.
