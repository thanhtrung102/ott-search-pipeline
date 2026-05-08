---
title: "4.5 Orchestration + Gold Layer (Iteration 4)"
weight: 45
pre: "<b>4.5 </b>"
---

## Context

Step Functions orchestrates the nightly batch pipeline as a state machine. Each state uses AWS SDK direct integration (`.sync` or `aws-sdk`) — Step Functions polls the downstream service itself rather than requiring a polling Lambda. This eliminates a class of infrastructure components and lets the state machine accurately report durations.

The pipeline runs at **01:30 UTC+7 (18:30 UTC)** daily. It completes by 02:00, so the gold layer is ready when analysts open QuickSight in the morning.

---

## State machine

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

Any state except `StartCrawler` routes to `PipelineFailure` on error, which publishes an SNS notification to the ops topic.

### Why `DropGoldTable` before the CTAS?

Athena CTAS fails if the target table already exists. `DROP TABLE IF EXISTS` makes the pipeline idempotent — it can be re-run on the same day without manual cleanup. This is also why the `DropGoldTable` step is separate from `RunAthenaGoldCTAS`: if the drop fails (e.g., permission error), the pipeline fails cleanly rather than leaving a half-written table.

### Why `MSCK REPAIR TABLE` after ETL?

The Glue ETL job writes partitions to S3 directly via PySpark's native writer (`df.write.partitionBy(...).parquet(...)`). This bypasses the Glue catalog — the new partitions exist on S3 but are not registered in the Glue metastore. `MSCK REPAIR TABLE` scans the S3 prefix and registers all partitions, making them visible to Athena. This step must run before the Gold CTAS can read from `ott_search_curated.search_enriched`.

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
The `gold_date_filter` context variable is baked into the Step Functions ASL at CDK synth time. For the demo dataset (June 2022 historical data), pass `--context gold_date_filter="dt >= '2022-06-01'"`. The production default is `dt >= date_add('day', -1, current_date)` — do not omit this override when using historical data or the CTAS will produce an empty table.
{{% /notice %}}

**Deploy time:** approximately 3 minutes.

---

## What was deployed

| Resource | Name | Configuration |
|---|---|---|
| Athena Workgroup | `ott-analytics-dev` | 10 GB scan cap, SSE-S3 results |
| Glue Database | `ott_search_gold` | — |
| Step Functions | `ott-daily-pipeline-dev` | X-Ray enabled, 3-hour timeout |
| EventBridge Rule | daily 18:30 UTC | Triggers state machine |

---

## Run the pipeline manually

Do not wait for the scheduled trigger. Invoke the state machine now:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
SM_ARN="arn:aws:states:ap-southeast-1:${ACCOUNT}:stateMachine:ott-daily-pipeline-dev"

EXEC_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn ${SM_ARN} \
  --name "manual-$(date +%Y%m%dT%H%M%S)" \
  --query executionArn \
  --output text)

echo "Execution ARN: ${EXEC_ARN}"
```

Monitor the execution:
```bash
# Poll until complete
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

Expected:
```json
{
  "Status": "SUCCEEDED",
  "Start": "2022-...",
  "Stop": "2022-..."
}
```

**Total pipeline duration:** approximately 10 minutes for the June 2022 dataset (Glue ETL takes ~7 minutes, CTAS ~1 minute).

---

## Gold CTAS SQL

The full `CREATE TABLE ... AS SELECT` query that builds `ott_search_gold.keyword_trends`. It runs as the `RunAthenaGoldCTAS` state in Step Functions.

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
    derived_genre,
    platform_group,
    network_type_norm,
    isp_segment,
    keyword_norm,
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
    )                                                          AS rank_today
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
  t.trend_date, t.derived_genre, t.platform_group, t.network_type_norm,
  t.isp_segment, t.keyword_norm, t.search_count, t.enter_count,
  t.abandonment_rate, t.unique_users, t.authenticated_rate,
  t.repeat_search_rate, t.premium_search_rate,
  t.rank_today,
  COALESCE(h.rank_7d_ago, 9999)               AS rank_7d_ago,
  COALESCE(h.rank_7d_ago, 9999) - t.rank_today AS rank_delta
FROM ranked_today t
LEFT JOIN ranked_7d h
  ON  t.derived_genre  = h.derived_genre
  AND t.platform_group = h.platform_group
  AND t.keyword_norm   = h.keyword_norm
WHERE t.rank_today <= 50
```

Key design decisions:
- **`rank_today <= 50`** — top-50 per (genre, platform_group) pair. With 8 genres × 6 platform groups × 50 keywords = 2,400 max rows per trend_date. The actual count is lower because not all genre × platform combinations have 50 distinct keywords.
- **`COALESCE(rank_7d_ago, 9999)`** — keywords with no history 7 days ago (new keywords) get rank 9999. A `rank_delta` of `9999 − rank_today` marks them as "new entrants."
- **`is_cross_partition_date = false`** — excludes rows where the event timestamp year doesn't match the `dt` partition year (corruption artifacts). Without this filter, events with Buddhist-era dates would appear in both the correct partition and the corrupt partition.

---

## Screenshot 1 — Step Functions execution graph

Navigate to: **Step Functions Console → State machines → `ott-daily-pipeline-dev` → Executions → [your execution]**

Click **Graph view** (not Definition). All state nodes should be green (SUCCEEDED). Hover over the `StartETLJob` node to see its duration annotation — this is the longest state.

{{% notice tip %}}
**📸 Screenshot 1:** Capture the Graph view with all states green. Make sure the `StartETLJob` tooltip showing elapsed time is visible. Save as `workshop/static/images/4.5-sfn-graph.png`.
{{% /notice %}}

---

## Screenshot 2 — Gold layer Athena validation

Navigate to: **Athena Console → Query editor** — workgroup `ott-analytics-dev`

Run the gold layer validation query:
```sql
SELECT
  derived_genre,
  platform_group,
  COUNT(*) AS keyword_slots,
  SUM(search_count) AS total_searches,
  ROUND(AVG(abandonment_rate), 4) AS avg_abandonment_rate,
  COUNT(CASE WHEN rank_delta > 0 THEN 1 END) AS rising_keywords
FROM ott_search_gold.keyword_trends
GROUP BY derived_genre, platform_group
ORDER BY total_searches DESC
LIMIT 20
```

{{% notice tip %}}
**📸 Screenshot 2:** Capture the full Athena result table. The `avg_abandonment_rate` column must show a non-zero value for at least one row (UNKNOWN × Android should be ~0.198). The `rising_keywords` column proves `rank_delta` is populated. Save as `workshop/static/images/4.5-gold-validation.png`.
{{% /notice %}}

Expected shape from June 2022 run (top rows by total_searches):

| derived_genre | platform_group | keyword_slots | total_searches | avg_abandonment |
|---|---|---|---|---|
| UNKNOWN | SmartTV | ~102 | ~27,700 | ~0.017 |
| UNKNOWN | iOS | ~101 | ~20,900 | ~0.005 |
| UNKNOWN | OTTBox | ~100 | ~18,900 | ~0.000 |
| THE_THAO | Android | ~100 | ~4,200 | ~0.025 |
| THE_THAO | OTTBox | ~150 | ~979 | ~0.014 |

The `avg_abandonment_rate` for THE_THAO on OTTBox is ~1.4% — sports searches on set-top boxes see higher abandonment than OTT movies but lower than the UNKNOWN category. The UNKNOWN category on Android shows the highest abandonment (~19.8%), reflecting free-text searches that matched no known content.

---

## Verify via CLI

```bash
# Count gold rows
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

QID=$(aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM ott_search_gold.keyword_trends" \
  --work-group ott-analytics-dev \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query QueryExecutionId --output text)

# Wait for completion
until [ "$(aws athena get-query-execution \
  --query-execution-id $QID \
  --query 'QueryExecution.Status.State' --output text)" != "RUNNING" ]; do
  sleep 5
done

aws athena get-query-results \
  --query-execution-id $QID \
  --query "ResultSet.Rows[1].Data[0].VarCharValue" \
  --output text
# Expected: ~4761 (may vary based on distinct keyword-genre-platform combinations in your data)
```
