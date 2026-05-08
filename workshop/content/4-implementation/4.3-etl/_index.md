---
title: "4.3 ETL Layer (Iteration 2)"
weight: 43
pre: "<b>4.3 </b>"
---

## What this deploys

- **Glue Crawler** `raw-events-crawler-dev` — scans `s3://{bucket}/raw/events/`, registers partition schema into `ott_search_raw.events`
- **Glue ETL job** `search-enrichment-job-dev` — PySpark Glue 4.0, G.1X, 10 DPU; transforms raw 12-column schema → curated 18-column schema
- **Glue Database** `ott_search_curated` — catalog for enriched output

---

## Deploy

```bash
cdk deploy OttETL-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --require-approval never
```

**Deploy time:** approximately 3 minutes.

---

## Upload Glue assets to S3

The Glue job reads its PySpark script and genre classifier from S3. Upload them before running:

```bash
# PySpark script
aws s3 cp glue/search_enrichment_job.py \
  "s3://${BUCKET}/glue-scripts/search_enrichment_job.py"

# Genre classifier module (packed as zip for Glue extra-py-files)
cd genre_classifier && zip -r ../genre_classifier.zip . && cd ..
aws s3 cp genre_classifier.zip \
  "s3://${BUCKET}/glue-scripts/genre_classifier.zip"

echo "Assets uploaded"
```

---

## Run the crawler

```bash
aws glue start-crawler --name raw-events-crawler-${CDK_ENV}

# Poll until READY (~90 seconds)
until [ "$(aws glue get-crawler \
  --name raw-events-crawler-${CDK_ENV} \
  --query 'Crawler.State' --output text)" = "READY" ]; do
  echo "Crawler running..."
  sleep 15
done
echo "Crawler finished"
```

Verify partition count:
```bash
aws glue get-partitions \
  --database-name ott_search_raw \
  --table-name events \
  --query 'length(Partitions)' \
  --output text
# Expected: 14 (one per dt=2022-06-NN folder)
```

---

## Run the ETL job

```bash
aws glue start-job-run \
  --job-name search-enrichment-job-${CDK_ENV} \
  --arguments '{
    "--PUSH_DOWN_PREDICATE": "dt >= '\''2022-06-01'\''",
    "--S3_BUCKET": "'"${BUCKET}"'",
    "--LLM_ENABLED": "false"
  }'
```

Monitor until complete:
```bash
RUN_ID=$(aws glue get-job-runs \
  --job-name search-enrichment-job-${CDK_ENV} \
  --query 'JobRuns[0].Id' --output text)

until [ "$(aws glue get-job-run \
  --job-name search-enrichment-job-${CDK_ENV} \
  --run-id ${RUN_ID} \
  --query 'JobRun.JobRunState' --output text)" != "RUNNING" ]; do
  echo "ETL running..."
  sleep 30
done

aws glue get-job-run \
  --job-name search-enrichment-job-${CDK_ENV} \
  --run-id ${RUN_ID} \
  --query '{State:JobRun.JobRunState,Duration:JobRun.ExecutionTime,DPU:JobRun.MaxCapacity}' \
  --output json
```

**Verified output (2026-05-08):**
```json
{
  "State": "SUCCEEDED",
  "Duration": 260,
  "DPU": 10.0
}
```

---

## 18-step enrichment pipeline

The job transforms the 12-column raw schema into 18 curated columns:

| Steps | What happens |
|---|---|
| 1 | Rename `eventid` → `event_id` |
| 2–3 | `clean_datetime()` UDF — repair Arabic-Indic numerals, Buddhist Era year, drop year < 2015 |
| 4 | Derive `hour_of_day_vn` (UTC + 7) |
| 5 | Pass through `session_action` from `category` |
| 6 | Derive `is_search_abandoned` (category == 'quit') |
| 7 | Derive `user_is_authenticated` (user_id IS NOT NULL) |
| 8 | SHA-256 hash `user_id` → `user_id_hashed` (null-safe) |
| 9 | Normalize keyword: `lower(trim(keyword))` → `keyword_norm` |
| 10 | Genre classification: LUT + regex → `derived_genre` (UNKNOWN if no match) |
| 11 | Platform normalization: 35 strings → 6 buckets → `platform_group` |
| 12 | Network type normalization → `network_type_norm` |
| 13 | ISP normalization: `upper(proxy_isp)` → `isp_segment` |
| 14 | Premium flag: any of VIP/HBO GO+/K+/MAX in `userplansmap` → `has_premium` |
| 15 | Subscription count: `len(userplansmap)` → `subscription_count` |
| 16 | Session ID: `SHA-256(user_id\|30min_bucket)` → `search_session_id` |
| 17 | Repeat search detection: LAG window over session → `is_repeat_search` |
| 18 | Cross-partition date flag: event year ≠ `dt` partition year → `is_cross_partition_date` |

### Datetime corruption types

Three types were found in the June 2022 dataset, each handled differently:

**Type 1 — Arabic-Indic numerals** (device locale bug):
```
Input:  "٢٠٢٢-٠٦-٠١ ١٣:٥٧:٤٧.٦٤٧"
Output: "2022-06-01 13:57:47.647"
Fix:    str.translate() with 10-char mapping table
```

**Type 2 — Buddhist Era year** (Thai locale, BE = CE + 543):
```
Input:  "2565-06-01 00:03:06.660"
Output: "2022-06-01 00:03:06.660"
Fix:    Replace prefix "2565" with "2022"
```

**Type 3 — Year 0004** (OTTBox firmware bug, cause unknown):
```
Input:  "0004-06-01 02:54:04.793"
Output: (row dropped — year < 2015 filter)
```

PySpark UDF:
```python
_AR = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

@udf(StringType())
def clean_datetime(dt_str: str) -> str | None:
    if dt_str is None:
        return None
    s = dt_str.strip().translate(_AR)   # Type 1: Arabic-Indic
    if s.startswith("2565"):            # Type 2: Buddhist Era
        s = "2022" + s[4:]
    m = _DT_RE.match(s)
    if not m:
        return None
    date_p, hh, mm, ss, frac = m.groups()
    ms = (frac or "000" + "000")[:3]
    return f"{date_p} {hh.zfill(2)}:{mm}:{ss}.{ms}"
# Step 3: filter year(event_ts) < 2015 drops Type 3
```

---

## Validate the curated output

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

QID=$(aws athena start-query-execution \
  --query-string "
    SELECT
      derived_genre,
      COUNT(*)                                          AS event_count,
      ROUND(AVG(CAST(is_search_abandoned AS INT)), 4)   AS abandonment_rate,
      COUNT(DISTINCT isp_segment)                       AS distinct_isps,
      COUNT(DISTINCT user_id_hashed)                    AS unique_users
    FROM ott_search_curated.search_enriched
    WHERE dt BETWEEN '2022-06-01' AND '2022-06-14'
      AND is_cross_partition_date = false
    GROUP BY derived_genre
    ORDER BY event_count DESC" \
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
  --output table
```

**Verified output shape (2026-05-08):**

| derived_genre | event_count | abandonment_rate |
|---|---|---|
| UNKNOWN | ~1,235,000 | ~0.145 |
| ANIME | ~34,600 | ~0.091 |
| NHAC | ~21,300 | ~0.025 |
| THE_THAO | ~14,600 | ~0.072 |
| TRUYEN_HINH | ~11,300 | ~0.125 |
| PHIM_VIET | ~6,000 | ~0.090 |
| PHIM_AU_MY | ~3,100 | ~0.066 |
| PHIM_TRUNG | ~2,300 | ~0.018 |

Total valid curated records (verified): **1,333,242** (1,334,620 total minus cross-partition date rows).

{{% notice note %}}
UNKNOWN dominates because the rule-based LUT + regex covers ~44.5% of keyword volume. Free-text searches with no genre match (the long tail) fall to UNKNOWN. This is expected behavior, not a bug — it is an explicit acknowledgment of classifier uncertainty. The LLM fallback (`LLM_ENABLED=true`) handles the long tail in production but requires a Secrets Manager API key and is disabled in this demo.
{{% /notice %}}

---

## Why PySpark native writer instead of Glue DynamicFrame

The raw Parquet files embed `derived_genre` and `platform_group` as both data columns (inside the file) and as directory partition path names. `DynamicFrame` attempts type inference on all columns including the directory-derived duplicates, raising `COLUMN_ALREADY_EXISTS`. The PySpark `DataFrameReader` with an explicit schema and `basePath` reads only the 10 named data columns, then adds the 4 partition columns from the directory structure — no overlap.

---

## Screenshot guidance

**Screenshot 1 — Glue Data Catalog schema**
Navigate to: **Glue Console → Data Catalog → Databases → ott_search_raw → Tables → events**.
Capture the Schema tab showing all 10 data columns and the Partition keys section showing `dt`, `hour`, `derived_genre`, `platform_group`.
Save as `workshop/static/images/4.3-glue-schema.png`.

**Screenshot 2 — ETL job run SUCCEEDED**
Navigate to: **Glue Console → ETL Jobs → `search-enrichment-job-dev` → Run history tab**.
Capture the run detail showing **State: Succeeded**, execution time ≈260 seconds, DPU=10.
Save as `workshop/static/images/4.3-glue-run.png`.

**Screenshot 3 — Athena curated validation query results**
Navigate to: **Athena Console → Query editor** (workgroup `ott-analytics-dev`).
Run the validation query above. Capture all 8 genre rows with non-null abandonment_rate.
Save as `workshop/static/images/4.3-curated-validation.png`.
