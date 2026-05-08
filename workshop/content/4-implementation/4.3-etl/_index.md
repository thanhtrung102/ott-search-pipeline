---
title: "4.3 ETL Layer (Iteration 2)"
weight: 43
pre: "<b>4.3 </b>"
---

## Context

The Glue ETL job transforms the raw 12-column schema into an 18-column curated schema. The three most significant transformations are:

1. **Datetime corruption recovery** — three distinct corruption types were found in the actual data
2. **Repeat-search detection** — requires a session-window LAG, which cannot be done in streaming
3. **User privacy protection** — SHA-256 hashing of `user_id` before any data reaches the curated layer

---

## Datetime corruption (§6.7)

Three corruption types were found in the June 2022 dataset, each requiring different handling:

### Type 1 — Arabic-Indic numerals

A device locale issue. The digits `٠١٢٣٤٥٦٧٨٩` represent 0–9.

```
Input:  "٢٠٢٢-٠٦-٠١ ١٣:٥٧:٤٧.٦٤٧"
Output: "2022-06-01 13:57:47.647"
```

Fix: `str.translate()` with a character mapping table for the 10 Arabic-Indic digit characters.

### Type 2 — Buddhist Era year

A device set to Thai locale uses the Buddhist Era calendar (BE = CE + 543). Year 2022 CE = 2565 BE.

```
Input:  "2565-06-01 00:03:06.660"
Output: "2022-06-01 00:03:06.660"
```

Fix: if the string starts with `"2565"`, replace with `"2022"`. Only years 2565 (= 2022 CE) appear in this dataset.

### Type 3 — Year 0004 (corrupt)

Cause unknown — possibly a firmware bug in a specific OTTBox model. Year 0004 cannot be mapped to any valid date. These rows are dropped.

```
Input:  "0004-06-01 02:54:04.793"
Output: (row dropped — year < 2015 filter in Step 3)
```

### PySpark UDF implementation

```python
_AR = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

_DT_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?'
)

@udf(StringType())
def clean_datetime(dt_str: str) -> str | None:
    if dt_str is None:
        return None
    s = dt_str.strip().translate(_AR)   # Type 1: Arabic numerals
    if s.startswith("2565"):            # Type 2: Buddhist Era year
        s = "2022" + s[4:]
    m = _DT_RE.match(s)
    if not m:
        return None
    date_p, hh, mm, ss, frac = m.groups()
    ms = (frac or "000" + "000")[:3]
    return f"{date_p} {hh.zfill(2)}:{mm}:{ss}.{ms}"
```

Step 3 then drops any row where `year(event_ts) < 2015` — this catches Type 3 and any other future corrupt years.

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

## Upload Glue script and classifier

The Glue job reads its script and the `genre_classifier` module from S3. Upload them before running the crawler:

```bash
BUCKET="ott-search-${CDK_ACCOUNT}-dev"

# Upload PySpark script
aws s3 cp glue/search_enrichment_job.py \
  "s3://${BUCKET}/glue-scripts/search_enrichment_job.py"

# Package and upload genre_classifier as a zip (Glue extra-py-files)
cd genre_classifier && zip -r ../genre_classifier.zip . && cd ..
aws s3 cp genre_classifier.zip \
  "s3://${BUCKET}/glue-scripts/genre_classifier.zip"
```

---

## Run the crawler

```bash
# Start the crawler
aws glue start-crawler --name raw-events-crawler-dev

# Poll until READY (usually ~90 seconds)
until [ "$(aws glue get-crawler \
  --name raw-events-crawler-dev \
  --query 'Crawler.State' --output text)" = "READY" ]; do
  echo "Waiting for crawler..."
  sleep 15
done
echo "Crawler finished"

# Verify partition count
aws glue get-partitions \
  --database-name ott_search_raw \
  --table-name events \
  --query 'length(Partitions)' \
  --output text
# Expected: 14+ partitions (one per dt=2022-06-* folder)
```

---

## Run the ETL job

```bash
BUCKET="ott-search-${CDK_ACCOUNT}-dev"

aws glue start-job-run \
  --job-name search-enrichment-job-dev \
  --arguments '{
    "--PUSH_DOWN_PREDICATE": "dt >= '\''2022-06-01'\''",
    "--S3_BUCKET": "'"${BUCKET}"'",
    "--LLM_ENABLED": "false"
  }'
```

Monitor job status:
```bash
# Get the most recent job run
RUN_ID=$(aws glue get-job-runs \
  --job-name search-enrichment-job-dev \
  --query 'JobRuns[0].Id' --output text)

# Poll until SUCCEEDED or FAILED
until [ "$(aws glue get-job-run \
  --job-name search-enrichment-job-dev \
  --run-id ${RUN_ID} \
  --query 'JobRun.JobRunState' --output text)" != "RUNNING" ]; do
  echo "ETL running..."
  sleep 30
done

aws glue get-job-run \
  --job-name search-enrichment-job-dev \
  --run-id ${RUN_ID} \
  --query '{State:JobRun.JobRunState,Duration:JobRun.ExecutionTime,DPU:JobRun.MaxCapacity}' \
  --output json
```

Expected:
```json
{
  "State": "SUCCEEDED",
  "Duration": 260,
  "DPU": 10.0
}
```

---

## Screenshot 1 — Glue Data Catalog table schema

Navigate to: **AWS Glue Console → Data Catalog → Databases → ott_search_raw → Tables → events**

Click the table name to open the schema panel. Verify:
- Columns list shows all 10 data columns (`eventid`, `datetime`, `user_id`, `keyword`, `category`, `proxy_isp`, `platform`, `networktype`, `action`, `userplansmap`)
- Partition keys panel shows `dt`, `hour`, `derived_genre`, `platform_group`

{{% notice tip %}}
**📸 Screenshot 1:** Navigate to **AWS Glue Console → Data Catalog → Databases → ott_search_raw → Tables → events**. Click the table name. Capture the **Schema** tab showing all 10 data column names and the **Partition keys** section showing `dt`, `hour`, `derived_genre`, `platform_group`. Save as `workshop/static/images/4.3-glue-schema.png`.
{{% /notice %}}

---

## Screenshot 2 — ETL job run details

Navigate to: **AWS Glue Console → ETL Jobs → search-enrichment-job-dev → Run history tab**

Click the most recent run. Record:
- **State:** SUCCEEDED
- **Execution time:** (seconds)
- **DPU capacity:** 10.0

{{% notice tip %}}
**📸 Screenshot 2:** Capture the run detail page. **State: Succeeded**, execution time in seconds, and DPU-hours must all be visible. Calculate cost: `DPU-hours × $0.44/DPU-hour`. Save as `workshop/static/images/4.3-glue-run.png`.
{{% /notice %}}

---

## Screenshot 3 — Validation gate query in Athena

Navigate to: **Athena Console → Query editor** — select workgroup `ott-analytics-dev`

Run:
```sql
SELECT
  derived_genre,
  COUNT(*)                                           AS event_count,
  ROUND(AVG(CAST(is_search_abandoned AS INT)), 4)    AS abandonment_rate,
  COUNT(DISTINCT isp_segment)                        AS distinct_isps,
  COUNT(DISTINCT user_id_hashed)                     AS unique_users
FROM ott_search_curated.search_enriched
WHERE dt BETWEEN '2022-06-01' AND '2022-06-14'
  AND is_cross_partition_date = false
GROUP BY derived_genre
ORDER BY event_count DESC
```

{{% notice tip %}}
**📸 Screenshot 3:** Capture the full Athena result table — all 8 `derived_genre` rows must be visible with non-null `abandonment_rate` and `distinct_isps` values. This is the proof that the ETL enrichment columns are correctly populated. Save as `workshop/static/images/4.3-curated-validation.png`.
{{% /notice %}}

Expected shape of results (your exact numbers may differ slightly due to Kinesis throttling during replay):

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

{{% notice note %}}
UNKNOWN is the dominant category because keyword-to-genre classification uses a rule-based lookup; free-text searches that match no pattern fall into UNKNOWN. Non-zero abandonment rates across all genres reflect the `session_action` field: `enter` events without a subsequent interaction are marked `is_search_abandoned = true`. THE_THAO shows mid-range abandonment — the sports spike anomaly (see Section 4.4) is a volume anomaly, not an abandonment anomaly.
{{% /notice %}}

---

## Explanation

**Why does the ETL job run at G.1X with 10 DPUs?** The job processes 1.3 M records with a session-window LAG (Step 17 — repeat-search detection). The window requires Spark to shuffle data by `search_session_id`. 10 DPUs (G.1X: 4 vCPUs, 16 GB each) provides enough executor headroom to complete the shuffle in ~260 seconds without spilling to disk. The CDK stack sets `MaxCapacity=10` in `GlueJobProps`; you can reduce this to 2 for cost savings at the expense of ~3× longer run times.

**Why not use Glue DynamicFrame?** The raw Parquet files embed `derived_genre` and `platform_group` as both data columns (inside the file) and as directory partition names. `DynamicFrame` attempts type inference on all columns including duplicates, causing `COLUMN_ALREADY_EXISTS` errors. The PySpark `DataFrameReader` with an explicit schema and `basePath` avoids this — it reads only the 10 explicitly named data columns from each file, then adds the 4 partition columns from the directory structure without overlap.
