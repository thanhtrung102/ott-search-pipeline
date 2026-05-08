---
title: "4.2 Ingest Layer (Iteration 1)"
weight: 42
pre: "<b>4.2 </b>"
---

## Context

The raw `log_search` dataset is a static batch of Parquet files. To demonstrate a real-time pipeline, a replay-producer Lambda reads these files and publishes events to Kinesis Data Streams at a compressed timescale — 14 days of events replayed as fast as Kinesis will accept them.

Before publishing, each event is enriched with two fields:

- **`derived_genre`** — keyword → content genre classification using a rule-based lookup table + regex patterns covering 8 genres. Example: `"bolero"` → `NHAC`, `"bóng đá"` → `THE_THAO`, `"Sword Art Online"` → `ANIME`.
- **`platform_group`** — 35 distinct device model strings bucketed to 5 groups: `OTTBox`, `SmartTV`, `Android`, `iOS`, `Web` (+ `Other`).

These fields travel with the event through the entire pipeline — Firehose uses them as S3 partition keys, the anomaly detector groups by `derived_genre`, and the gold CTAS aggregates by both.

---

## Deploy

```bash
cdk deploy OttIngestion-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --require-approval never
```

**Deploy time:** approximately 4 minutes.

---

## What was deployed

| Resource | Name | Configuration |
|---|---|---|
| Kinesis Data Stream | `ott-search-stream-dev` | 2 shards, 24h retention, KMS encrypted |
| Kinesis Firehose | `ott-search-firehose-dev` | Source: KDS; Dest: S3; JSON→Parquet, SNAPPY; 64MB/60s buffer |
| Glue Database | `ott_search_raw` | — |
| Glue Table | `ott_search_raw.events` | 10 data columns; partition keys: `dt`, `hour`, `derived_genre`, `platform_group` |
| Lambda | `ott-replay-producer-dev` | Python 3.11, 3008 MB, 15 min timeout |

### Firehose dynamic partitioning

Firehose extracts partition values from each JSON record using a JQ expression:

```
{
  dt:.datetime[:10],
  hour:.datetime[11:13],
  derived_genre:.derived_genre,
  platform_group:.platform_group
}
```

Records land at:
```
s3://ott-search-{account}-dev/raw/events/
  dt=2022-06-01/
    hour=18/
      derived_genre=NHAC/
        platform_group=SmartTV/
          ott-search-firehose-dev-1-2022-06-01-11-23-45-xxxxxxxx.parquet
```

{{% notice warning %}}
The JQ expression reads the raw `datetime` field, **not** the `datetime_clean` field added by the replay-producer. This means Firehose partitions on the raw datetime, which includes corrupted values (`2565-*`, `0004-*`). The Glue ETL handles these via `is_cross_partition_date` flag. All valid June 2022 events land in the correct `dt=2022-06-*` partitions.
{{% /notice %}}

---

## Upload source data

If you have not uploaded the source Parquet files yet, do so now (see Section 2, item 8):

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

for dir in /path/to/log_search/2022*/; do
  raw_date=$(basename "$dir")
  dt="dt=${raw_date:0:4}-${raw_date:4:2}-${raw_date:6:2}"
  aws s3 cp "$dir" "s3://${BUCKET}/raw-source/log_search/${dt}/" \
    --recursive --exclude ".*" --exclude "_SUCCESS"
done

# Verify 14 date folders uploaded
aws s3 ls "s3://${BUCKET}/raw-source/log_search/" | grep PRE | wc -l
# Expected: 14
```

---

## Run the replay producer

The replay producer is invoked once per date folder. Invoke all 14 in parallel (one Lambda per day):

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"
FN="ott-replay-producer-dev"

for day in 01 02 03 04 05 06 07 08 09 10 11 12 13 14; do
  DATE="2022-06-${day}"
  PAYLOAD="{\"s3_prefix\":\"s3://${BUCKET}/raw-source/log_search/dt=${DATE}\"}"
  
  # Write payload to temp file (avoids base64 encoding issues)
  echo "$PAYLOAD" > /tmp/payload_${day}.json
  
  # Invoke asynchronously (Event type — fire and forget)
  aws lambda invoke \
    --function-name ${FN} \
    --invocation-type Event \
    --payload fileb:///tmp/payload_${day}.json \
    /tmp/response_${day}.json &
done
wait
echo "All 14 invocations dispatched"
```

{{% notice info %}}
**Why 14 concurrent invocations?** A single Lambda invocation loading all 14 days (1,146,996 rows) would time out at 15 minutes. One invocation per day folder keeps each Lambda under 5 minutes and fits within the 2-shard Kinesis capacity (2,000 records/sec shared across 14 Lambdas).
{{% /notice %}}

Monitor Lambda execution in CloudWatch:
```bash
# Check for errors in the last 30 minutes
aws logs filter-log-events \
  --log-group-name "/aws/lambda/ott-replay-producer-dev" \
  --start-time $(($(date +%s) - 1800))000 \
  --filter-pattern "Replay complete" \
  --query "events[*].message" \
  --output text
```

---

## Screenshot 1 — Kinesis IncomingRecords metric

Navigate to: **CloudWatch → Metrics → Kinesis → Stream Metrics → `ott-search-stream-dev` → IncomingRecords**

Set time range to last 30 minutes. You should see a burst curve as the 14 concurrent Lambda invocations push records.

{{% notice tip %}}
**📸 Screenshot 1:** Navigate to **CloudWatch → Metrics → Kinesis → Stream Metrics → `ott-search-stream-dev`**. Add `IncomingRecords` (Sum) and `IncomingBytes` (Sum). Set time range to **Last 30 minutes**, period **1 minute**. Capture the burst curve. Save as `workshop/static/images/4.2-kinesis-incoming.png`.
{{% /notice %}}

---

## Screenshot 2 — S3 partitions after Firehose delivery

Navigate to: **S3 Console → `ott-search-{account}-dev` → raw/events/**

Expand the prefix tree to show the partition structure:

```
raw/events/
  dt=2022-06-01/
    hour=18/
      derived_genre=NHAC/
        platform_group=SmartTV/
  dt=2022-06-02/
  ...
  dt=2022-06-14/
```

{{% notice tip %}}
**📸 Screenshot 2:** Navigate to **S3 Console → `ott-search-{account}-dev` → raw/events/**. Expand one path to show the full partition depth: `dt=2022-06-01/hour=18/derived_genre=NHAC/platform_group=SmartTV/`. All 14 `dt=2022-06-*` folders must be visible. Save as `workshop/static/images/4.2-s3-partitions.png`.
{{% /notice %}}

Verify all 14 date partitions landed:

```bash
aws s3 ls "s3://${BUCKET}/raw/events/" --recursive \
  | grep "dt=2022-06" \
  | awk -F'dt=' '{print $2}' | cut -d'/' -f1 \
  | sort -u
```

Expected output (14 lines):
```
2022-06-01
2022-06-02
2022-06-03
2022-06-04
2022-06-05
2022-06-06
2022-06-07
2022-06-08
2022-06-09
2022-06-10
2022-06-11
2022-06-12
2022-06-13
2022-06-14
```

---

## Screenshot 3 — Firehose delivery metrics

Navigate to: **Kinesis Data Firehose Console → `ott-search-firehose-dev` → Monitoring tab**

Check these metrics over the replay window:
- `DeliveryToS3.Success` — count of successful S3 deliveries
- `DeliveryToS3.DataFreshness` — lag between Kinesis record creation and S3 landing (should peak ≤ 120 seconds at buffer flush)

{{% notice tip %}}
**📸 Screenshot 3:** Navigate to **Kinesis Firehose Console → `ott-search-firehose-dev` → Monitoring tab**. Set time range to cover the replay window. Capture both `DeliveryToS3.Success` and `DeliveryToS3.DataFreshness` graphs in the same frame. `DataFreshness` should peak at ≤ 120 seconds. Save as `workshop/static/images/4.2-firehose-metrics.png`.
{{% /notice %}}

---

## Explanation

Dynamic partitioning is the mechanism that makes downstream Athena queries efficient. When a Marketing analyst queries "top keywords in ANIME searches on June 8", Athena reads only the `dt=2022-06-08/derived_genre=ANIME/` partition — skipping 91% of the data (13 of 14 date partitions, and 7 of 8 genre partitions).

Without dynamic partitioning, every query would scan the full 1.3 M-row dataset regardless of filters, incurring unnecessary cost and latency.

The `platform_group` partition adds a further 6× reduction when filtering by device type, which is the common case for the Marketing dashboard (e.g., "SmartTV only").
