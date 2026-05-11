---
title: "4.2 Ingest Layer (Iteration 1)"
weight: 42
pre: "<b>4.2 </b>"
---

## What this deploys

- **Kinesis Data Streams** — 2 shards, KMS SSE (`ott-kinesis-key`), 24-hour retention
- **Kinesis Firehose** — JQ dynamic partitioning → `raw/events/`, JSON→Parquet SNAPPY, 64 MB/60 s buffer
- **replay-producer Lambda** — reads source Parquet from S3, enriches with `derived_genre` and `platform_group`, publishes to Kinesis

---

## Deploy

```bash
cdk deploy OttIngestion-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --require-approval never
```

**Deploy time:** approximately 6 minutes.

Expected terminal output:
```
✅  OttIngestion-demo

Outputs:
OttIngestion-demo.StreamName = ott-search-stream-demo
OttIngestion-demo.FirehoseName = ott-search-firehose-demo
OttIngestion-demo.ReplayFunctionName = ott-replay-producer-demo
```

---

## Proof of deployment

```bash
# Kinesis stream — 2 shards, ACTIVE
aws kinesis describe-stream-summary \
  --stream-name ott-search-stream-${CDK_ENV} \
  --query 'StreamDescriptionSummary.{Status:StreamStatus,Shards:OpenShardCount,Retention:RetentionPeriodHours}' \
  --output json
```

Expected:
```json
{"Status": "ACTIVE", "Shards": 2, "Retention": 24}
```

```bash
# Firehose delivery stream — ACTIVE
aws firehose describe-delivery-stream \
  --delivery-stream-name ott-search-firehose-${CDK_ENV} \
  --query 'DeliveryStreamDescription.DeliveryStreamStatus' \
  --output text
# Expected: ACTIVE
```

---

## Run the replay

Upload the June 2022 dataset to S3 first (see Prerequisites, Step 9), then trigger all 14 daily folders **sequentially** (one at a time) to avoid Kinesis throttling:

```bash
FN="ott-replay-producer-${CDK_ENV}"

for day in 01 02 03 04 05 06 07 08 09 10 11 12 13 14; do
  echo "Replaying 202206${day}..."
  aws lambda invoke \
    --function-name ${FN} \
    --invocation-type RequestResponse \
    --cli-binary-format raw-in-base64-out \
    --payload "{\"s3_prefix\": \"s3://${BUCKET}/raw-source/log_search/202206${day}\"}" \
    --cli-read-timeout 960 \
    /tmp/response_${day}.json
  cat /tmp/response_${day}.json
done
echo "All 14 days replayed"
```

Each day completes in ~1–2 minutes (SPEEDUP_FACTOR=100000, no rate pacing). Total ~18 minutes for 14 days.

{{% notice note %}}
The raw-source directories are named `20220601/` through `20220614/` (YYYYMMDD format, no `dt=` prefix). Ensure this matches your dataset upload (see Prerequisites, Step 9).
{{% /notice %}}

**Verified result (2026-05-09):** 1,146,996 Kinesis records published from 1,146,996 source rows, 0 failed (sequential run, no throttling). All records flow through Kinesis → Firehose → `raw/events/`. The Glue ETL in Section 4.3 reads from `raw/events/` (Firehose output), not directly from raw-source.

---

## Proof: records landing in S3

Wait ~90 seconds after replay starts (Firehose buffers 60 s / 64 MB):

```bash
# Count Parquet files in raw prefix
aws s3 ls "s3://${BUCKET}/raw/events/" --recursive | grep ".parquet" | wc -l
# Expected: 100+ files (grows during replay)

# Verify partition hierarchy
aws s3 ls "s3://${BUCKET}/raw/events/dt=2022-06-01/" | head -3
# Expected: PRE hour=00/  PRE hour=01/  ...
```

```bash
# Kinesis records published — CloudWatch metric (run after replay completes)
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --dimensions Name=StreamName,Value=ott-search-stream-${CDK_ENV} \
  --start-time $(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 7200 \
  --statistics Sum \
  --query 'Datapoints[0].Sum' \
  --output text
# Expected: ~1,146,996 (full 14-day replay within the last 2 hours)
```

---

## Anomaly injection (for Section 4.4 demo)

To demonstrate the real-time anomaly detector, replay June 2 with a synthetic 10× sports spike at 20:00:

```bash
aws lambda invoke \
  --function-name ${FN} \
  --invocation-type RequestResponse \
  --cli-binary-format raw-in-base64-out \
  --payload '{"s3_prefix": "s3://'"${BUCKET}"'/raw-source/log_search/20220602", "inject_anomaly": true, "anomaly_genre": "THE_THAO", "anomaly_hour": 20, "anomaly_multiplier": 10}' \
  --cli-read-timeout 960 \
  /tmp/response_inject.json
cat /tmp/response_inject.json
echo "Anomaly injection complete"
```

---

## Resources deployed

| Resource | Name | Configuration |
|---|---|---|
| Kinesis Data Stream | `ott-search-stream-demo` | 2 shards, 24h retention, KMS SSE (`ott-kinesis-key`) |
| Kinesis Firehose | `ott-search-firehose-demo` | JSON→Parquet SNAPPY, dynamic partitioning via JQ |
| Lambda | `ott-replay-producer-demo` | 15 min timeout, 3008 MB, `ott-lambda-sg` |
| Glue Database | `ott_search_raw` | Catalog for crawler output |

Firehose dynamic partition JQ expression:
```json
{
  "dt":             ".datetime[:10]",
  "hour":           ".datetime[11:13]",
  "derived_genre":  ".derived_genre",
  "platform_group": ".platform_group"
}
```

{{% notice note %}}
Firehose partitions on the raw `datetime` field (before datetime repair). Events with corrupted dates land in the correct S3 partition because the raw datetime string is structurally valid. The ETL step repairs the values and sets `is_cross_partition_date = true` for the ~0.1% of events with year-level corruption (year 0004, Buddhist Era 2565).
{{% /notice %}}

---

## Screenshot guidance

**Screenshot 1 — Kinesis stream IncomingRecords**
Navigate to: **Kinesis Console → Data Streams → `ott-search-stream-demo` → Monitoring tab**.
Set time range to the replay window. Capture the `IncomingRecords` metric showing the spike.
Save as `workshop/static/images/4.2-kinesis.png`.

**Screenshot 2 — S3 raw partition structure**
Navigate to: **S3 Console → `ott-search-{account}-demo` → raw/events/**.
Expand one date prefix to show `dt=.../hour=.../derived_genre=.../platform_group=.../` hierarchy.
Save as `workshop/static/images/4.2-s3-raw.png`.
