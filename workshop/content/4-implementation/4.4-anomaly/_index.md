---
title: "4.4 Anomaly Detection (Iteration 3)"
weight: 44
pre: "<b>4.4 </b>"
---

## Context

The anomaly-detector Lambda subscribes to the same Kinesis stream as Firehose. Both consumers receive every event independently — neither blocks the other.

The Lambda groups incoming `enter` events by `(derived_genre, hour_of_day_vn)` and computes a z-score against a 7-day rolling baseline stored in DynamoDB:

```
z = (observed_count − rolling_mean) / rolling_std
```

A z-score above **+3.0** (SPIKE) or below **−3.0** (DROP) indicates an event rate that occurs by chance less than 0.3% of the time under normal distribution — a signal worth alerting on.

When an anomaly is detected, the Lambda:
1. Writes an item to `ott-anomaly-events-dev` with `is_anomaly: true`, `z_score`, `anomaly_type`
2. Publishes an EventBridge event (`source: ott.anomaly-detector`, `detail-type: SearchAnomalyDetected`)
3. EventBridge routes the event to SNS → email

---

## Deploy

```bash
cdk deploy OttCompute-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --context alert_email=${ALERT_EMAIL} \
  --require-approval never
```

**Deploy time:** approximately 4 minutes.

---

## What was deployed

| Resource | Name | Configuration |
|---|---|---|
| DynamoDB | `ott-baseline-stats-dev` | PK: `genre_hour` (e.g. `THE_THAO#20`), on-demand, KMS |
| DynamoDB | `ott-anomaly-events-dev` | PK: `anomaly_id`, SK: `score_ts`, TTL: 30 days, GSI: genre+time |
| Lambda | `ott-anomaly-detector-dev` | Python 3.11, 256 MB, 3 min, Kinesis ESM batch=100 |
| Lambda | `ott-baseline-updater-dev` | Python 3.11, 256 MB, 5 min, EventBridge daily 01:00 UTC+7 |
| EventBridge Bus | `ott-search-events-dev` | Custom bus |
| SNS Topic | `ott-search-anomaly-alerts-dev` | KMS encrypted, email subscription |

---

## Confirm SNS email subscription

After deploying, check your inbox for the SNS subscription confirmation email. **Click "Confirm subscription"** — alerts will not be delivered until you confirm.

```bash
# Verify subscription status
aws sns list-subscriptions-by-topic \
  --topic-arn $(aws sns list-topics \
    --query "Topics[?contains(TopicArn,'anomaly-alerts-dev')].TopicArn" \
    --output text) \
  --query "Subscriptions[*].{Protocol:Protocol,Endpoint:Endpoint,Status:SubscriptionArn}" \
  --output table
```

Expected: your email address in the `Endpoint` column. The `Status` column shows the subscription ARN once confirmed, or `PendingConfirmation` if you have not yet clicked the confirmation link in your inbox. **Alerts will not be delivered until you confirm** — check spam if the email did not arrive.

---

## Seed the baseline

The anomaly detector needs a baseline in DynamoDB before it can score events. Seed it from the curated data produced in Iteration 2:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

aws lambda invoke \
  --function-name ott-baseline-updater-dev \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/baseline_response.json

cat /tmp/baseline_response.json
# Expected: {"slots_written": 192, "athena_rows": <N>}
```

{{% notice info %}}
The baseline updater uses the `BASELINE_DATE_FROM` environment variable if set. For the demo dataset, set `BASELINE_DATE_FROM=2022-06-01` on the Lambda so it queries the full June window instead of the default 7-day rolling window from today (which has no data).

```bash
aws lambda update-function-configuration \
  --function-name ott-baseline-updater-dev \
  --environment "Variables={
    BASELINE_TABLE=ott-baseline-stats-dev,
    ATHENA_WORKGROUP=ott-analytics-dev,
    ATHENA_OUTPUT=s3://${BUCKET}/athena-results/,
    BASELINE_DATE_FROM=2022-06-01
  }"
```
{{% /notice %}}

Verify 192 baseline items (8 genres × 24 hours):
```bash
aws dynamodb scan \
  --table-name ott-baseline-stats-dev \
  --select COUNT \
  --query Count \
  --output text
# Expected: 192
```

Sample a few items to confirm they have non-zero means:
```bash
# DynamoDB does not support begins_with in key conditions for scan — use filter expression
aws dynamodb scan \
  --table-name ott-baseline-stats-dev \
  --filter-expression "begins_with(genre_hour, :prefix)" \
  --expression-attribute-values '{":prefix":{"S":"THE_THAO"}}' \
  --query "Items[*].{key:genre_hour.S,mean:rolling_mean.N,std:rolling_std.N}" \
  --output table
```

---

## Anomaly injection demo

To demonstrate anomaly detection without waiting for a real anomaly in the 2022 dataset, the replay producer can inject a synthetic volume spike. The `inject_anomaly` event parameter duplicates all `THE_THAO` enter events at hour 20 (20:00 Vietnam time, prime time for live football) by a configurable multiplier.

This is realistic: a major Vietnam national team match generates search spikes of this magnitude.

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

# Re-run June 2 with 10× sports spike at 20:00
cat > /tmp/inject_payload.json << 'EOF'
{
  "s3_prefix": "s3://BUCKET/raw-source/log_search/dt=2022-06-02",
  "inject_anomaly": true,
  "anomaly_genre": "THE_THAO",
  "anomaly_hour": 20,
  "anomaly_multiplier": 10
}
EOF

# Substitute actual bucket name
sed -i "s/BUCKET/${BUCKET}/" /tmp/inject_payload.json

aws lambda invoke \
  --function-name ott-replay-producer-dev \
  --payload fileb:///tmp/inject_payload.json \
  /tmp/inject_response.json

cat /tmp/inject_response.json
# Expected: {"records_published": <N>, "records_failed": 0}
```

---

## Screenshot 1 — Anomaly alert email

After the injection replay completes (approximately 2–4 minutes), check your inbox for the SNS alert.

{{% notice tip %}}
**📸 Screenshot 1:** Open the SNS alert email. The subject should contain `SearchAnomalyDetected`. Capture the full email body — verify `derived_genre: THE_THAO`, `hour_of_day_vn: 20`, `z_score` > 3.0, `anomaly_type: SPIKE`. Save as `workshop/static/images/4.4-anomaly-email.png`.
{{% /notice %}}

---

## Screenshot 2 — CloudWatch AnomalyScore metric

Navigate to: **CloudWatch Console → Metrics → All metrics → Custom namespaces → OTT/SearchPipeline → Genre**

1. Select `AnomalyScore` with dimension `Genre=THE_THAO`
2. Click **Graph selected**
3. Set time range to **Last 30 minutes**

{{% notice tip %}}
**📸 Screenshot 2:** Capture the line chart. The spike should be clearly visible and well above any ±3.0 reference line you add (click **Add a math expression → Horizontal annotation → value: 3**). Save as `workshop/static/images/4.4-anomaly-score.png`.
{{% /notice %}}

---

## Screenshot 3 — X-Ray service map

Navigate to: **CloudWatch Console → X-Ray → Service map**

Set time range to cover the anomaly injection run (last 30 minutes).

You should see the Lambda node (`ott-anomaly-detector-dev`) with three outbound connections:
- **DynamoDB** (GetItem for baseline lookup)
- **EventBridge** (PutEvents for anomaly event)
- **CloudWatch** (PutMetricData for `AnomalyScore`)

{{% notice tip %}}
**📸 Screenshot 3:** Capture the service map with all three outbound connections visible from the Lambda node. Hover over the DynamoDB edge to show its average latency annotation. Save as `workshop/static/images/4.4-xray-map.png`.
{{% /notice %}}

---

## Screenshot 4 — DynamoDB anomaly events

Navigate to: **DynamoDB Console → Tables → `ott-anomaly-events-dev` → Explore items**

To filter by genre, use the GSI:
1. Switch the table view to **Index: genre-ts-index**
2. Set **Partition key = THE_THAO** and click **Run**

{{% notice tip %}}
**📸 Screenshot 4:** Capture the items list showing at least 3 rows with `is_anomaly: true`, `derived_genre: THE_THAO`, `z_score > 3`, `anomaly_type: SPIKE`. Save as `workshop/static/images/4.4-dynamodb-anomalies.png`.
{{% /notice %}}

{{% notice note %}}
If you did not run the anomaly injection, you will see DROP anomalies instead of SPIKEs. These occur when the baseline is seeded from a data subset and the replay volume is lower than expected. Both demonstrate that the detector is working — the anomaly type depends on whether observed volume is above or below the baseline mean.
{{% /notice %}}

---

## Verify via CLI

```bash
# Count anomaly events
aws dynamodb scan \
  --table-name ott-anomaly-events-dev \
  --select COUNT \
  --query Count \
  --output text

# Query THE_THAO anomalies via GSI
aws dynamodb query \
  --table-name ott-anomaly-events-dev \
  --index-name genre-ts-index \
  --key-condition-expression "derived_genre = :g" \
  --filter-expression "is_anomaly = :t" \
  --expression-attribute-values '{
    ":g": {"S": "THE_THAO"},
    ":t": {"BOOL": true}
  }' \
  --query "Count" \
  --output text
```
