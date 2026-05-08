---
title: "4.4 Anomaly Detection (Iteration 3)"
weight: 44
pre: "<b>4.4 </b>"
---

## What this deploys

- **DynamoDB `ott-baseline-stats-dev`** — rolling 7-day mean and std per (genre, hour) slot; 192 items (8 genres × 24 hours)
- **DynamoDB `ott-anomaly-events-dev`** — anomaly records with TTL 30 days; KMS encrypted
- **anomaly-detector Lambda** — Kinesis ESM consumer (batch=100, bisect-on-error); computes z-score, writes DynamoDB, publishes EventBridge
- **baseline-updater Lambda** — invoked by Step Functions; updates rolling baseline from curated layer
- **EventBridge rule** — `SearchAnomalyDetected` → SNS `ott-anomaly-alerts-dev`
- **SNS topic** — email subscription to `ALERT_EMAIL`

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

**Deploy time:** approximately 5 minutes.

After deploy, **confirm the SNS email subscription** — AWS sends a confirmation link to `ALERT_EMAIL`. Alerts will not arrive until confirmed.

---

## Seed the baseline

The anomaly detector compares observed counts against a pre-computed baseline. For the June 2022 historical dataset, seed the baseline from that month's data:

```bash
# Set the baseline date range on the Lambda (one-time, for demo)
aws lambda update-function-configuration \
  --function-name ott-baseline-updater-${CDK_ENV} \
  --environment "Variables={BASELINE_DATE_FROM=2022-06-01,DDB_BASELINE_TABLE=ott-baseline-stats-${CDK_ENV}}"

# Invoke the baseline updater
aws lambda invoke \
  --function-name ott-baseline-updater-${CDK_ENV} \
  --payload '{}' \
  /tmp/baseline_result.json

cat /tmp/baseline_result.json
```

**Verified output (2026-05-08):**
```json
{"slots_written": 192}
```

192 slots = 8 genres × 24 hours. Each slot stores `mean` and `std` of search count for that (genre, hour) combination.

### Verify baseline in DynamoDB

```bash
aws dynamodb scan \
  --table-name ott-baseline-stats-${CDK_ENV} \
  --select COUNT \
  --query 'Count' \
  --output text
# Expected: 192
```

---

## Z-score formula

```
z = (observed_count - rolling_mean) / max(rolling_std, 1.0)
```

Threshold: |z| > 3.0 triggers an anomaly event. The `max(std, 1.0)` floor prevents division by zero for (genre, hour) combinations with very low historical variance.

---

## Proof: anomaly detection firing

After running the replay (Section 4.2) with the anomaly injection payload, check DynamoDB for anomaly events:

```bash
aws dynamodb scan \
  --table-name ott-anomaly-events-${CDK_ENV} \
  --select COUNT \
  --query 'Count' \
  --output text
```

**Verified output (2026-05-08):**
```
65148
```

Breakdown:
- **65,051 DROP** anomalies — expected: the initial baseline was seeded before full June data was replayed, so observed counts were lower than the seeded baseline for most slots
- **97 SPIKE** anomalies — genuine high-volume events including the injected THE_THAO × hour 20 spike

```bash
# Check the SPIKE anomalies specifically
aws dynamodb query \
  --table-name ott-anomaly-events-${CDK_ENV} \
  --index-name anomaly-type-index \
  --key-condition-expression "anomaly_type = :t" \
  --expression-attribute-values '{":t": {"S": "SPIKE"}}' \
  --select COUNT \
  --query 'Count' \
  --output text
# Expected: 97
```

---

## Proof: EventBridge → SNS alert path

```bash
# Check CloudWatch for AnomalyScore metric (published by anomaly-detector)
aws cloudwatch get-metric-statistics \
  --namespace OTT/SearchPipeline \
  --metric-name AnomalyScore \
  --dimensions Name=Genre,Value=THE_THAO \
  --start-time $(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 7200 \
  --statistics Maximum \
  --query 'sort_by(Datapoints, &Timestamp)[-1].Maximum' \
  --output text
# Expected: >3.0 if injection was run (THE_THAO 10× spike produces z >> 3)
```

```bash
# Verify SNS subscription confirmed
aws sns list-subscriptions-by-topic \
  --topic-arn $(aws sns list-topics \
    --query 'Topics[?contains(TopicArn,`ott-anomaly-alerts`)].TopicArn' \
    --output text) \
  --query 'Subscriptions[0].{Protocol:Protocol,Status:SubscriptionArn}' \
  --output json
# Expected: {"Protocol": "email", "Status": "arn:aws:sns:..."}  (not PendingConfirmation)
```

---

## Resources deployed

| Resource | Name | Configuration |
|---|---|---|
| DynamoDB | `ott-baseline-stats-dev` | On-demand, KMS (`ott-dynamodb-key`), PAY_PER_REQUEST |
| DynamoDB | `ott-anomaly-events-dev` | On-demand, KMS, TTL attribute `expiry_ts` (30 days) |
| Lambda | `ott-anomaly-detector-dev` | Kinesis ESM: batch=100, bisect-on-error, 512 MB |
| Lambda | `ott-baseline-updater-dev` | Invoked by Step Functions + manual seed |
| EventBridge Rule | `SearchAnomalyDetected` | Pattern: `detail-type = SearchAnomalyDetected` → SNS |
| SNS Topic | `ott-anomaly-alerts-dev` | KMS encrypted (`ott-sns-key`), email subscription |

---

## Screenshot guidance

**Screenshot 1 — DynamoDB baseline table**
Navigate to: **DynamoDB Console → Tables → `ott-baseline-stats-dev` → Explore items**.
Capture 3–5 items showing `derived_genre`, `hour_of_day_vn`, `mean`, `std` columns.
Save as `workshop/static/images/4.4-baseline-table.png`.

**Screenshot 2 — DynamoDB anomaly events**
Navigate to: **DynamoDB Console → Tables → `ott-anomaly-events-dev` → Explore items**.
Filter by `anomaly_type = SPIKE`. Capture items showing THE_THAO spike with z_score > 3.
Save as `workshop/static/images/4.4-anomaly-events.png`.

**Screenshot 3 — SNS confirmation email**
Show the SNS email alert received after the injection replay. Subject line should contain `SearchAnomalyDetected` or `OTT Search Anomaly`.
Save as `workshop/static/images/4.4-sns-email.png`.
