---
title: "7. Live Demo Script"
weight: 70
chapter: true
pre: "<b>7. </b>"
---

# Live Demo Script

A **10–15 minute** walkthrough of the four QuickSight dashboards plus one terminal command proving the governance boundary. The pipeline does not need to re-run — data is already in QuickSight SPICE.

**Prepare:** Open the four QuickSight dashboards in separate tabs. Open a terminal with your AWS credentials loaded. Have the governance commands (Step 5) ready to paste.

---

## Opening (1 minute)

> "FPT Play has approximately 81,928 search events per day from June 2022 — across sports, music, anime, drama, and Vietnamese film. This pipeline answers three questions the raw event data cannot: **what is trending, what is anomalous, and where is the search experience failing.** I will show you the answer to each question — starting with the most important one."

Navigate to the **Search Quality Profile** dashboard (lead with the insight, not the data volume).

---

## Dashboard 1 — Search Quality Profile (4 minutes)

**Open the abandonment rate heatmap.**

> "Each cell is the abandonment rate for a content genre × device class combination. Abandonment means: the user typed a search query, got results, and left without clicking anything. A higher rate means the search results are not relevant."

Point to the darkest cell (UNKNOWN × SmartTV):

> "The UNKNOWN category on SmartTV has **30.87% abandonment** — nearly one in three users who typed a free-text search on a Smart TV got no useful result and quit. UNKNOWN means the keyword did not match any genre in the classifier: no bolero lookup hit, no sports term regex, nothing. These are searches the platform cannot answer."

Pause.

> "Now look at the genre dimension. THE_THAO × Android is around 5.5% — low abandonment. Sports searches on Android are resolving well. THE_THAO × OTTBox is 6.2%. The failure is **classifier coverage**, not a content gap on a specific device class. The search index has the content — the classifier cannot map the free-text query to it."

> "A product manager seeing this dashboard files a ticket to expand the genre classifier — not a CDN or search-index bug. Without the pipeline, this signal was buried inside raw event rows that nobody could query. A product team seeing abandonment_rate = 0.31 on UNKNOWN × SmartTV can prioritise classifier work in the next sprint."

---

## Dashboard 2 — Anomaly Timeline (2 minutes)

**Switch to the anomaly timeline.**

> "The real-time path computes a z-score for every (genre, hour) combination every time a new Kinesis batch arrives — typically every 30–60 seconds. A z-score above 3 means the observed search volume is more than 3 standard deviations above the 7-day rolling mean for that genre at that hour."

Point to the THE_THAO spike at hour 20:

> "This spike was injected synthetically to demonstrate the alert. The sports (THE_THAO) search volume at 20:00 Vietnam time was multiplied by 10×. Under normal distribution, a z-score of 28 has less than one-in-a-billion probability of occurring by chance. This is not a traffic fluctuation — this is a structural event."

> "The on-call engineer received this alert as an email — via SNS — within 5 minutes of the spike beginning. Before any user filed a support ticket."

Show the reference lines at ±3.0:

> "These are the thresholds. A spike above +3 means anomalously high volume — a viral event, a system incident flooding retries, or a bot pattern. A drop below −3 means anomalously low volume — a CDN outage, a search service failure, or a content removal event. Both directions matter."

---

## Dashboard 3 — Keyword Leaderboard (2 minutes)

**Switch to the keyword leaderboard. Set filters: `derived_genre = NHAC`, `platform_group = SmartTV`.**

> "Music (NHAC) is the highest-volume genre by distinct keyword count — 392 unique search terms in 14 days. Bolero keywords consistently rank at the top across all platform groups."

Point to a keyword with a positive `rank_delta`:

> "This keyword was ranked lower last week. It is rising. A positive rank delta is a content acquisition signal. If FPT Play does not have licensing for the top bolero content — the songs users are searching for right now — those users will find the same content on a competitor."

Point to a keyword with `rank_7d_ago = 9999`:

> "This keyword has no history from 7 days ago. It did not appear in the top 50 last week. It appeared this week. That is an emerging search term — a new trend before it peaks. The 7-day window makes new entrants visible rather than invisible."

---

## Dashboard 4 — ISP × Platform Heatmap (1 minute)

**Switch to the ISP × platform heatmap.**

> "This shows where unique users are concentrated by network provider and device class. VNPT subscribers on SmartTV are the highest-engagement segment. This is consistent with VNPT's fixed broadband market position — households with a broadband connection are more likely to have a smart TV in the living room than a mobile-first Viettel subscriber."

> "This tells a content deal team: if you are negotiating a sponsorship or an exclusive streaming window, SmartTV × VNPT is the audience you are buying access to."

---

## Governance demonstration (2 minutes)

**Switch to terminal.**

> "Before I close: I want to show you the data governance boundary. Marketing analysts can use these dashboards without ever seeing an individual user's data."

```bash
# Assume the Marketing role
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ENV=${CDK_ENV:-demo}
eval $(aws sts assume-role \
  --role-arn "arn:aws:iam::${ACCOUNT}:role/ott-marketing-${ENV}" \
  --role-session-name live-demo \
  --query 'Credentials' \
  --output json | python3 -c "
import json,sys
c=json.load(sys.stdin)
print(f'export AWS_ACCESS_KEY_ID={c[\"AccessKeyId\"]}')
print(f'export AWS_SECRET_ACCESS_KEY={c[\"SecretAccessKey\"]}')
print(f'export AWS_SESSION_TOKEN={c[\"SessionToken\"]}')
")
BUCKET="ott-search-${ACCOUNT}-${ENV}"

# Attempt 1: query curated layer — WILL FAIL
QID=$(aws athena start-query-execution \
  --query-string "SELECT user_id_hashed FROM ott_search_curated.search_enriched LIMIT 1" \
  --work-group ott-analytics-${ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/demo/" \
  --query QueryExecutionId --output text)
sleep 5
aws athena get-query-execution --query-execution-id $QID \
  --query 'QueryExecution.Status.{State:State,Reason:StateChangeReason}' --output json
```

Show the `FAILED` output:

> "Access denied at S3 — the Marketing role's IAM policy is scoped to the `gold/` prefix. It cannot read `curated/`. Lake Formation additionally restricts which columns are visible in the gold layer."

```bash
# Attempt 2: query gold layer — WILL SUCCEED
QID=$(aws athena start-query-execution \
  --query-string "SELECT keyword_norm, abandonment_rate, rank_delta
                  FROM ott_search_gold.keyword_trends
                  WHERE derived_genre = 'THE_THAO'
                  ORDER BY search_count DESC LIMIT 5" \
  --work-group ott-analytics-${ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/demo/" \
  --query QueryExecutionId --output text)
sleep 10
aws athena get-query-results --query-execution-id $QID \
  --query "ResultSet.Rows[*].Data[*].VarCharValue" --output json
# Note: On Windows, this may print a charmap encoding error for Vietnamese characters.
# Verify success via: aws athena get-query-execution --query-execution-id $QID --query 'QueryExecution.Status.State' --output text

# Restore credentials
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
```

> "The same role. Five keyword rows, abandonment rates, rank deltas. No user IDs, no authentication patterns, no subscription revenue data. IAM + Lake Formation together enforce this boundary."

---

## Closing (1 minute)

**Return to the architecture diagram (Section 3).**

> "Every resource in this demo — VPC, KMS keys, Kinesis, Firehose, Glue, Step Functions, DynamoDB, SNS, Lake Formation, CloudTrail, GuardDuty, Macie — was deployed from a single CDK command: `cdk deploy --all`. It can be destroyed with `cdk destroy --all`. Total infrastructure cost for this demo run was approximately **$9.60**, dominated by a one-month QuickSight subscription."

> "The pipeline answers the question we started with: where is the search experience failing? On SmartTV, **nearly 31% of free-text searches produce no useful result** (30.87%). The classifier is missing the long tail of search vocabulary on the living-room device class. This dashboard makes that visible in under 5 minutes — and points directly at the fix."

---

## Backup Q&A

| Question | Answer |
|---|---|
| How accurate is the genre classifier? | UNKNOWN = 56.5% of keywords. Honest, not a failure. LLM fallback handles long tail in production. |
| What happens if Glue fails? | Step Functions → PipelineFailure state → SNS → CloudWatch composite alarm fires within 26h. Next run reprocesses via predicate. |
| Why Kinesis instead of just Glue on S3? | Anomaly detection needs ≤5 min latency. Glue startup alone is 5–10 min. Kinesis delivers to Lambda in seconds. |
| Why PySpark native writer instead of Glue DynamicFrame? | Raw Parquet embeds `derived_genre` as both a data column and partition path. DynamicFrame raises `COLUMN_ALREADY_EXISTS`. PySpark DataFrameReader with explicit schema and basePath avoids this. |
| How does rank_delta work for new keywords? | `COALESCE(rank_7d_ago, 9999)`. New keywords get rank_7d_ago=9999, so rank_delta = 9999 − rank_today. Clearly marks new entrants. |
| What is `is_cross_partition_date`? | Year in event_ts ≠ year in dt partition. Filters out Buddhist-era and year-0004 corrupt rows that landed in a wrong S3 partition. |
