---
title: "1. Introduction"
weight: 10
chapter: true
pre: "<b>1. </b>"
---

# Introduction

## Problem statement

FPT Play's Product Analytics team has no way to detect emerging content trends or search experience failures in real time. The raw `log_search` table exists in a data warehouse, but it refreshes once per day in batch — which means a sports search failure at 20:00 (prime time for live football) is not visible until 08:00 the next morning.

This pipeline closes that gap with two paths built on the same Kinesis stream:

- **Real-time path** — anomaly-detector Lambda computes z-scores and sends SNS alerts within 5 minutes
- **Batch path** — Firehose → Glue ETL → Step Functions → Athena CTAS → QuickSight by 02:00 daily

## User stories

> *As a Marketing analyst, I want to see which keywords are trending in C-drama searches on SmartTV this week versus last week, so that I can recommend content acquisition targets to the programming team.*

> *As an on-call engineer, I want to receive an alert within 5 minutes when search volume for any genre deviates abnormally, so that I can investigate a potential CDN failure, search index outage, or sudden viral event before user complaints arrive.*

> *As a Data Governance officer, I want Marketing analysts to be able to query keyword trend data without seeing individual user activity or authentication status, so that the platform complies with its data privacy commitments.*

## Key findings from the data

All figures verified against the `demo` environment on **2026-05-10**:

| Finding | Value |
|---|---|
| Highest abandonment | **UNKNOWN × SmartTV: 30.87%** — nearly 1-in-3 SmartTV free-text searches produce no useful result |
| Second-highest abandonment | UNKNOWN × Android: 13.91% |
| Sports (THE_THAO) overall | 7.02% abandonment; OTTBox: 6.16%; Android: 5.51% |
| Classifier coverage | Rule-based LUT covers ~43.5% of keyword volume; 56.5% falls to UNKNOWN |
| Music keyword diversity | NHAC: 392 distinct keyword slots in gold layer — highest of any genre |

The primary signal: the UNKNOWN × SmartTV abandonment is a **classifier coverage problem**, not a content gap. The search index has the content; the classifier cannot map the free-text query to it. This pipeline makes that visible within minutes of occurrence.

## What is being built

```
log_search Parquet files (S3)
         │
         ▼
  replay-producer Lambda
  (enriches: derived_genre, platform_group)
         │
         ▼
  Kinesis Data Streams (2 shards)
         │
    ┌────┴────────────────────┐
    │                         │
    ▼                         ▼
Kinesis Firehose         anomaly-detector Lambda
(JSON → Parquet)         (z-score vs DynamoDB baseline)
    │                         │
    ▼                         ▼
S3 raw/events/           DynamoDB ott-anomaly-events
(partitioned by          EventBridge → SNS alert
 dt/hour/genre/platform) (≤ 5 min latency)
    │
    ▼
Glue Crawler → Glue ETL (18-step enrichment, ~200s)
    │
    ▼
S3 curated/search_enriched/ (992,650 records)
    │
    ▼
Step Functions → Athena CTAS (~12 min total)
    │
    ▼
S3 gold/keyword_trends/ (6,908 rows) → QuickSight
```

All infrastructure is CDK Python. Reproducible from `cdk deploy --all` in under 15 minutes.
