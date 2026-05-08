---
title: "1. Introduction"
weight: 10
chapter: true
pre: "<b>1. </b>"
---

# Introduction

## Problem statement

FPT Play's Product Analytics team has no way to detect emerging content trends or search experience failures in real time. The raw `log_search` table exists in a data warehouse, but it is refreshed once per day in batch — which means a sports search failure at 20:00 (prime time for live football) is not visible until 08:00 the next morning, eight hours after the event.

This pipeline closes that gap.

## User stories

> *As a Marketing analyst, I want to see which keywords are trending in C-drama searches on SmartTV this week versus last week, so that I can recommend content acquisition targets to the programming team.*

> *As an on-call engineer, I want to receive an alert within 5 minutes when search volume for any genre deviates abnormally, so that I can investigate a potential CDN failure, search index outage, or sudden viral event before user complaints arrive.*

> *As a Data Governance officer, I want Marketing analysts to be able to query keyword trend data without seeing individual user activity or authentication status, so that the platform complies with its data privacy commitments.*

## Why this matters

Sports content (THE_THAO) exhibits **14.3% search abandonment** in the June 2022 dataset — users searching for live football matches on OTTBox set-top boxes type a query, get no useful results, and quit. These are disproportionately K+ premium subscribers — the platform's highest-value users hitting a search index gap for live broadcast content on this device class.

The pipeline surfaces this within minutes of it occurring, rather than the next morning.

## What is being built

The pipeline processes 14 days of `log_search` Parquet files (1,146,996 source events) through a two-layer architecture deployed entirely from CDK:

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
 dt/hour/genre/platform)
    │
    ▼
Glue Crawler → Glue ETL (18-step enrichment)
    │
    ▼
S3 curated/search_enriched/
    │
    ▼
Step Functions → Athena CTAS
    │
    ▼
S3 gold/keyword_trends/ → QuickSight dashboard
```

All infrastructure is defined as CDK Python. A reader with this repository can reproduce the entire deployed state from a single `cdk deploy --all` command.
