---
title: "OTT Search Pipeline Workshop"
---

# OTT Search Pipeline

Build a real-time and batch analytics pipeline for Vietnamese OTT search behavior on AWS.

## What you will build

A dual-path data pipeline that processes FPT Play search events:

- **Real-time path** — Kinesis Data Streams → anomaly-detector Lambda → DynamoDB → SNS alert within 5 minutes of an anomaly
- **Batch path** — Firehose → S3 raw → Glue ETL → Athena CTAS → S3 gold → QuickSight dashboard by 02:00 each morning

## Three questions the pipeline answers

1. **What is trending?** Top 50 keywords per genre and platform, with 7-day rank deltas
2. **Is something wrong right now?** Z-score alert when a genre's search rate deviates ≥ 3σ from its 7-day rolling baseline
3. **Where is the search experience failing?** Abandonment rate heatmap by genre × platform

## Structure

| Section | Content |
|---|---|
| [1. Introduction]({{% relref "1-introduction" %}}) | Problem statement and business context |
| [2. Prerequisites]({{% relref "2-prerequisites" %}}) | Tools, accounts, and dataset |
| [3. Architecture]({{% relref "3-architecture" %}}) | Full pipeline diagram and data-flow schema |
| [4. Implementation]({{% relref "4-implementation" %}}) | Step-by-step deployment across 8 iterations |
| [5. Results]({{% relref "5-results" %}}) | What the pipeline produced from June 2022 data |
| [6. Clean Up]({{% relref "6-cleanup" %}}) | Tear-down in reverse dependency order |
| [7. Live Demo Script]({{% relref "7-live-demo" %}}) | 15-minute walkthrough guide |

---

> **Note:** Screenshots throughout this workshop were captured from a `demo` environment. When you deploy with `--context env=dev`, resource names ending in `-demo` will appear as `-dev` in your account. All commands are written for `env=dev`.
