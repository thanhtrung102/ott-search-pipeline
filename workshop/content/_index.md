---
title: "OTT Search Pipeline Workshop"
---

# OTT Search Analytics Pipeline

A dual-path AWS analytics pipeline for Vietnamese OTT search behavior — built entirely as CDK Python, deployable from a single `cdk deploy --all` command.

## What the pipeline answers

| Question | Answer |
|---|---|
| What is trending? | Top-50 keywords per genre × platform, with 7-day rank deltas, by 02:00 each morning |
| Is something wrong right now? | Z-score alert within 5 minutes of a genre's search rate deviating ≥ 3σ |
| Where is the search experience failing? | Abandonment rate heatmap — UNKNOWN × SmartTV at **30.95%** |

## Verified results (June 2022 dataset, run 2026-05-08)

| Metric | Value |
|---|---|
| Source events | 1,146,996 (14 daily Parquet folders) |
| Curated records | **1,334,620** total; **1,333,242** valid |
| Gold keyword_trends rows | **4,761** |
| DynamoDB baseline slots | **192** (8 genres × 24 hours) |
| DynamoDB anomaly events | **65,148** (65,051 DROP, 97 SPIKE) |
| Step Functions pipeline duration | **10 minutes 10 seconds** |
| Glue ETL duration | **260 seconds** (G.1X, 10 DPU) |
| Anomaly alert latency | **≤ 5 minutes** |
| Demo cost | **~$9.60** |

## Workshop sections

| Section | Content |
|---|---|
| [0. Project Proposal]({{% relref "0-proposal" %}}) | Business problem, architecture, verified success metrics |
| [1. Introduction]({{% relref "1-introduction" %}}) | Problem statement, user stories, key findings |
| [2. Prerequisites]({{% relref "2-prerequisites" %}}) | Tools, accounts, dataset upload |
| [3. Architecture]({{% relref "3-architecture" %}}) | Full pipeline diagram and schema |
| [4. Implementation]({{% relref "4-implementation" %}}) | Step-by-step deployment with verified outputs |
| [5. Results]({{% relref "5-results" %}}) | Pipeline outputs and data analysis |
| [6. Clean Up]({{% relref "6-cleanup" %}}) | Tear-down in reverse dependency order |
| [7. Live Demo Script]({{% relref "7-live-demo" %}}) | 15-minute walkthrough guide |
