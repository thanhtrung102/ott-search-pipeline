---
title: "4.6 QuickSight Dashboards (Iteration 5)"
weight: 46
pre: "<b>4.6 </b>"
---

## Context

QuickSight connects to Athena via the `ott-analytics-dev` workgroup. The `keyword_trends` dataset is imported into SPICE (Super-fast Parallel In-memory Calculation Engine) — a columnar in-memory store that enables sub-second query response without scanning S3 on every dashboard interaction. SPICE refreshes daily at 03:00 UTC+7, one hour after Step Functions completes.

{{% notice info %}}
QuickSight cannot be provisioned via CDK at the Standard tier. This section documents the console setup steps. Screenshots were captured from the `demo` environment.
{{% /notice %}}

---

## Setup (console steps)

### Step 1 — Sign up for QuickSight

1. Open **AWS Console → QuickSight**
2. Click **Sign up for QuickSight**
3. Select **Standard** tier (~$9/month per author, cancel after demo)
4. Region: **Asia Pacific (Singapore) ap-southeast-1**
5. Account name: `ott-search-analytics`
6. Notification email: your address
7. Under **IAM role**: allow QuickSight to access Amazon Athena
8. Under **S3 buckets**: select `ott-search-{account}-dev`
9. Click **Finish**

---

### Step 2 — Create dataset

1. **QuickSight → Datasets → New dataset**
2. Choose **Athena**
3. Data source name: `ott-analytics-dev`
4. Workgroup: `ott-analytics-dev`
5. Click **Create data source**
6. Select database: `ott_search_gold`
7. Select table: `keyword_trends`
8. Click **Select**
9. Choose **Import to SPICE** (for sub-second performance)
10. Click **Visualize**

SPICE import takes approximately 2–3 minutes for the 4,761-row gold table.

---

### Step 3 — Create the four dashboards

Create an Analysis with four sheets, one per dashboard:

---

#### Dashboard 1 — Search Quality Profile (abandonment heatmap)

This is the lead insight dashboard. Build it first.

**Visual type:** Pivot table (heat map)
- **Rows:** `derived_genre`
- **Columns:** `platform_group`
- **Values:** `abandonment_rate` (aggregation: Average)
- **Conditional formatting:** Blue scale — darker = higher abandonment

**Interpretation:** UNKNOWN × Android should be the darkest cell — free-text searches on Android that matched no known content fail at ~19.8%. THE_THAO × OTTBox (~1.4%) is mid-range. This is the primary product finding.

{{% notice tip %}}
**📸 Screenshot 1:** Capture the full QuickSight dashboard. The pivot table heatmap must be visible with `derived_genre` rows and `platform_group` columns. UNKNOWN × Android should be the darkest cell (~19.8% abandonment). The filter panel showing `platform_group` control should be visible on the right. Save as `workshop/static/images/4.6-quality-profile.png`.
{{% /notice %}}

Caption: "UNKNOWN category on Android has the highest search abandonment rate (~19.8%) — free-text searches that matched no known content pattern. THE_THAO × OTTBox sits at ~1.4%, indicating sports searches on set-top boxes are resolving reasonably. The UNKNOWN bucket is the primary product signal: improving keyword classification coverage on Android would most reduce abandonment."

---

#### Dashboard 2 — Keyword Leaderboard

**Visual type:** Bar chart (horizontal)
- **Y-axis:** `keyword_norm`
- **X-axis:** `enter_count`
- **Color:** `rank_delta` (positive = rising, negative = falling)
- **Filter:** `derived_genre` (control), `platform_group` (control)

**Add a calculated field for rank trend icon:**
```
ifelse(rank_delta > 0, "↑", ifelse(rank_delta < 0, "↓", "→"))
```

{{% notice tip %}}
**📸 Screenshot 2:** Set filters to `derived_genre=NHAC`, `platform_group=SmartTV`. Capture the bar chart with rank trend indicators visible next to keyword names. The filter panel showing both filters active should be visible. Save as `workshop/static/images/4.6-leaderboard.png`.
{{% /notice %}}

Caption: "Top music (NHAC) searches on SmartTV for June 2022. Bolero keywords dominate. Rank delta shows keywords that were not in the top 50 seven days prior have rank_7d_ago=9999 (shown as blank or 9999)."

---

#### Dashboard 3 — Anomaly Timeline

**Visual type:** Line chart
- **X-axis:** `score_ts` (from `ott-anomaly-events-dev` — requires a second dataset)
- **Y-axis:** `z_score`
- **Color:** `derived_genre`
- **Reference lines:** y = 3.0 (red), y = −3.0 (red)

To add the anomaly data, create a second dataset from Athena:
```sql
SELECT anomaly_id, derived_genre, hour_of_day_vn, 
       z_score, anomaly_type, observed_count, score_ts
FROM ott_search_curated.search_enriched  -- Note: query anomaly table via Athena
```

Or connect directly to DynamoDB via the DynamoDB connector (available in QuickSight Enterprise tier).

{{% notice tip %}}
**📸 Screenshot 3:** Capture the line chart with `z_score` on Y-axis and both ±3.0 reference lines visible. The THE_THAO spike should be clearly visible above the +3.0 line. Save as `workshop/static/images/4.6-anomaly-timeline.png`.
{{% /notice %}}

Caption: "Z-score spike at 20:00 Vietnam time in the THE_THAO genre — the injected 10× sports search anomaly. The reference lines at ±3.0 define the alert threshold. Under normal distribution, a z-score this extreme has < 0.3% probability of occurring by chance."

---

#### Dashboard 4 — ISP × Platform Heatmap

**Visual type:** Pivot table
- **Rows:** `isp_segment`
- **Columns:** `platform_group`
- **Values:** `unique_users` (aggregation: Sum)
- **Conditional formatting:** Blue scale

{{% notice tip %}}
**📸 Screenshot 4:** Capture the pivot table with ISP names as rows and platform groups as columns. Conditional formatting (darker = higher `unique_users`) must be visible. Hover over one cell to show the tooltip with the exact `unique_users` value. Save as `workshop/static/images/4.6-isp-heatmap.png`.
{{% /notice %}}

Caption: "ISP × platform usage distribution. VNPT subscribers show disproportionate SmartTV usage consistent with VNPT's broadband cable penetration. Viettel dominates Android, consistent with its mobile network market share."

---

### Step 4 — Configure SPICE refresh

1. **QuickSight → Datasets → keyword_trends**
2. **Refresh → Add new schedule**
3. Frequency: **Daily**
4. Time: **03:00** (timezone: Asia/Ho_Chi_Minh, UTC+7)
5. Click **Save**

This runs one hour after Step Functions completes (pipeline completes by 02:00), ensuring SPICE always reflects the latest day's data.

---

### Step 5 — Publish as dashboard

1. From the Analysis view, click **Share → Publish dashboard**
2. Name: `OTT Search Analytics`
3. Click **Publish dashboard**
4. Share with specific users or groups as needed

---

## Verification

```bash
# Confirm gold table has data for QuickSight to read
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

aws athena start-query-execution \
  --query-string "SELECT derived_genre, COUNT(*) AS slots, SUM(search_count) AS searches
                  FROM ott_search_gold.keyword_trends
                  GROUP BY derived_genre ORDER BY searches DESC" \
  --work-group ott-analytics-dev \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query QueryExecutionId --output text
```

From the June 2022 run, expected genre distribution:

| derived_genre | keyword_slots | total_searches |
|---|---|---|
| UNKNOWN | 560 | 97,451 |
| ANIME | 519 | 26,060 |
| NHAC | 668 | 14,371 |
| TRUYEN_HINH | 616 | 11,525 |
| THE_THAO | 615 | 11,209 |
| PHIM_VIET | 777 | 6,532 |
| PHIM_AU_MY | 587 | 4,532 |
| PHIM_TRUNG | 419 | 3,860 |

The UNKNOWN bucket (560 slots, 55.5% of total searches) represents keywords that the rule-based classifier and LUT did not match. This is an honest acknowledgment of classifier uncertainty — not a misclassification. The LLM fallback path (disabled in this demo: `LLM_ENABLED=false`) handles the long tail in production.
