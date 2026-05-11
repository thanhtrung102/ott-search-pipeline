---
title: "4.6 QuickSight Dashboards (Iteration 5)"
weight: 46
pre: "<b>4.6 </b>"
---

## Context

QuickSight connects to Athena via the `ott-analytics-demo` workgroup. The `keyword_trends` dataset is imported into SPICE — a columnar in-memory store that enables sub-second response without scanning S3 on every interaction. SPICE refreshes daily at 03:00 UTC+7, one hour after Step Functions completes.

{{% notice info %}}
QuickSight Standard cannot be provisioned via CDK. This section documents manual console steps. Complete Prerequisites Step 10 (QuickSight sign-up) before starting.
{{% /notice %}}

---

## Step 1 — Create Athena data source

1. **QuickSight → Datasets → New dataset**
2. Choose **Athena**
3. Data source name: `ott-analytics-demo`
4. Workgroup: `ott-analytics-demo`
5. Click **Create data source**
6. Database: `ott_search_gold` → Table: `keyword_trends`
7. Choose **Import to SPICE** → **Visualize**

SPICE import takes ~2 minutes for 6,908 rows.

---

## Step 2 — Dashboard 1: Search Quality Profile (lead insight)

**Visual type:** Pivot table with conditional formatting (heat map)
- **Rows:** `derived_genre`
- **Columns:** `platform_group`
- **Values:** `abandonment_rate` (Average)
- **Conditional formatting:** Blue gradient — darker = higher abandonment

**Verified insight:** UNKNOWN × SmartTV is the darkest cell at **~30.87% abandonment**. UNKNOWN × Android is second at **~13.91%**. THE_THAO × OTTBox is **~6.2%**. The primary signal is classifier coverage: the rule-based LUT classifies ~43.5% of keyword volume; the 56.5% falling to UNKNOWN drives the high SmartTV abandonment.

{{% notice tip %}}
**📸 Screenshot 1:** Capture the heatmap with all genre × platform cells visible. UNKNOWN × SmartTV must be the darkest cell. Save as `workshop/static/images/4.6-quality-profile.png`.
{{% /notice %}}

Caption: "UNKNOWN × SmartTV shows ~30.87% search abandonment — nearly 1-in-3 SmartTV free-text searches produce no useful result. The failure is classifier coverage, not a device-specific content gap: the search index has the content, but the classifier cannot map the free-text query to it."

---

## Step 3 — Dashboard 2: Keyword Leaderboard

**Visual type:** Horizontal bar chart
- **Y-axis:** `keyword_norm`
- **X-axis:** `enter_count`
- **Color:** `rank_delta` (positive = rising, negative = falling)
- **Filter controls:** `derived_genre`, `platform_group`

**Add calculated field for trend icon:**
```
ifelse(rank_delta > 0, "↑", ifelse(rank_delta < 0, "↓", "→"))
```

**Verified insight (NHAC × SmartTV):** NHAC has 392 distinct keyword slots — the most diverse search vocabulary of any genre. Bolero keywords consistently rank at the top across all platform groups. Keywords with `rank_7d_ago = 9999` are new entrants — they did not appear in the top 50 seven days prior.

{{% notice tip %}}
**📸 Screenshot 2:** Set filters `derived_genre=NHAC`, `platform_group=SmartTV`. Capture the bar chart showing bolero keywords at top, with rank delta indicators visible. Save as `workshop/static/images/4.6-leaderboard.png`.
{{% /notice %}}

---

## Step 4 — Dashboard 3: Anomaly Timeline

For this dashboard, create a second dataset from the DynamoDB anomaly table (query via Athena if you exported anomaly events to S3, or use the DynamoDB connector in QuickSight Enterprise):

```sql
SELECT anomaly_id, derived_genre, hour_of_day_vn,
       z_score, anomaly_type, observed_count, score_ts
FROM ott_anomaly_events  -- adjust table/source as needed
```

**Visual type:** Line chart
- **X-axis:** `score_ts`
- **Y-axis:** `z_score`
- **Color:** `derived_genre`
- **Reference lines:** y = 3.0 (red dashed), y = −3.0 (red dashed)

**Verified insight:** The injected THE_THAO × hour 20 spike produces z-score >> 3 (10× volume multiplier on a baseline of known variance). The on-call engineer received the SNS email alert within 5 minutes.

{{% notice tip %}}
**📸 Screenshot 3:** Capture the line chart with both ±3.0 reference lines visible and the THE_THAO spike clearly above the +3.0 threshold. Save as `workshop/static/images/4.6-anomaly-timeline.png`.
{{% /notice %}}

---

## Step 5 — Dashboard 4: ISP × Platform Heatmap

**Visual type:** Pivot table
- **Rows:** `isp_segment`
- **Columns:** `platform_group`
- **Values:** `unique_users` (Sum)
- **Conditional formatting:** Blue gradient

**Verified insight:** VNPT subscribers show disproportionate SmartTV engagement — consistent with VNPT's fixed broadband market position (households with broadband are more likely to have a SmartTV). Viettel subscribers show higher Android proportion — consistent with Viettel's mobile network dominance.

{{% notice tip %}}
**📸 Screenshot 4:** Capture the ISP × platform pivot table with conditional formatting visible. Hover over one cell to show the exact `unique_users` value in the tooltip. Save as `workshop/static/images/4.6-isp-heatmap.png`.
{{% /notice %}}

---

## Step 6 — Configure SPICE refresh

1. **QuickSight → Datasets → keyword_trends**
2. **Refresh → Add new schedule**
3. Frequency: **Daily**, Time: **03:00** (Asia/Ho_Chi_Minh, UTC+7)
4. **Save**

---

## Step 7 — Publish dashboard

1. Analysis view → **Share → Publish dashboard**
2. Name: `OTT Search Analytics`
3. **Publish dashboard**

---

## Verify gold data for QuickSight

```bash
aws athena start-query-execution \
  --query-string "SELECT derived_genre, COUNT(*) AS slots, SUM(search_count) AS searches
                  FROM ott_search_gold.keyword_trends
                  GROUP BY derived_genre ORDER BY searches DESC" \
  --work-group ott-analytics-${CDK_ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query QueryExecutionId --output text
```

**Verified genre distribution (2026-05-10):**

| derived_genre | keyword_slots | total_searches |
|---|---|---|
| UNKNOWN | 701 | 43,884 |
| ANIME | 545 | 11,795 |
| NHAC | 1,420 | 5,899 |
| TRUYEN_HINH | 924 | 5,474 |
| THE_THAO | 1,021 | 3,965 |
| PHIM_VIET | 735 | 2,599 |
| PHIM_TRUNG | 644 | 2,072 |
| PHIM_AU_MY | 918 | 2,052 |

UNKNOWN accounts for 56.5% of total searches — 43,884 out of ~77,740. This is the honest classifier coverage number. NHAC has the most distinct keywords (392) despite lower total search volume than ANIME and UNKNOWN.
