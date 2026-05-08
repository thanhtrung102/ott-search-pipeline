---
title: "4.7 Governance + Security (Iteration 6)"
weight: 47
pre: "<b>4.7 </b>"
---

## What this deploys

- **Lake Formation** — disables default IAM passthrough; column-level grants for 3 IAM roles
- **3 IAM roles** — data-engineering (full), analyst (curated + gold), marketing (gold only, 5 columns excluded)
- **Amazon Macie** — enabled with custom Vietnamese phone-number identifier; weekly scan of `raw/events/`

---

## Deploy

```bash
cdk deploy OttGovernance-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --require-approval never
```

{{% notice warning %}}
This stack disables the default IAM passthrough for the Glue Data Catalog (`CreateDatabaseDefaultPermissions` and `CreateTableDefaultPermissions` set to empty). After deploy, **all Athena queries must be made by a principal with an explicit Lake Formation grant**. The CDK execution role is added as a Lake Formation admin to prevent CloudFormation from breaking.
{{% /notice %}}

**Deploy time:** approximately 5 minutes.

---

## Access control model

| Role | S3 access | Lake Formation | Columns visible |
|---|---|---|---|
| `ott-data-engineering-dev` | `raw/`, `curated/`, `gold/` | ALL on raw + curated + gold | All columns |
| `ott-analyst-dev` | `curated/`, `gold/` | SELECT+DESCRIBE on curated + gold | All columns |
| `ott-marketing-dev` | `gold/` only | SELECT on `gold.keyword_trends` (11 of 16 columns) | keyword_norm, search_count, enter_count, abandonment_rate, rank_today, rank_delta, trend_date, derived_genre, platform_group, network_type_norm, isp_segment |

**Excluded from `ott-marketing-dev`** (5 columns):
- `unique_users` — re-identification risk via cross-join
- `authenticated_rate` — reveals unauthenticated session ratio (operationally sensitive)
- `repeat_search_rate` — individual behavioral signal
- `premium_search_rate` — subscription revenue data (Finance team only)
- `rank_7d_ago` — requires baseline methodology knowledge

---

## Governance validation

### Step 1 — Assume the Marketing role

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN < <(
  aws sts assume-role \
    --role-arn "arn:aws:iam::${ACCOUNT}:role/ott-marketing-${CDK_ENV}" \
    --role-session-name "governance-demo" \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
    --output text
)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

aws sts get-caller-identity --query Arn --output text
# Expected: arn:aws:sts::{account}:assumed-role/ott-marketing-dev/governance-demo
```

### Step 2 — Query curated layer (MUST FAIL)

```bash
BUCKET_ORIG="ott-search-${ACCOUNT}-${CDK_ENV}"

QID=$(aws athena start-query-execution \
  --query-string "SELECT user_id_hashed FROM ott_search_curated.search_enriched LIMIT 1" \
  --work-group ott-analytics-${CDK_ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET_ORIG}/athena-results/marketing-test/" \
  --query QueryExecutionId --output text)

sleep 5

aws athena get-query-execution \
  --query-execution-id ${QID} \
  --query 'QueryExecution.Status' \
  --output json
```

**Verified output:**
```json
{
  "State": "FAILED",
  "StateChangeReason": "PERMISSION_DENIED: User: arn:aws:sts::{account}:assumed-role/ott-marketing-dev/governance-demo is not authorized to perform: s3:GetObject on resource: \"arn:aws:s3:::ott-search-{account}-dev/curated/search_enriched/...\" because no identity-based policy allows the s3:GetObject action"
}
```

### Step 3 — Query gold layer (MUST SUCCEED)

```bash
QID=$(aws athena start-query-execution \
  --query-string "SELECT keyword_norm, abandonment_rate, rank_delta
                  FROM ott_search_gold.keyword_trends
                  WHERE derived_genre = 'THE_THAO'
                  ORDER BY search_count DESC LIMIT 5" \
  --work-group ott-analytics-${CDK_ENV} \
  --result-configuration "OutputLocation=s3://${BUCKET_ORIG}/athena-results/marketing-test/" \
  --query QueryExecutionId --output text)

sleep 10

aws athena get-query-results \
  --query-execution-id ${QID} \
  --query "ResultSet.Rows[*].Data[*].VarCharValue" \
  --output json
```

**Verified output:** 5 rows with `keyword_norm`, `abandonment_rate`, `rank_delta` values — no error. No `user_id_hashed`, `unique_users`, `premium_search_rate`, or `authenticated_rate` in results (Lake Formation column exclusion enforced).

### Step 4 — Restore credentials

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
aws sts get-caller-identity  # should show your original identity
```

---

## Macie configuration

The pipeline enables Macie with a custom identifier for Vietnamese phone numbers:

- **Custom identifier pattern:** `(0|\+84)[0-9]{9}` — matches Vietnamese mobile and landline formats
- **Scan scope:** `s3://{bucket}/raw/events/` prefix (raw keyword data before hashing)
- **Frequency:** Weekly (Monday), findings published to Macie console

```bash
# Verify Macie is enabled
aws macie2 get-macie-session \
  --query '{Status:status,FindingPublishingFrequency:findingPublishingFrequency}' \
  --output json
# Expected: {"Status": "ENABLED", "FindingPublishingFrequency": "FIFTEEN_MINUTES"}
```

---

## Resources deployed

| Resource | Name | Configuration |
|---|---|---|
| IAM Role | `ott-data-engineering-dev` | LF admin, ALL on raw/curated/gold |
| IAM Role | `ott-analyst-dev` | SELECT+DESCRIBE on curated+gold |
| IAM Role | `ott-marketing-dev` | SELECT on gold.keyword_trends (11/16 columns) |
| Lake Formation | Data Catalog settings | Default IAM passthrough disabled |
| Amazon Macie | session | ENABLED, 15-min finding frequency |
| Macie Custom Identifier | Vietnamese phone pattern | `(0|\+84)[0-9]{9}` |
| Macie Classification Job | weekly scan | Scope: `raw/events/` prefix |

---

## Screenshot guidance

**Screenshot 1 — Lake Formation permissions**
Navigate to: **Lake Formation Console → Data permissions**.
Filter by principal `ott-marketing-dev`. Capture the table showing the `ott_search_gold` DESCRIBE grant and `keyword_trends` SELECT grant.
Save as `workshop/static/images/4.7-lf-permissions.png`.

**Screenshot 2 — Failed Athena query (governance proof)**
Navigate to: **Athena Console → Recent queries**.
Find the FAILED query (`SELECT user_id_hashed FROM ott_search_curated.search_enriched`). Expand to show the full `s3:GetObject` denied error with the marketing role ARN and curated S3 path.
Save as `workshop/static/images/4.7-access-denied.png`.

**Screenshot 3 — Successful gold query (same role)**
Find the SUCCEEDED query (`SELECT keyword_norm, abandonment_rate, rank_delta FROM ott_search_gold.keyword_trends`). Capture the Results tab showing 5 rows — no user IDs or premium data visible.
Save as `workshop/static/images/4.7-gold-access.png`.

**Screenshot 4 — Macie console**
Navigate to: **Macie Console → Summary**.
Capture the scanning status and findings count. Zero findings proves keyword data contains no PII matches; any findings prove Macie is actively scanning.
Save as `workshop/static/images/4.7-macie.png`.
