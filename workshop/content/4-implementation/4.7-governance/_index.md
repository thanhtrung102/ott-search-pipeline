---
title: "4.7 Governance + Security (Iteration 6)"
weight: 47
pre: "<b>4.7 </b>"
---

## Context

Lake Formation enforces table- and column-level access control independently of S3 bucket policies. A principal with S3 `s3:GetObject` permission but no Lake Formation grant **cannot** query a table through Athena — Lake Formation intercepts the query before it reaches S3. This is demonstrated live: a Marketing role that can query keyword trends cannot see individual user data.

Three access personas are deployed:

| Role | Access |
|---|---|
| `ott-data-engineering-dev` | ALL permissions on all three databases (raw, curated, gold) |
| `ott-analyst-dev` | SELECT + DESCRIBE on curated and gold — all columns |
| `ott-marketing-dev` | DESCRIBE on gold database, SELECT on `keyword_trends` — **5 columns excluded** |

Excluded columns for `ott-marketing-dev`:
- `unique_users` — could enable re-identification via cross-join
- `authenticated_rate` — reveals unauthenticated session ratio (operationally sensitive)
- `repeat_search_rate` — individual behavior pattern
- `premium_search_rate` — subscription revenue data (Finance team only)
- `rank_7d_ago` — requires understanding of baseline methodology

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
This stack disables the default IAM passthrough for the Glue Data Catalog (`CreateDatabaseDefaultPermissions` and `CreateTableDefaultPermissions` are set to empty). After this stack deploys, **all Athena queries must be made by a principal with an explicit Lake Formation grant**. The CDK execution role (`cdk-hnb659fds-cfn-exec-role-{account}-{region}`) is added as a Lake Formation admin to prevent CloudFormation deployments from breaking.
{{% /notice %}}

**Deploy time:** approximately 5 minutes (Custom Resource for Macie classification job takes extra time).

---

## What was deployed

| Resource | Configuration |
|---|---|
| IAM role `ott-data-engineering-dev` | LF admin, ALL on raw/curated/gold |
| IAM role `ott-analyst-dev` | SELECT+DESCRIBE on curated+gold |
| IAM role `ott-marketing-dev` | SELECT on gold.keyword_trends (11 of 16 columns) |
| Lake Formation settings | Default IAM passthrough disabled |
| Amazon Macie session | Enabled, findings every 15 min |
| Macie custom identifier | `(0|\+84)[0-9]{9}` — Vietnamese phone numbers |
| Macie classification job | Weekly (Monday), scopes `raw/events/` prefix |

---

## Governance validation

Assume the Marketing role and run two queries — one that must fail, one that must succeed.

### Step 1 — Assume the Marketing role

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN < <(
  aws sts assume-role \
    --role-arn "arn:aws:iam::${ACCOUNT}:role/ott-marketing-dev" \
    --role-session-name "governance-demo" \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
    --output text
)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

echo "Now acting as: $(aws sts get-caller-identity --query Arn --output text)"
```

---

### Step 2 — Attempt 1: Query curated layer (MUST FAIL)

```bash
ACCOUNT_ORIG="YOUR_ACCOUNT_ID"   # set this before assuming the role
BUCKET="ott-search-${ACCOUNT_ORIG}-dev"

QID=$(aws athena start-query-execution \
  --query-string "SELECT user_id_hashed FROM ott_search_curated.search_enriched LIMIT 1" \
  --work-group ott-analytics-dev \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/marketing-test/" \
  --query QueryExecutionId --output text)

sleep 5

aws athena get-query-execution \
  --query-execution-id ${QID} \
  --query 'QueryExecution.Status' \
  --output json
```

Expected response — the query fails with an S3 access denial (the marketing role's IAM policy restricts S3 access to the `gold/` prefix):
```
PERMISSION_DENIED: User: arn:aws:sts::{account}:assumed-role/ott-marketing-dev/...
is not authorized to perform: s3:GetObject on resource:
"arn:aws:s3:::ott-search-{account}-dev/curated/search_enriched/..."
because no identity-based policy allows the s3:GetObject action
```

{{% notice note %}}
The `ott-marketing-dev` role's IAM policy explicitly restricts S3 access to the `gold/` prefix and `athena-results/` — it has no `s3:GetObject` on `curated/`. Enforcement is layered: IAM blocks S3 access to the curated layer; Lake Formation additionally limits which columns the role sees in the gold layer. The Marketing analyst cannot reach user-level data by either path.
{{% /notice %}}

---

### Step 3 — Attempt 2: Query gold layer (MUST SUCCEED)

```bash
QID=$(aws athena start-query-execution \
  --query-string "SELECT keyword_norm, abandonment_rate, rank_delta 
                  FROM ott_search_gold.keyword_trends 
                  WHERE derived_genre = 'THE_THAO'
                  ORDER BY search_count DESC 
                  LIMIT 5" \
  --work-group ott-analytics-dev \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/marketing-test/" \
  --query QueryExecutionId --output text)

sleep 10

aws athena get-query-results \
  --query-execution-id ${QID} \
  --query "ResultSet.Rows[*].Data[*].VarCharValue" \
  --output json
```

Expected: 5 rows with keyword_norm, abandonment_rate, and rank_delta values (no error).

---

### Step 4 — Restore your original credentials

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
aws sts get-caller-identity  # should show your original identity
```

---

## Screenshot 1 — Lake Formation permissions table

Navigate to: **Lake Formation Console → Data permissions**

Filter by principal `ott-marketing-dev`. The table should show:
- Database: `ott_search_gold` → `DESCRIBE`
- Table: `ott_search_gold.keyword_trends` → `SELECT` (all tables wildcard)

{{% notice tip %}}
**📸 Screenshot 1:** Navigate to **Lake Formation Console → Data permissions**. Filter by principal `ott-marketing-dev`. Capture the permissions table showing the `ott_search_gold` database `DESCRIBE` grant and the `keyword_trends` table `SELECT` grant. Save as `workshop/static/images/4.7-lf-permissions.png`.
{{% /notice %}}

---

## Screenshot 2 — Failed Athena query (governance proof)

Navigate to: **Athena Console → Recent queries**

Find the failed query (`SELECT user_id_hashed FROM ott_search_curated.search_enriched`). Click on it to expand.

{{% notice tip %}}
**📸 Screenshot 2:** Capture the query detail pane showing **State: FAILED** and the full error text showing `s3:GetObject` denied on the `curated/` prefix. The error must reference the marketing role ARN and the curated S3 path — this is the governance proof. Save as `workshop/static/images/4.7-access-denied.png`.
{{% /notice %}}

---

## Screenshot 3 — Successful Athena query on gold layer

Navigate to: **Athena Console → Recent queries**

Find the successful query (`SELECT keyword_norm, abandonment_rate, rank_delta FROM ott_search_gold.keyword_trends`). Click to see results.

{{% notice tip %}}
**📸 Screenshot 3:** Capture the results tab showing 5 rows of `keyword_norm`, `abandonment_rate`, `rank_delta` values. The same Marketing role that triggered a Lake Formation denial in Screenshot 2 is producing this result. Save as `workshop/static/images/4.7-gold-access.png`.
{{% /notice %}}

Caption: "The same Marketing role that cannot access any row from the curated layer successfully queries keyword trends from the gold layer. Lake Formation enforces this boundary at the query execution layer — not at the S3 bucket policy level. There is no workaround via direct S3 access."

---

## Screenshot 4 — Macie findings dashboard

Navigate to: **Amazon Macie Console → Summary**

The dashboard shows the scanning status and any findings from the weekly classification job.

{{% notice tip %}}
**📸 Screenshot 4:** Capture the Macie Summary page. If 0 findings: shows the pipeline keyword data contains no PII. If findings exist: even better — proves Macie is actively scanning. Save as `workshop/static/images/4.7-macie.png`.
{{% /notice %}}

{{% notice info %}}
The `keyword_norm` field in the raw data is free-text search input and may occasionally contain phone numbers if users search by contact number. The Macie custom identifier `(0|\+84)[0-9]{9}` scans for Vietnamese phone patterns in `raw/events/` weekly. Any findings appear in the Macie console and trigger a Macie alert.
{{% /notice %}}
