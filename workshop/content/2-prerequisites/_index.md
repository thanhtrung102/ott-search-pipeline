---
title: "2. Prerequisites"
weight: 20
chapter: true
pre: "<b>2. </b>"
---

# Prerequisites

Complete every item before starting Section 4. Commands will fail silently if a dependency is missing.

---

## Checklist

### 1. AWS Account — AdministratorAccess

Required for CDK bootstrap only. After bootstrap the pipeline uses least-privilege IAM roles.

```bash
aws sts get-caller-identity
# Expected: {"UserId": "...", "Account": "123456789012", "Arn": "arn:aws:iam::..."}
```

### 2. AWS CLI v2

```bash
aws --version
# Expected: aws-cli/2.x.x Python/3.x.x
```

### 3. Python 3.11

```bash
python3 --version
# Expected: Python 3.11.x
```

### 4. Node.js 18+

```bash
node --version
# Expected: v18.x.x or v20.x.x
```

### 5. AWS CDK v2

```bash
npm install -g aws-cdk
cdk --version
# Expected: 2.x.x (build xxxxxxx)
```

### 6. cfn-nag (Ruby gem — for Iteration 7 CI/CD)

```bash
gem install cfn-nag
cfn_nag_scan --version
# Expected: cfn-nag x.x.x
```

### 7. Clone the repository and create venv

```bash
git clone https://github.com/thanhtrung102/ott-search-pipeline
cd ott-search-pipeline
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 8. CDK bootstrap

```bash
export CDK_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_ENV=dev
export ALERT_EMAIL=your@email.com
export BUCKET="ott-search-${CDK_ACCOUNT}-${CDK_ENV}"

cdk bootstrap aws://${CDK_ACCOUNT}/ap-southeast-1
# Expected: ✅  Environment aws://123456789012/ap-southeast-1 bootstrapped.
```

### 9. Dataset upload

Upload the June 2022 `log_search` Parquet files to S3 **after** deploying OttStorage (Section 4.1):

```bash
for dir in /path/to/log_search/2022*/; do
  raw_date=$(basename "$dir")          # e.g. 20220601
  dt="dt=${raw_date:0:4}-${raw_date:4:2}-${raw_date:6:2}"  # dt=2022-06-01
  aws s3 cp "$dir" "s3://${BUCKET}/raw-source/log_search/${dt}/" \
    --recursive --exclude ".*" --exclude "_SUCCESS"
done

# Verify
aws s3 ls "s3://${BUCKET}/raw-source/log_search/" | grep PRE | wc -l
# Expected: 14
```

### 10. QuickSight (for Iteration 5 only — manual console step)

QuickSight Standard cannot be provisioned by CDK.

1. AWS Console → **QuickSight** → **Sign up for QuickSight**
2. Select **Standard** (~$9/month — cancel after demo)
3. Region: **ap-southeast-1**
4. Allow access to **Amazon Athena** and bucket `ott-search-{account}-dev`

### 11. SNS email confirmation

After `cdk deploy OttCompute-dev`, AWS sends a subscription confirmation email to `ALERT_EMAIL`. Click the link to confirm — alerts will not arrive until confirmed.

---

## Shell variables reference

Set these in your shell before starting Section 4. Every `cdk deploy` command uses them:

```bash
export CDK_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_ENV=dev
export ALERT_EMAIL=your@email.com
export BUCKET="ott-search-${CDK_ACCOUNT}-${CDK_ENV}"
```

| Variable | Example | Purpose |
|---|---|---|
| `CDK_ENV` | `dev` | Appended to all resource names |
| `CDK_ACCOUNT` | `123456789012` | CDK environment target |
| `ALERT_EMAIL` | `you@example.com` | SNS anomaly alert destination |
| `BUCKET` | `ott-search-123456789012-dev` | S3 bucket for all data |
| `gold_date_filter` | `dt >= '2022-06-01'` | Athena CTAS date scope (historical demo) |

{{% notice warning %}}
The `gold_date_filter` override is **required** when running the historical June 2022 demo. Without it, the Athena CTAS uses the production default (`dt >= date_add('day', -1, current_date)`) and produces an empty gold table.
{{% /notice %}}
