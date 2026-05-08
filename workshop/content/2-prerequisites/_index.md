---
title: "2. Prerequisites"
weight: 20
chapter: true
pre: "<b>2. </b>"
---

# Prerequisites

Complete every item in this checklist before starting Section 4. The workshop commands will fail silently if a dependency is missing.

---

## Checklist

### 1. AWS Account with AdministratorAccess

Required for initial CDK bootstrap only. After bootstrap the pipeline uses least-privilege IAM roles.

Verify your identity:
```bash
aws sts get-caller-identity
```
Expected: a JSON object with your `Account`, `UserId`, and `Arn`. If this returns an error, run `aws configure` first.

---

### 2. AWS CLI v2

```bash
# Install (Linux/macOS)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# Verify
aws --version
# Expected: aws-cli/2.x.x Python/3.x.x ...
```

---

### 3. Python 3.11

```bash
python3 --version
# Expected: Python 3.11.x
```

If you have `pyenv`: `pyenv install 3.11.9 && pyenv local 3.11.9`

---

### 4. Node.js 18+

Required by the AWS CDK CLI.

```bash
node --version
# Expected: v18.x.x or v20.x.x
```

---

### 5. AWS CDK v2

```bash
npm install -g aws-cdk
cdk --version
# Expected: 2.x.x (build xxxxxxx)
```

---

### 6. Git

```bash
git --version
# Expected: git version 2.x.x
```

---

### 7. cfn-nag (security static analysis)

Used in the CI/CD pipeline (Iteration 7) to catch CloudFormation security misconfigurations before deploy.

```bash
# Requires Ruby
gem install cfn-nag

cfn_nag_scan --version
# Expected: cfn-nag x.x.x
```

---

### 8. Clone the repository

```bash
git clone https://github.com/{YOUR_USERNAME}/ott-search-pipeline
cd ott-search-pipeline

python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

### 9. Dataset: log_search Parquet files

The source data is 14 daily folders of `log_search` Parquet files from June 2022.

Upload them to the source prefix in your S3 bucket. The files need to be in `dt=YYYY-MM-DD` folder format:

```bash
# After StorageStack is deployed (Section 4.1), run:
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ott-search-${ACCOUNT}-dev"

for dir in /path/to/log_search/2022*/; do
  raw_date=$(basename "$dir")        # e.g. 20220601
  dt="dt=${raw_date:0:4}-${raw_date:4:2}-${raw_date:6:2}"   # dt=2022-06-01
  aws s3 cp "$dir" "s3://${BUCKET}/raw-source/log_search/${dt}/" \
    --recursive --exclude ".*" --exclude "_SUCCESS"
done
```

Verify the upload:
```bash
aws s3 ls "s3://${BUCKET}/raw-source/log_search/" | grep PRE | wc -l
# Expected: 14
```

---

### 10. QuickSight account (for Iteration 5 only)

QuickSight cannot be activated via CDK. This is a one-time manual step:

1. Open the AWS Console → search **QuickSight**
2. Click **Sign up for QuickSight**
3. Select **Standard** (sufficient for this workshop — ~$9/month, cancel after demo)
4. Region: **ap-southeast-1**
5. Account name: `ott-search-analytics` (or any name you choose)
6. Notification email: your email address
7. Check **Amazon Athena** under S3 access settings

---

### 11. Email address for SNS anomaly alerts

You will pass this as a CDK context variable. Alerts are sent here when the anomaly detector fires.

Note it now — you will use it in every `cdk deploy` command:
```
--context alert_email=your@email.com
```

---

## Context variables reference

All `cdk deploy` commands in this workshop use these context variables. Set them in your shell or pass them directly:

| Variable | Example value | Where used |
|---|---|---|
| `env` | `dev` | All stack names |
| `account` | `123456789012` | CDK environment |
| `region` | `ap-southeast-1` | CDK environment |
| `alert_email` | `your@email.com` | SNS subscriptions |
| `gold_date_filter` | `dt >= '2022-06-01'` | Athena CTAS WHERE clause (demo only) |

The default `gold_date_filter` is `dt >= date_add('day', -1, current_date)` — for the historical demo dataset, override it to cover all June 2022 data.
