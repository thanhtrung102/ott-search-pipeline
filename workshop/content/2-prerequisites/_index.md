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
# Expected: {"UserId": "...", "Account": "703668403514", "Arn": "arn:aws:iam::703668403514:user/terraform-admin"}
```

**Verified output (2026-05-09):**
```json
{
    "UserId": "AIDA2HVPHXU5BNZGSFGWH",
    "Account": "703668403514",
    "Arn": "arn:aws:iam::703668403514:user/terraform-admin"
}
```

### 2. AWS CLI v2

```bash
aws --version
# Expected: aws-cli/2.x.x Python/3.x.x
```

**Verified output:** `aws-cli/2.34.16 Python/3.14.3 Windows/11 exe/AMD64`

### 3. Python 3.11

**Linux / macOS:**
```bash
python3 --version
# Expected: Python 3.11.x
```

**Windows (project venv):**
```powershell
.venv\Scripts\python.exe --version
# Expected: Python 3.11.x
```

**Verified output:** `Python 3.11.9`

{{% notice info %}}
`python3` is not aliased on Windows. Always invoke Python via `.venv\Scripts\python.exe` or `.venv\Scripts\activate` inside the project directory.
{{% /notice %}}

### 4. Node.js 18+

```bash
node --version
# Expected: v18.x.x, v20.x.x, or v22.x.x
```

**Verified output:** `v22.20.0`

### 5. AWS CDK v2

```bash
npm install -g aws-cdk
cdk --version
# Expected: 2.x.x (build xxxxxxx)
```

**Verified output:** `2.1121.0 (build 7abdd4e)`

{{% notice info %}}
**Windows:** After `npm install -g aws-cdk`, add `%APPDATA%\npm` to your PATH if `cdk` is not found:
```powershell
$env:PATH += ";$env:APPDATA\npm"
```
{{% /notice %}}

### 6. cfn-nag (Ruby gem — for Iteration 7 CI/CD)

```bash
gem install cfn-nag
cfn_nag_scan --version
# Expected: 0.x.x
```

**Verified output:** `0.8.10`

{{% notice warning %}}
**Windows:** `gem install cfn-nag` requires MSYS2 and the MINGW toolchain. If you see *"MSYS2 could not be found"*:

1. Install Ruby with DevKit: `winget install RubyInstallerTeam.Ruby.3.3`
2. Install MSYS2 to D: (C: must have ≥ 400 MB free): run the MSYS2 installer with `--root D:\msys64`
3. Set the path and install the toolchain:
```powershell
$env:MSYS2_INSTALL_PATH = "D:\msys64"
ridk install 3
gem install cfn-nag --no-document
```
{{% /notice %}}

### 7. Clone the repository and create venv

```bash
git clone https://github.com/thanhtrung102/ott-search-pipeline
cd ott-search-pipeline
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Windows equivalent:**
```powershell
git clone https://github.com/thanhtrung102/ott-search-pipeline
cd ott-search-pipeline
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

**Verify all stacks import cleanly:**
```powershell
python -c "
import stacks.network_stack, stacks.storage_stack, stacks.ingestion_stack
import stacks.etl_stack, stacks.compute_stack, stacks.analytics_stack
import stacks.governance_stack, stacks.observability_stack
print('All stacks OK')
"
# Expected: All stacks OK
```

**Verified output:** `All stacks OK` — `aws-cdk-lib 2.252.0`, `constructs 10.6.0`

### 8. CDK bootstrap

```bash
export CDK_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_ENV=demo
export ALERT_EMAIL=thanhtrungnsl2003@gmail.com
export BUCKET="ott-search-${CDK_ACCOUNT}-${CDK_ENV}"

cdk bootstrap aws://${CDK_ACCOUNT}/ap-southeast-1
# Expected: ✅  Environment aws://703668403514/ap-southeast-1 bootstrapped.
```

**Windows (PowerShell):**
```powershell
$env:CDK_ACCOUNT = aws sts get-caller-identity --query Account --output text
$env:CDK_ENV     = "demo"
$env:ALERT_EMAIL = "thanhtrungnsl2003@gmail.com"
$env:BUCKET      = "ott-search-$($env:CDK_ACCOUNT)-$($env:CDK_ENV)"

cdk bootstrap aws://$($env:CDK_ACCOUNT)/ap-southeast-1
```

**Verified:** Stack `CDKToolkit` — `CREATE_COMPLETE` in `ap-southeast-1`

### 9. Dataset upload

Upload the June 2022 `log_search` Parquet files to S3 **after** deploying OttStorage (Section 4.1).

The source directory contains 14 daily folders named `20220601/` through `20220614/`. Upload them preserving that naming exactly (do **not** rename to `dt=YYYY-MM-DD`):

```bash
for day in 01 02 03 04 05 06 07 08 09 10 11 12 13 14; do
  aws s3 cp "/path/to/log_search/202206${day}/" \
    "s3://${BUCKET}/raw-source/log_search/202206${day}/" \
    --recursive --exclude ".*" --exclude "_SUCCESS"
done
```

**Verify (Linux/macOS):**
```bash
aws s3 ls "s3://${BUCKET}/raw-source/log_search/" | grep PRE | wc -l
# Expected: 14
```

**Verify (Windows PowerShell):**
```powershell
(aws s3 ls "s3://$($env:BUCKET)/raw-source/log_search/") -match "PRE" | Measure-Object | Select-Object -ExpandProperty Count
# Expected: 14
```

**Verified output:** `14` date partitions in `s3://ott-search-703668403514-demo/raw-source/log_search/` — `20220601/` through `20220614/`

### 10. QuickSight

QuickSight Standard cannot be provisioned by CDK.

**Console setup (one-time):**
1. AWS Console → **QuickSight** → **Sign up for QuickSight**
2. Select **Standard** (~$9/month — cancel after demo)
3. Region: **ap-southeast-1**
4. Allow access to **Amazon Athena** and bucket `ott-search-{account}-demo`

**Grant S3 access to the OTT bucket** — update the `AWSQuickSightS3Policy` on role `aws-quicksight-service-role-v0` to include `ott-search-{account}-demo` (list, get, put permissions).

**Create the Athena data source via CLI:**
```bash
aws quicksight create-data-source --region ap-southeast-1 \
  --cli-input-json file://qs-datasource.json
```

Where `qs-datasource.json` contains:
```json
{
  "AwsAccountId": "703668403514",
  "DataSourceId": "ott-athena-demo",
  "Name": "OTT Athena Demo",
  "Type": "ATHENA",
  "DataSourceParameters": {
    "AthenaParameters": { "WorkGroup": "ott-analytics-demo" }
  }
}
```

**Verify:**
```bash
aws quicksight describe-data-source --region ap-southeast-1 \
  --aws-account-id 703668403514 --data-source-id ott-athena-demo \
  --query "DataSource.Status" --output text
# Expected: CREATION_SUCCESSFUL
```

**Verified output:** `CREATION_SUCCESSFUL` — workgroup `ott-analytics-demo`

### 11. SNS email confirmation

After `cdk deploy OttCompute-demo`, AWS sends subscription confirmation emails to `ALERT_EMAIL`. Click the link in **both** emails:
- `ott-pipeline-ops-demo` → `thanhtrungnsl2003@gmail.com`
- `ott-search-anomaly-alerts-demo` → `thanhtrungnsl2003@gmail.com`

**Verify:**
```bash
aws sns list-subscriptions-by-topic --region ap-southeast-1 \
  --topic-arn "arn:aws:sns:ap-southeast-1:${CDK_ACCOUNT}:ott-search-anomaly-alerts-demo" \
  --query "Subscriptions[*].{Endpoint:Endpoint,Status:SubscriptionArn}" \
  --output table
# Expected: SubscriptionArn contains a full ARN (not "PendingConfirmation")
```

**Verified output (2026-05-09):**
- `ott-pipeline-ops-demo` — 1 confirmed subscription: `thanhtrungnsl2003@gmail.com`
- `ott-search-anomaly-alerts-demo` — 2 confirmed subscriptions: `thanhtrungnsl2003@gmail.com`, `tranlethanhtrung21@gmail.com`

No `PendingConfirmation` entries on either topic.

---

## Shell variables reference

Set these in your shell before starting Section 4. Every `cdk deploy` command uses them:

**Linux / macOS:**
```bash
export CDK_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_ENV=demo
export ALERT_EMAIL=thanhtrungnsl2003@gmail.com
export BUCKET="ott-search-${CDK_ACCOUNT}-${CDK_ENV}"
```

**Windows (PowerShell):**
```powershell
$env:CDK_ACCOUNT = aws sts get-caller-identity --query Account --output text
$env:CDK_ENV     = "demo"
$env:ALERT_EMAIL = "thanhtrungnsl2003@gmail.com"
$env:BUCKET      = "ott-search-$($env:CDK_ACCOUNT)-$($env:CDK_ENV)"
```

| Variable | Verified value | Purpose |
|---|---|---|
| `CDK_ENV` | `demo` | Appended to all resource names |
| `CDK_ACCOUNT` | `703668403514` | CDK environment target |
| `ALERT_EMAIL` | `thanhtrungnsl2003@gmail.com` | SNS anomaly alert destination |
| `BUCKET` | `ott-search-703668403514-demo` | S3 bucket for all data |
| `gold_date_filter` | `dt >= '2022-06-01'` | Athena CTAS date scope (historical demo) |

{{% notice warning %}}
The `gold_date_filter` override is **required** when running the historical June 2022 demo. Without it, the Athena CTAS uses the production default (`dt >= date_add('day', -1, current_date)`) and produces an empty gold table.
{{% /notice %}}
