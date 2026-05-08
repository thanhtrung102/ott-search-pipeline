---
title: "4.8 CI/CD + Observability (Iteration 7)"
weight: 48
pre: "<b>4.8 </b>"
---

## What this deploys

**OttObservability:**
- CloudWatch custom metric namespace `OTT/SearchPipeline`
- 4 CloudWatch alarms + 1 composite alarm `pipeline-health-dev`
- CloudWatch dashboard `ott-pipeline-health-dev`

**OttPipeline:**
- CodePipeline CI/CD with 6 stages (Source → Build → Deploy_Dev → Integration_Test → Manual_Approval → Deploy_Prod)
- CodeBuild build stage: `pip install`, `cdk synth`, `cfn_nag_scan`
- cfn-nag security gate — pipeline fails if any CloudFormation template has violations

---

## Deploy

```bash
cdk deploy OttObservability-${CDK_ENV} OttPipeline-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context region=ap-southeast-1 \
  --context alert_email=${ALERT_EMAIL} \
  --require-approval never
```

**Deploy time:** approximately 10 minutes. CodePipeline requires a one-time CodeStar Connection setup (see below).

---

## CodeStar Connection (one-time manual step)

```bash
# After deploy, the Source stage will show "Failed - Action execution failed"
# until the connection is authorized. Do this once:
```

1. **Developer Tools Console → Connections → Find `ott-github-connection`**
2. Status shows **Pending** — click **Update pending connection**
3. **Connect to GitHub** → authorize the app → **Connect**
4. Status changes to **Available**

Then update `cdk.json`:
```json
{
  "context": {
    "github_connection_arn": "arn:aws:codestar-connections:ap-southeast-1:{account}:connection/xxxxxxxx",
    "github_owner": "thanhtrung102",
    "github_repo": "ott-search-pipeline"
  }
}
```

Re-deploy OttPipeline to pick up the connection ARN:
```bash
cdk deploy OttPipeline-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --require-approval never
```

---

## CloudWatch metrics and alarms

| Metric | Published by | What it measures |
|---|---|---|
| `SearchEnterEventsPerMinute` | anomaly-detector | Enter events per minute by genre |
| `SearchQuitEventsPerMinute` | anomaly-detector | Quit events per minute by genre |
| `AnomalyScore` | anomaly-detector | Z-score per (genre, hour) slot |
| `GlueJobDurationSeconds` | Glue job metrics | ETL execution time |
| `DailyPipelineSuccess` | Step Functions PipelineSuccess state | 1 = pipeline succeeded today |

| Alarm | Threshold | Meaning |
|---|---|---|
| `ott-lambda-error-rate-dev` | Lambda errors > 5 in 5 min | Lambda function failing |
| `ott-kinesis-iterator-age-dev` | Iterator age > threshold in 5 min | Kinesis consumer falling behind |
| `ott-glue-job-failure-dev` | DailyPipelineSuccess < 1 in 26h | Nightly pipeline did not complete |
| `ott-anomaly-score-breach-dev` | AnomalyScore > 3.0 | Z-score exceeded threshold |
| **`pipeline-health-dev`** | **Composite alarm** | OR of all 4 above |

---

## Proof: DailyPipelineSuccess metric

After the Step Functions run (Section 4.5), verify the metric was published:

```bash
aws cloudwatch get-metric-statistics \
  --namespace OTT/SearchPipeline \
  --metric-name DailyPipelineSuccess \
  --start-time $(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 86400 \
  --statistics Sum \
  --query 'Datapoints[0].Sum' \
  --output text
```

**Verified output (2026-05-08):** `1.0`

---

## Trigger and verify the CI/CD pipeline

```bash
# Make a small change to trigger the pipeline
git add stacks/compute_stack.py
git commit -m "doc: note z-score threshold rationale"
git push origin main
```

Monitor:
```bash
aws codepipeline get-pipeline-state \
  --name ott-search-pipeline-${CDK_ENV} \
  --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" \
  --output table
```

Expected (after full run):
```
--------------------------------------
|        GetPipelineState            |
+-----------------------+------------+
|  Stage                |  Status    |
+-----------------------+------------+
|  Source               |  Succeeded |
|  Build                |  Succeeded |
|  Deploy_Dev           |  Succeeded |
|  Integration_Test     |  Succeeded |
|  Manual_Approval      |  InProgress|
|  Deploy_Prod          |  (pending) |
+-----------------------+------------+
```

cfn-nag output in Build stage logs (look for):
```
Running cfn_nag_scan...
Analyzing CloudFormation template cdk.out/OttNetwork-dev.template.json
------------------------------------------------------------
0 suppressed violations
0 violations found
```

---

## Composite alarm demonstration

```bash
# Force the composite alarm into ALARM state
aws cloudwatch set-alarm-state \
  --alarm-name ott-lambda-error-rate-${CDK_ENV} \
  --state-value ALARM \
  --state-reason "Manual test for workshop demo"
```

Navigate to: **CloudWatch → Alarms → `pipeline-health-dev`** — composite alarm turns red.

```bash
# Reset after capturing screenshot
aws cloudwatch set-alarm-state \
  --alarm-name ott-lambda-error-rate-${CDK_ENV} \
  --state-value OK \
  --state-reason "Resetting after demo"
```

---

## Resources deployed

### OttObservability

| Resource | Name | Configuration |
|---|---|---|
| CloudWatch Dashboard | `ott-pipeline-health-dev` | 5 metric widgets |
| CloudWatch Alarm | `ott-lambda-error-rate-dev` | Errors > 5 in 5 min |
| CloudWatch Alarm | `ott-kinesis-iterator-age-dev` | Iterator age threshold |
| CloudWatch Alarm | `ott-glue-job-failure-dev` | DailyPipelineSuccess < 1 in 26h window |
| CloudWatch Alarm | `ott-anomaly-score-breach-dev` | AnomalyScore > 3.0 |
| Composite Alarm | `pipeline-health-dev` | OR of all 4 child alarms |

### OttPipeline

| Stage | Tool | Action |
|---|---|---|
| Source | CodeStar Connection | GitHub push → artifact |
| Build | CodeBuild | `cdk synth` + `cfn_nag_scan` (fails on violations) |
| Deploy_Dev | CloudFormation | `cdk deploy --all` to dev |
| Integration_Test | CodeBuild | `pytest tests/integration/` |
| Manual_Approval | SNS | Gate before production deploy |
| Deploy_Prod | CloudFormation | `cdk deploy --all` to prod |

---

## Screenshot guidance

**Screenshot 1 — CodePipeline all stages**
Navigate to: **CodePipeline Console → `ott-search-pipeline-dev`**.
Capture all 6 stages with Source, Build, Deploy_Dev, Integration_Test showing Succeeded.
Save as `workshop/static/images/4.8-codepipeline.png`.

**Screenshot 2 — cfn-nag output in CodeBuild logs**
Click the Build stage → View logs. Find the `cfn_nag_scan` section.
Capture the `0 violations found` output with at least one template filename visible.
Save as `workshop/static/images/4.8-cfnnag.png`.

**Screenshot 3 — CloudWatch dashboard**
Navigate to: **CloudWatch → Dashboards → `ott-pipeline-health-dev`** → Last 24 hours.
All 5 metric widgets must show data (not empty). AnomalyScore widget should show the THE_THAO spike.
Save as `workshop/static/images/4.8-cw-dashboard.png`.

**Screenshot 4 — Composite alarm ALARM then OK**
Capture the composite alarm in red after `set-alarm-state ALARM`, then green after reset.
Save as `workshop/static/images/4.8-alarm-firing.png` and `4.8-alarm-ok.png`.
