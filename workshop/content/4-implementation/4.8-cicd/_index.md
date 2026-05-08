---
title: "4.8 CI/CD + Observability (Iteration 7)"
weight: 48
pre: "<b>4.8 </b>"
---

## Context

The observability stack adds CloudWatch custom metrics, alarms, and a composite health alarm. The pipeline stack adds a CodePipeline CI/CD pipeline that runs cfn-nag security static analysis on every git push before deploying to the dev environment.

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

**Deploy time:** approximately 10 minutes (CodePipeline requires CodeStar connection setup — see Step 1 below).

---

## CodePipeline setup (one manual step)

CodePipeline requires a CodeStar Connection to authorize GitHub access. This is a one-time console step:

1. Open **Developer Tools → Connections → Create connection**
2. Provider: **GitHub**
3. Connection name: `ott-github-connection`
4. Click **Connect to GitHub** → authorize the app
5. Copy the connection ARN

Update `cdk.json` with your connection ARN and GitHub repo:
```json
{
  "context": {
    "github_connection_arn": "arn:aws:codestar-connections:ap-southeast-1:{account}:connection/xxxxxxxx",
    "github_owner": "your-username",
    "github_repo": "ott-search-pipeline"
  }
}
```

Re-deploy:
```bash
cdk deploy OttPipeline-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --require-approval never
```

---

## What was deployed

### Observability stack

| Resource | Name | Configuration |
|---|---|---|
| CloudWatch Dashboard | `ott-pipeline-health-dev` | Custom metric widgets |
| CloudWatch Alarm | `ott-lambda-error-rate-dev` | Lambda errors > 5 in 5 min |
| CloudWatch Alarm | `ott-kinesis-iterator-age-dev` | Kinesis iterator age > threshold in 5 min |
| CloudWatch Alarm | `ott-glue-job-failure-dev` | DailyPipelineSuccess < 1 in 26h |
| CloudWatch Alarm | `ott-anomaly-score-breach-dev` | AnomalyScore > 3.0 (z-score threshold) |

**Custom metric namespace:** `OTT/SearchPipeline`

| Metric | Published by | Meaning |
|---|---|---|
| `SearchEnterEventsPerMinute` | anomaly-detector | Rate of enter events by genre |
| `SearchQuitEventsPerMinute` | anomaly-detector | Rate of quit events by genre |
| `AnomalyScore` | anomaly-detector | Z-score for (genre, hour) slot |
| `GlueJobDurationSeconds` | Glue job metrics | ETL job execution time |
| `DailyPipelineSuccess` | Step Functions | 1 = succeeded, 0 = not yet run |

### CI/CD pipeline

| Stage | Tool | Action |
|---|---|---|
| Source | CodeStar Connection | GitHub push → artifact |
| Build | CodeBuild | `pip install -r requirements.txt`, `cdk synth`, `cfn_nag_scan` |
| Deploy_Dev | CloudFormation | `cdk deploy --all` to dev |
| Integration_Test | CodeBuild | Runs `pytest tests/integration/` |
| Manual_Approval | SNS | Gate before production deploy |
| Deploy_Prod | CloudFormation | `cdk deploy --all` to prod |

The Build stage fails the entire pipeline if cfn-nag finds any security violations — this prevents insecure configurations from reaching the dev environment.

---

## Trigger the CI/CD pipeline

Make a small change to trigger the pipeline:

```bash
# Example: update anomaly threshold comment in compute_stack.py
git add stacks/compute_stack.py
git commit -m "doc: note z-score threshold rationale"
git push origin main
```

Monitor the pipeline:
```bash
aws codepipeline get-pipeline-state \
  --name ott-search-pipeline-dev \
  --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" \
  --output table
```

---

## Screenshot 1 — CodePipeline all-green

Navigate to: **CodePipeline Console → Pipelines → `ott-search-pipeline-dev`**

All four stages should be green (Succeeded): Source → Build → Deploy → Integration-test.

{{% notice tip %}}
**📸 Screenshot 1:** Navigate to **CodePipeline Console → Pipelines → `ott-search-pipeline-dev`**. All six stage boxes (Source, Build, Deploy_Dev, Integration_Test, Manual_Approval, Deploy_Prod) must show green **Succeeded** status. The Source stage should show the latest commit SHA. Capture the full pipeline view. Save as `workshop/static/images/4.8-codepipeline.png`.
{{% /notice %}}

---

## Screenshot 2 — cfn-nag output in CodeBuild logs

Navigate to: **CodePipeline → Build stage → [latest execution] → View logs**

Scroll to the cfn-nag section:

{{% notice tip %}}
**📸 Screenshot 2:** Find the log lines showing `cfn_nag_scan` running and the `0 violations found` result. The text must include at least one template name (e.g., `OttStorage-dev.template.json`) and the final `0 violations` count. Save as `workshop/static/images/4.8-cfnnag.png`.
{{% /notice %}}

Expected log output:
```
Running cfn_nag_scan...
Analyzing CloudFormation template cdk.out/OttNetwork-dev.template.json
------------------------------------------------------------
0 suppressed violations

0 violations found
------------------------------------------------------------
Analyzing CloudFormation template cdk.out/OttStorage-dev.template.json
...
Build completed successfully
```

---

## Screenshot 3 — CloudWatch dashboard

Navigate to: **CloudWatch Console → Dashboards → `ott-pipeline-health-dev`**

All metric widgets should show data:
- **SearchEnterEventsPerMinute** — activity during replay (high) and idle (zero)
- **SearchQuitEventsPerMinute** — quit events by genre
- **AnomalyScore** — z-score time series; THE_THAO spike visible if injection was run
- **GlueJobDurationSeconds** — horizontal bar at ~420 seconds per run
- **DailyPipelineSuccess** — 1 for each successful execution

{{% notice tip %}}
**📸 Screenshot 3:** Navigate to **CloudWatch Console → Dashboards → `ott-pipeline-health-dev`**. Set time range to **Last 24 hours**. All metric widgets must show data (not empty). The `AnomalyScore` widget should show a spike if you ran the injection demo. Save as `workshop/static/images/4.8-cw-dashboard.png`.
{{% /notice %}}

---

## Screenshot 4 — Composite alarm in ALARM then OK

Navigate to: **CloudWatch → Alarms → `pipeline-health-dev`**

To demonstrate the composite alarm, manually trigger a child alarm:

```bash
# Force lambda-error-rate alarm into ALARM state
aws cloudwatch set-alarm-state \
  --alarm-name ott-lambda-error-rate-dev \
  --state-value ALARM \
  --state-reason "Manual test for workshop demo"
```

{{% notice tip %}}
**📸 Screenshot 4a:** Immediately after running `set-alarm-state --state-value ALARM`, navigate to **CloudWatch → Alarms → `pipeline-health-dev`**. Capture the composite alarm in red **In alarm** state with the child alarm reason visible. Save as `workshop/static/images/4.8-alarm-firing.png`.
{{% /notice %}}

Wait 2 minutes, then reset:
```bash
aws cloudwatch set-alarm-state \
  --alarm-name ott-lambda-error-rate-dev \
  --state-value OK \
  --state-reason "Resetting after demo"
```

{{% notice tip %}}
**📸 Screenshot 4b:** After resetting, refresh the alarm page. Capture the composite alarm returning to green **OK** state. Having both 4a and 4b proves the alarm is wired correctly — it fires and clears. Save as `workshop/static/images/4.8-alarm-ok.png`.
{{% /notice %}}

Caption: "The composite alarm aggregates four individual alarms into a single operational health signal. An on-call engineer watching this dashboard can see at a glance whether the pipeline is healthy without checking four separate alarms."
