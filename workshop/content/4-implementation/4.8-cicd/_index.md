---
title: "4.8 CI/CD + Observability (Iteration 7)"
weight: 48
pre: "<b>4.8 </b>"
---

## What this deploys

**OttObservability:**
- CloudWatch custom metric namespace `OTT/SearchPipeline`
- 4 CloudWatch alarms + 1 composite alarm `ott-pipeline-health-demo`
- CloudWatch dashboard `ott-pipeline-health-demo`

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

**Deploy time:** approximately 10 minutes. After deploy, push code to the CodeCommit remote (see Source configuration below) to activate the pipeline.

---

## Source configuration

The pipeline stack selects the source provider based on CDK context:

- **With** `github_owner` + `github_repo` + `github_connection` context → GitHub via CodeStar Connection
- **Without** those context keys (default) → CodeCommit fallback (repo auto-created as `ott-search-pipeline-{env}`)

The demo deployment uses **CodeCommit** (no GitHub context provided).

### Push code to CodeCommit to activate the pipeline

```bash
# Add the CodeCommit remote (one-time)
git remote add codecommit \
  https://git-codecommit.ap-southeast-1.amazonaws.com/v1/repos/ott-search-pipeline-${CDK_ENV}

# Configure git to use AWS CLI credential helper
git config credential.helper '!aws codecommit credential-helper $@'
git config credential.UseHttpPath true

# Push — this triggers the Source stage immediately
GIT_ASKPASS="" GIT_TERMINAL_PROMPT=0 git push codecommit main
```

### Optional: use GitHub instead

To switch to GitHub, create a CodeStar Connection in the console, then re-deploy with the extra context:
```bash
cdk deploy OttPipeline-${CDK_ENV} \
  --context env=${CDK_ENV} \
  --context account=${CDK_ACCOUNT} \
  --context github_owner=<your-github-username> \
  --context github_repo=ott-search-pipeline \
  --context github_connection=arn:aws:codestar-connections:ap-southeast-1:{account}:connection/xxxxxxxx \
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
| `ott-lambda-error-rate-demo` | Lambda errors > 5 in 5 min | Lambda function failing |
| `ott-kinesis-iterator-age-demo` | Iterator age > threshold in 5 min | Kinesis consumer falling behind |
| `ott-glue-job-failure-demo` | DailyPipelineSuccess < 1 in 26h | Nightly pipeline did not complete |
| `ott-anomaly-score-breach-demo` | AnomalyScore > 3.0 | Z-score exceeded threshold |
| **`ott-pipeline-health-demo`** | **Composite alarm** | OR of all 4 above |

---

## Proof: DailyPipelineSuccess metric

After the Step Functions run (Section 4.5), verify the metric was published:

```bash
aws cloudwatch get-metric-statistics \
  --namespace OTT/SearchPipeline \
  --metric-name DailyPipelineSuccess \
  --dimensions Name=Environment,Value=${CDK_ENV} \
  --start-time $(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 86400 \
  --statistics Sum \
  --query 'Datapoints[0].Sum' \
  --output text
```

Note: `date -u -d '...'` requires GNU date (Linux / Cloud9). On macOS use `date -u -v-24H +%Y-%m-%dT%H:%M:%S`.

**Verified output (2026-05-10):** `1.0` (1 successful pipeline run — the scheduled nightly run. Value increments by 1 per successful run on the same day.)

---

## Trigger and verify the CI/CD pipeline

```bash
# Make a small change to trigger the pipeline
git add stacks/compute_stack.py
git commit -m "doc: note z-score threshold rationale"
GIT_ASKPASS="" GIT_TERMINAL_PROMPT=0 git push codecommit main
# (replace codecommit with origin if using GitHub source)
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
|  Deploy_Prod          |  None      |
+-----------------------+------------+
```

cfn-nag output in Build stage logs (look for):
```
cdk.out/OttNetwork-dev.template.json
| WARN W28
| WARN W29
| WARN W60
Failures count: 0
Warnings count: 5
```

`Failures count: 0` on every template means no policy violations. Warnings (W-codes) are advisory only; the buildspec uses `|| true` so warnings do not fail the pipeline. The templates are synthesized as `-dev` (CDK default env) because the buildspec runs `cdk synth --all` without a context env override.

---

## Composite alarm demonstration

```bash
# Force the composite alarm into ALARM state
aws cloudwatch set-alarm-state \
  --alarm-name ott-lambda-error-rate-demo \
  --state-value ALARM \
  --state-reason "Manual test for workshop demo"
```

Navigate to: **CloudWatch → Alarms → `ott-pipeline-health-demo`** — composite alarm turns red.

```bash
# Reset after capturing screenshot
aws cloudwatch set-alarm-state \
  --alarm-name ott-lambda-error-rate-demo \
  --state-value OK \
  --state-reason "Resetting after demo"
```

---

## Resources deployed

### OttObservability

| Resource | Name | Configuration |
|---|---|---|
| CloudWatch Dashboard | `ott-pipeline-health-demo` | 5 metric widgets |
| CloudWatch Alarm | `ott-lambda-error-rate-demo` | Errors > 5 in 5 min |
| CloudWatch Alarm | `ott-kinesis-iterator-age-demo` | Iterator age threshold |
| CloudWatch Alarm | `ott-glue-job-failure-demo` | DailyPipelineSuccess < 1 in 26h window |
| CloudWatch Alarm | `ott-anomaly-score-breach-demo` | AnomalyScore > 3.0 |
| Composite Alarm | `ott-pipeline-health-demo` | OR of all 4 child alarms |

### OttPipeline

| Stage | Tool | Action |
|---|---|---|
| Source | CodeCommit (default) or GitHub via CodeStar Connection | Push to `main` → artifact |
| Build | CodeBuild (`ott-cdk-synth-{env}`) | `cdk synth` + `pytest` + `cfn_nag_scan` |
| Deploy_Dev | CloudFormation | Deploy `OttNetwork-dev` to dev environment |
| Integration_Test | CodeBuild (`ott-integration-test-{env}`) | `pytest tests/integration/` |
| Manual_Approval | SNS | Gate before production deploy |
| Deploy_Prod | CloudFormation | Deploy `OttNetwork-prod` to prod environment |

---

## Screenshot guidance

**Screenshot 1 — CodePipeline all stages**
Navigate to: **CodePipeline Console → `ott-search-pipeline-demo`**.
Capture all 6 stages with Source, Build, Deploy_Dev, Integration_Test showing Succeeded.
Save as `workshop/static/images/4.8-codepipeline.png`.

**Screenshot 2 — cfn-nag output in CodeBuild logs**
Click the Build stage → View logs. Find the `cfn_nag_scan` section.
Capture the `Failures count: 0` output with at least one template filename visible (e.g. `cdk.out/OttNetwork-dev.template.json`).
Save as `workshop/static/images/4.8-cfnnag.png`.

**Screenshot 3 — CloudWatch dashboard**
Navigate to: **CloudWatch → Dashboards → `ott-pipeline-health-demo`** → Last 24 hours.
All 5 metric widgets must show data (not empty). AnomalyScore widget should show the THE_THAO spike.
Save as `workshop/static/images/4.8-cw-dashboard.png`.

**Screenshot 4 — Composite alarm ALARM then OK**
Capture the composite alarm in red after `set-alarm-state ALARM`, then green after reset.
Save as `workshop/static/images/4.8-alarm-firing.png` and `4.8-alarm-ok.png`.
