import aws_cdk as cdk

from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack
from stacks.ingestion_stack import IngestionStack
from stacks.etl_stack import ETLStack
from stacks.compute_stack import ComputeStack
from stacks.analytics_stack import AnalyticsStack
from stacks.governance_stack import GovernanceStack
from stacks.observability_stack import ObservabilityStack
from stacks.pipeline_stack import PipelineStack

app = cdk.App()

env_name    = app.node.try_get_context("env")         or "dev"
alert_email = app.node.try_get_context("alert_email") or ""

aws_env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "ap-southeast-1",
)

# ── Iteration 0: Foundation ───────────────────────────────────────────────────
network = NetworkStack(app, f"OttNetwork-{env_name}", env=aws_env, env_name=env_name)
storage = StorageStack(app, f"OttStorage-{env_name}", env=aws_env, env_name=env_name)
storage.add_dependency(network)

# ── Iteration 1: Ingest layer ─────────────────────────────────────────────────
ingestion = IngestionStack(
    app, f"OttIngestion-{env_name}",
    env=aws_env, env_name=env_name,
    bucket=storage.bucket,
    kinesis_key=storage.kinesis_key,
    s3_key=storage.s3_key,
)
ingestion.add_dependency(storage)

# ── Iteration 2: Glue ETL ─────────────────────────────────────────────────────
etl = ETLStack(
    app, f"OttETL-{env_name}",
    env=aws_env, env_name=env_name,
    bucket=storage.bucket,
    s3_key=storage.s3_key,
    glue_sg=network.glue_sg,
    vpc=network.vpc,
)
etl.add_dependency(ingestion)

# ── Iteration 3: Anomaly detection ───────────────────────────────────────────
compute = ComputeStack(
    app, f"OttCompute-{env_name}",
    env=aws_env, env_name=env_name,
    alert_email=alert_email,
    bucket=storage.bucket,
    stream=ingestion.stream,
    dynamodb_key=storage.dynamodb_key,
    sns_key=storage.sns_key,
    lambda_sg=network.lambda_sg,
    vpc=network.vpc,
)
compute.add_dependency(ingestion)

# ── Iteration 4: Step Functions + Gold layer ─────────────────────────────────
analytics = AnalyticsStack(
    app, f"OttAnalytics-{env_name}",
    env=aws_env, env_name=env_name,
    bucket=storage.bucket,
    enrichment_job_name=etl.enrichment_job_name,
    crawler_name=etl.crawler_name,
    baseline_updater_arn=compute.baseline_updater_arn,
    failure_topic_arn=compute.alert_topic_arn,
)
analytics.add_dependency(etl)
analytics.add_dependency(compute)

# ── Iteration 6: Lake Formation governance ────────────────────────────────────
governance = GovernanceStack(
    app, f"OttGovernance-{env_name}",
    env=aws_env, env_name=env_name,
    bucket=storage.bucket,
)
governance.add_dependency(analytics)

# ── Iteration 7: Observability hardening ─────────────────────────────────────
observability = ObservabilityStack(
    app, f"OttObservability-{env_name}",
    env=aws_env, env_name=env_name, alert_email=alert_email,
    anomaly_fn_name=compute.anomaly_detector_name,
    stream_name=ingestion.stream_name,
)
observability.add_dependency(compute)

# ── Iteration 7: CI/CD pipeline ──────────────────────────────────────────────
pipeline = PipelineStack(
    app, f"OttPipeline-{env_name}",
    env=aws_env, env_name=env_name, alert_email=alert_email,
)
pipeline.add_dependency(observability)
pipeline.add_dependency(governance)

app.synth()
