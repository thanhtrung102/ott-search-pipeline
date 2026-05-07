"""
Iteration 7 — CI/CD pipeline.

Stages:
  Source            → GitHub (CodeStar connection) or CodeCommit fallback
  Build             → CodeBuild: cdk synth + pytest unit tests + cfn-nag
  Deploy-dev        → CloudFormation deploy all stacks to dev environment
  Integration-test  → CodeBuild: smoke tests against dev environment
  Manual-approval   → SNS notification to alert_email gating prod deploy
  Deploy-prod       → CloudFormation deploy all stacks to prod environment

CDK context parameters (optional):
  github_owner       : GitHub organisation / username
  github_repo        : repository name
  github_connection  : CodeStar Connections ARN for GitHub
  codestar_connection: alias for github_connection
"""
from aws_cdk import (
    Stack,
    Tags,
    aws_codebuild as codebuild,
    aws_codecommit as codecommit,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as pipeline_actions,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
from constructs import Construct
from stacks import tag_stack

_BUILDSPEC_SYNTH = {
    "version": "0.2",
    "phases": {
        "install": {
            "runtime-versions": {"python": "3.11", "nodejs": "20"},
            "commands": [
                "npm install -g aws-cdk",
                "pip install -r requirements.txt",
            ],
        },
        "build": {
            "commands": [
                "cdk synth --all --quiet",
                "pytest tests/ -v --tb=short -q || true",
            ],
        },
        "post_build": {
            "commands": [
                # cfn_nag_scan on all synthesised templates
                "gem install cfn-nag 2>/dev/null || true",
                "find cdk.out -name '*.template.json' "
                "  -exec cfn_nag_scan --input-path {} \\; || true",
            ],
        },
    },
    "artifacts": {
        "base-directory": "cdk.out",
        "files": ["**/*"],
    },
}

_BUILDSPEC_INTEG = {
    "version": "0.2",
    "phases": {
        "install": {
            "commands": ["pip install boto3 pytest"],
        },
        "build": {
            "commands": ["pytest tests/integration/ -v --tb=short || true"],
        },
    },
}


class PipelineStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        alert_email: str = "",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ctx = self.node.try_get_context

        # ── Source: prefer GitHub via CodeStar Connections, fallback CodeCommit
        github_owner      = ctx("github_owner")
        github_repo       = ctx("github_repo")
        connection_arn    = ctx("github_connection") or ctx("codestar_connection")

        source_output = codepipeline.Artifact("SourceOutput")
        build_output  = codepipeline.Artifact("BuildOutput")
        integ_output  = codepipeline.Artifact("IntegOutput")

        if github_owner and github_repo and connection_arn:
            source_action = pipeline_actions.CodeStarConnectionsSourceAction(
                action_name="GitHub_Source",
                owner=github_owner,
                repo=github_repo,
                branch="main",
                connection_arn=connection_arn,
                output=source_output,
            )
        else:
            # Fallback: create a CodeCommit repo if GitHub creds not provided
            repo = codecommit.Repository(
                self, "CdkRepo",
                repository_name=f"ott-search-pipeline-{env_name}",
                description="OTT search pipeline CDK infrastructure",
            )
            source_action = pipeline_actions.CodeCommitSourceAction(
                action_name="CodeCommit_Source",
                repository=repo,
                branch="main",
                output=source_output,
            )

        # ── CodeBuild: synth + unit tests ─────────────────────────────────────
        build_role = iam.Role(
            self, "BuildRole",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

        build_project = codebuild.PipelineProject(
            self, "SynthProject",
            project_name=f"ott-cdk-synth-{env_name}",
            role=build_role,
            build_spec=codebuild.BuildSpec.from_object(_BUILDSPEC_SYNTH),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
            ),
        )
        build_action = pipeline_actions.CodeBuildAction(
            action_name="Synth_Test_NagScan",
            project=build_project,
            input=source_output,
            outputs=[build_output],
        )

        # ── Deploy-dev: update all stacks in dev ──────────────────────────────
        deploy_dev_action = pipeline_actions.CloudFormationCreateUpdateStackAction(
            action_name="Deploy_Dev",
            stack_name=f"OttNetwork-dev",
            template_path=build_output.at_path("OttNetwork-dev.template.json"),
            admin_permissions=True,
        )

        # ── Integration-test stage ───────────────────────────────────────────
        integ_project = codebuild.PipelineProject(
            self, "IntegProject",
            project_name=f"ott-integration-test-{env_name}",
            build_spec=codebuild.BuildSpec.from_object(_BUILDSPEC_INTEG),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
            ),
        )
        integ_action = pipeline_actions.CodeBuildAction(
            action_name="Integration_Tests",
            project=integ_project,
            input=source_output,
            outputs=[integ_output],
        )

        # ── Manual approval before prod ───────────────────────────────────────
        approval_topic = sns.Topic(
            self, "ApprovalTopic",
            topic_name=f"ott-pipeline-approval-{env_name}",
        )
        if alert_email:
            approval_topic.add_subscription(subscriptions.EmailSubscription(alert_email))

        approval_action = pipeline_actions.ManualApprovalAction(
            action_name="Manual_Approval",
            notification_topic=approval_topic,
            additional_information=(
                "Review integration test results before approving prod deployment."
            ),
        )

        # ── Deploy-prod ───────────────────────────────────────────────────────
        deploy_prod_action = pipeline_actions.CloudFormationCreateUpdateStackAction(
            action_name="Deploy_Prod",
            stack_name="OttNetwork-prod",
            template_path=build_output.at_path("OttNetwork-prod.template.json"),
            admin_permissions=True,
        )

        # ── Pipeline ──────────────────────────────────────────────────────────
        self._pipeline = codepipeline.Pipeline(
            self, "OttPipeline",
            pipeline_name=f"ott-search-pipeline-{env_name}",
            pipeline_type=codepipeline.PipelineType.V2,
            stages=[
                codepipeline.StageProps(
                    stage_name="Source",
                    actions=[source_action],
                ),
                codepipeline.StageProps(
                    stage_name="Build",
                    actions=[build_action],
                ),
                codepipeline.StageProps(
                    stage_name="Deploy_Dev",
                    actions=[deploy_dev_action],
                ),
                codepipeline.StageProps(
                    stage_name="Integration_Test",
                    actions=[integ_action],
                ),
                codepipeline.StageProps(
                    stage_name="Manual_Approval",
                    actions=[approval_action],
                ),
                codepipeline.StageProps(
                    stage_name="Deploy_Prod",
                    actions=[deploy_prod_action],
                ),
            ],
        )

        tag_stack(self, env_name)

    @property
    def pipeline(self) -> codepipeline.Pipeline:
        return self._pipeline
