"""
Iteration 0 — Network foundation.

VPC layout (target CIDRs per spec §4.13):
  Private-a/b : 10.0.1.0/24, 10.0.2.0/24  — Glue, Lambda
  Public-a/b  : 10.0.101.0/24, 10.0.102.0/24 — NAT Gateway
CDK auto-assigns CIDRs sequentially; exact ranges can be pinned via L1
constructs in a production hardening pass.

VPC Endpoints:
  Gateway   : S3 (free)
  Interface : Glue, Athena, CloudWatch, CloudWatch Logs, SNS,
              Kinesis Streams, Kinesis Firehose, Secrets Manager, STS, KMS

NAT Gateways: 1 (AZ-a only). Tear down between iterations to save ~$0.045/hr.
"""
from aws_cdk import (
    Stack,
    Tags,
    aws_ec2 as ec2,
)
from constructs import Construct
from stacks import tag_stack


class NetworkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, env_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── VPC ───────────────────────────────────────────────────────────────
        self._vpc = ec2.Vpc(
            self, "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                    map_public_ip_on_launch=False,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
            enable_dns_support=True,
            enable_dns_hostnames=True,
        )

        # ── Security Groups ───────────────────────────────────────────────────
        # Glue ETL: HTTPS egress only (to VPC CIDR where endpoints live)
        self._glue_sg = ec2.SecurityGroup(
            self, "GlueSg",
            vpc=self._vpc,
            security_group_name=f"ott-glue-{env_name}",
            description="Glue ETL - HTTPS egress to VPC endpoints",
            allow_all_outbound=False,
        )
        self._glue_sg.add_egress_rule(
            ec2.Peer.ipv4(self._vpc.vpc_cidr_block),
            ec2.Port.tcp(443),
            "HTTPS to VPC endpoints",
        )

        # Lambda functions: same policy
        self._lambda_sg = ec2.SecurityGroup(
            self, "LambdaSg",
            vpc=self._vpc,
            security_group_name=f"ott-lambda-{env_name}",
            description="Lambda - HTTPS egress to VPC endpoints",
            allow_all_outbound=False,
        )
        self._lambda_sg.add_egress_rule(
            ec2.Peer.ipv4(self._vpc.vpc_cidr_block),
            ec2.Port.tcp(443),
            "HTTPS to VPC endpoints",
        )

        # Endpoint SG: accepts inbound 443 from Glue and Lambda SGs
        endpoint_sg = ec2.SecurityGroup(
            self, "EndpointSg",
            vpc=self._vpc,
            security_group_name=f"ott-endpoints-{env_name}",
            description="VPC interface endpoints - inbound 443 from Glue and Lambda",
            allow_all_outbound=False,
        )
        endpoint_sg.add_ingress_rule(
            self._glue_sg, ec2.Port.tcp(443), "Glue to endpoints",
        )
        endpoint_sg.add_ingress_rule(
            self._lambda_sg, ec2.Port.tcp(443), "Lambda to endpoints",
        )

        # ── VPC Endpoints ─────────────────────────────────────────────────────
        # Gateway endpoint (S3) — free, no SG needed
        self._vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        private_subnets = ec2.SubnetSelection(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
        )

        # Named interface endpoints backed by CDK enum values
        interface_services: dict[str, ec2.InterfaceVpcEndpointAwsService] = {
            "Glue":           ec2.InterfaceVpcEndpointAwsService.GLUE,
            "Athena":         ec2.InterfaceVpcEndpointAwsService.ATHENA,
            "CloudWatchLogs": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            "CloudWatch":     ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH,
            "Sns":            ec2.InterfaceVpcEndpointAwsService.SNS,
            "KinesisStreams":  ec2.InterfaceVpcEndpointAwsService.KINESIS_STREAMS,
            "SecretsManager": ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            "Sts":            ec2.InterfaceVpcEndpointAwsService.STS,
            "Kms":            ec2.InterfaceVpcEndpointAwsService.KMS,
        }
        for logical_id, service in interface_services.items():
            self._vpc.add_interface_endpoint(
                f"{logical_id}Endpoint",
                service=service,
                subnets=private_subnets,
                security_groups=[endpoint_sg],
                private_dns_enabled=True,
            )

        # Kinesis Firehose — no CDK enum in older versions; use service name directly
        self._vpc.add_interface_endpoint(
            "FirehoseEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService("kinesis-firehose"),
            subnets=private_subnets,
            security_groups=[endpoint_sg],
            private_dns_enabled=True,
        )

        tag_stack(self, env_name)

    # ── Exported properties ───────────────────────────────────────────────────

    @property
    def vpc(self) -> ec2.Vpc:
        return self._vpc

    @property
    def glue_sg(self) -> ec2.SecurityGroup:
        return self._glue_sg

    @property
    def lambda_sg(self) -> ec2.SecurityGroup:
        return self._lambda_sg
