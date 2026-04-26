"""IngestStack — daily forward-sync ingest for football data.

Components:
    * S3 bucket for raw API-Football JSONs
    * Docker-image Lambda that runs the ingest handler
    * Step Functions state machine chaining fetch_fixtures_window +
      fetch_fixture_details for club and national domains
    * EventBridge rule firing daily at 06:00 UTC
    * Dead-letter queue for failed Lambda invocations

No feature rebuild — raw JSON only. Extend later once the first month
of data has accumulated.
"""

from __future__ import annotations

from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


# Path to the repo root relative to this file: infrastructure/stacks/ingest_stack.py
REPO_ROOT = Path(__file__).resolve().parents[2]


class IngestStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        api_football_secret_name: str,
        data_bucket_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- S3 raw data bucket ------------------------------------------------
        data_bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=data_bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            enforce_ssl=True,
            # Keep data if the stack is ever destroyed by accident.
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionOldRawToIA",
                    enabled=True,
                    prefix="",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(60),
                        ),
                    ],
                )
            ],
        )

        # --- API-Football key from Secrets Manager ----------------------------
        api_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "ApiFootballSecret", api_football_secret_name
        )

        # --- Dead-letter queue ------------------------------------------------
        dlq = sqs.Queue(
            self,
            "IngestDLQ",
            retention_period=Duration.days(14),
        )

        # --- Lambda function (container image) --------------------------------
        ingest_fn = _lambda.DockerImageFunction(
            self,
            "IngestFunction",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(REPO_ROOT),
                file="Dockerfile.lambda",
            ),
            memory_size=1024,
            timeout=Duration.minutes(15),
            dead_letter_queue=dlq,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "API_FOOTBALL_KEY_SECRET_ARN": api_secret.secret_arn,
                "API_FOOTBALL_PLAN": "pro",
            },
        )
        data_bucket.grant_read_write(ingest_fn)
        api_secret.grant_read(ingest_fn)
        ingest_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "cloudwatch:namespace": "FootballPredictions/Ingest",
                    },
                },
            ),
        )

        # --- Step Functions ---------------------------------------------------
        def _invoke(id_: str, payload: dict) -> tasks.LambdaInvoke:
            return tasks.LambdaInvoke(
                self,
                id_,
                lambda_function=ingest_fn,
                payload=sfn.TaskInput.from_object(payload),
                output_path="$.Payload",
                retry_on_service_exceptions=True,
            )

        def _domain_chain(domain: str) -> sfn.IChainable:
            fetch_window = _invoke(
                f"{domain.title()}FetchFixturesWindow",
                {"task": "fetch_fixtures_window", "domain": domain},
            )
            fetch_details = _invoke(
                f"{domain.title()}FetchFixtureDetails",
                {
                    "task": "fetch_fixture_details",
                    "domain": domain,
                    "params": {
                        "fixture_ids.$": "$.new_fixture_ids",
                    },
                },
            )
            return fetch_window.next(fetch_details)

        # Run club and national sequentially to respect API rate limits on the
        # Pro plan (450/min). Parallel would double our burst rate.
        definition = _domain_chain("club").next(_domain_chain("national"))

        state_machine = sfn.StateMachine(
            self,
            "IngestStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(1),
        )

        # --- EventBridge daily schedule --------------------------------------
        events.Rule(
            self,
            "DailyIngestRule",
            description="Trigger daily incremental football-data ingest at 06:00 UTC",
            schedule=events.Schedule.cron(hour="6", minute="0"),
            targets=[targets.SfnStateMachine(state_machine)],
        )

        # --- Exports ----------------------------------------------------------
        self.data_bucket = data_bucket
        self.ingest_function = ingest_fn
        self.state_machine = state_machine

        CfnOutput(self, "DataBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(self, "IngestFunctionName", value=ingest_fn.function_name)
