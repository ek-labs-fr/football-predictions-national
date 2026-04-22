"""FeatureStack — daily feature-engineering Lambda, triggered after ingest.

Components:
    * DockerImageFunction ``FeatureFunction`` (pandas/pyarrow image)
    * EventBridge rule matching Step Functions ``SUCCEEDED`` for the ingest
      state machine → invokes the Lambda with ``{"domain": "both"}``
    * IAM grant: read/write on the existing FPIngestStack data bucket

The stack imports the existing bucket and state machine ARN by name so that
the two stacks can be updated independently (no CloudFormation export/import
coupling).
"""

from __future__ import annotations

from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct


REPO_ROOT = Path(__file__).resolve().parents[2]


class FeatureStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        data_bucket_name: str,
        ingest_state_machine_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- Import the existing data bucket ----------------------------------
        data_bucket = s3.Bucket.from_bucket_name(self, "DataBucket", data_bucket_name)

        # --- Lambda (container image) ----------------------------------------
        feature_fn = _lambda.DockerImageFunction(
            self,
            "FeatureFunction",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(REPO_ROOT),
                file="Dockerfile.features",
            ),
            memory_size=3008,
            timeout=Duration.minutes(15),
            environment={
                "DATA_BUCKET": data_bucket_name,
            },
        )
        data_bucket.grant_read_write(feature_fn)

        # --- EventBridge: fire on ingest state-machine SUCCEEDED --------------
        events.Rule(
            self,
            "OnIngestSucceeded",
            description="Run feature pipeline after the daily ingest succeeds",
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": ["SUCCEEDED"],
                    "stateMachineArn": [ingest_state_machine_arn],
                },
            ),
            targets=[
                targets.LambdaFunction(
                    feature_fn,
                    event=events.RuleTargetInput.from_object({"domain": "both"}),
                )
            ],
        )

        # --- Exports ----------------------------------------------------------
        self.feature_function = feature_fn

        CfnOutput(self, "FeatureFunctionName", value=feature_fn.function_name)
