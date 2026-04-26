"""CDK app entrypoint for football-predictions infrastructure.

Currently deploys only the daily incremental ingest pipeline. Additional
stacks (feature store, prediction API, UI) can be added here later without
changing this file's shape.
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.feature_stack import FeatureStack
from stacks.hosting_stack import HostingStack
from stacks.inference_stack import InferenceStack
from stacks.ingest_stack import IngestStack
from stacks.observability_stack import ObservabilityStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "eu-west-1"),
)

ingest = IngestStack(
    app,
    "FPIngestStack",
    env=env,
    api_football_secret_name=app.node.try_get_context("api_football_secret_name")
    or "football-predictions/api-football-key",
    data_bucket_name=app.node.try_get_context("data_bucket_name"),
)

features = FeatureStack(
    app,
    "FPFeatureStack",
    env=env,
    data_bucket_name=ingest.data_bucket.bucket_name,
    ingest_state_machine_arn=ingest.state_machine.state_machine_arn,
)

InferenceStack(
    app,
    "FPInferenceStack",
    env=env,
    data_bucket_name=ingest.data_bucket.bucket_name,
    feature_function_arn=features.feature_function.function_arn,
)

HostingStack(
    app,
    "FPHostingStack",
    env=env,
    data_bucket_name=ingest.data_bucket.bucket_name,
)

ObservabilityStack(
    app,
    "FPObservabilityStack",
    env=env,
    ingest_state_machine_arn=ingest.state_machine.state_machine_arn,
    ingest_function_name=ingest.ingest_function.function_name,
    alert_email=app.node.try_get_context("alert_email") or "ekmillenium@hotmail.com",
)

app.synth()
