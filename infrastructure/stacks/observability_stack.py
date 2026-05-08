"""ObservabilityStack — alarms + dashboard for the daily pipeline.

Covers the full ingest → feature → inference chain. The earlier version
only watched the ingest leg, which let a silent FeatureFunction crash-loop
go unnoticed for ~36h while the dashboard JSON went stale on S3.

Components:
    * SNS topic ``IngestAlerts`` with email subscription
    * CloudWatch alarms wired to SNS:
        - IngestExecutionFailed       — Step Functions FailedExecutions > 0
        - IngestStale                 — no execution started in the last 25h
        - IngestLambdaErrors          — Ingest Lambda Errors > 0
        - FeatureLambdaErrors         — Feature Lambda Errors > 0
        - InferenceLambdaErrors       — Inference Lambda Errors > 0
        - InferenceStale              — Inference Lambda hasn't been
                                        invoked in the last 25h (catches
                                        silent breaks in the feature →
                                        inference EventBridge link)
        - ApiQuotaLow                 — < 20% requests remaining (custom metric)
        - FixturesIngestedDrop        — < 1 fixture/day (custom metric, per domain)
    * CloudWatch dashboard for at-a-glance status

Custom metrics are emitted by src/data/lambda_handlers.py under the
``FootballPredictions/Ingest`` namespace. Free-tier coverage:
- 10 free CW alarms/month (we use 8)
- 10 free custom metrics/month (we use 3 — FixturesIngested×2 domains + quota)
- 1k free SNS email notifications/month
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
)
from aws_cdk import (
    aws_cloudwatch as cw,
)
from aws_cdk import (
    aws_cloudwatch_actions as cw_actions,
)
from aws_cdk import (
    aws_sns as sns,
)
from aws_cdk import (
    aws_sns_subscriptions as subs,
)

if TYPE_CHECKING:
    from constructs import Construct

_API_FOOTBALL_PRO_DAILY_LIMIT = 7500
_QUOTA_LOW_THRESHOLD = int(_API_FOOTBALL_PRO_DAILY_LIMIT * 0.20)


class ObservabilityStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        ingest_state_machine_arn: str,
        ingest_function_name: str,
        feature_function_name: str,
        inference_function_name: str,
        alert_email: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- SNS topic + email subscription -----------------------------------
        topic = sns.Topic(self, "IngestAlerts", display_name="FP ingest alerts")
        topic.add_subscription(subs.EmailSubscription(alert_email))
        action = cw_actions.SnsAction(topic)

        # --- Step Functions metrics (state machine ARN) -----------------------
        sm_dimensions = {"StateMachineArn": ingest_state_machine_arn}

        failed_executions = cw.Metric(
            namespace="AWS/States",
            metric_name="ExecutionsFailed",
            dimensions_map=sm_dimensions,
            period=Duration.hours(1),
            statistic="Sum",
        )
        executions_started = cw.Metric(
            namespace="AWS/States",
            metric_name="ExecutionsStarted",
            dimensions_map=sm_dimensions,
            period=Duration.hours(25),
            statistic="Sum",
        )

        # --- Lambda metrics ---------------------------------------------------
        def lambda_errors_metric(function_name: str) -> cw.Metric:
            return cw.Metric(
                namespace="AWS/Lambda",
                metric_name="Errors",
                dimensions_map={"FunctionName": function_name},
                period=Duration.hours(1),
                statistic="Sum",
            )

        lambda_errors = lambda_errors_metric(ingest_function_name)
        feature_errors = lambda_errors_metric(feature_function_name)
        inference_errors = lambda_errors_metric(inference_function_name)

        inference_invocations = cw.Metric(
            namespace="AWS/Lambda",
            metric_name="Invocations",
            dimensions_map={"FunctionName": inference_function_name},
            period=Duration.hours(25),
            statistic="Sum",
        )

        # --- Custom metrics emitted by the ingest Lambda ----------------------
        quota_remaining = cw.Metric(
            namespace="FootballPredictions/Ingest",
            metric_name="ApiFootballRequestsRemaining",
            period=Duration.minutes(15),
            statistic="Minimum",
        )

        def fixtures_ingested(domain: str) -> cw.Metric:
            # 7-day rolling sum: the metric counts *newly completed* fixtures
            # (fixture IDs not yet in the manifest), so a 1-day window has
            # legitimate zeros on mid-week days with no completed matches.
            # Catching a real ingestion outage requires summing over a full
            # match week.
            return cw.Metric(
                namespace="FootballPredictions/Ingest",
                metric_name="FixturesIngested",
                dimensions_map={"Domain": domain},
                period=Duration.days(7),
                statistic="Sum",
            )

        # --- Alarms -----------------------------------------------------------
        alarms: list[cw.Alarm] = []

        a = cw.Alarm(
            self,
            "IngestExecutionFailed",
            metric=failed_executions,
            evaluation_periods=1,
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            alarm_description="Daily ingest Step Functions execution failed.",
        )
        alarms.append(a)

        a = cw.Alarm(
            self,
            "IngestStale",
            metric=executions_started,
            evaluation_periods=1,
            threshold=1,
            comparison_operator=cw.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.BREACHING,
            alarm_description="Daily ingest hasn't started in the last 25 hours.",
        )
        alarms.append(a)

        a = cw.Alarm(
            self,
            "IngestLambdaErrors",
            metric=lambda_errors,
            evaluation_periods=1,
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            alarm_description="Ingest Lambda emitted an error in the last hour.",
        )
        alarms.append(a)

        a = cw.Alarm(
            self,
            "FeatureLambdaErrors",
            metric=feature_errors,
            evaluation_periods=1,
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            alarm_description="Feature Lambda emitted an error in the last hour.",
        )
        alarms.append(a)

        a = cw.Alarm(
            self,
            "InferenceLambdaErrors",
            metric=inference_errors,
            evaluation_periods=1,
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            alarm_description="Inference Lambda emitted an error in the last hour.",
        )
        alarms.append(a)

        a = cw.Alarm(
            self,
            "InferenceStale",
            metric=inference_invocations,
            evaluation_periods=1,
            threshold=1,
            comparison_operator=cw.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.BREACHING,
            alarm_description=(
                "Inference Lambda hasn't been invoked in the last 25h — "
                "the feature → inference EventBridge link may be broken."
            ),
        )
        alarms.append(a)

        a = cw.Alarm(
            self,
            "ApiQuotaLow",
            metric=quota_remaining,
            evaluation_periods=1,
            threshold=_QUOTA_LOW_THRESHOLD,
            comparison_operator=cw.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            alarm_description=(
                f"API-Football daily quota below 20% ({_QUOTA_LOW_THRESHOLD} requests remaining)."
            ),
        )
        alarms.append(a)

        for domain in ("club", "national"):
            a = cw.Alarm(
                self,
                f"FixturesIngestedDrop{domain.title()}",
                metric=fixtures_ingested(domain),
                evaluation_periods=1,
                threshold=1,
                comparison_operator=cw.ComparisonOperator.LESS_THAN_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.BREACHING,
                alarm_description=(
                    f"No fixtures ingested for {domain} in the last 7 days — "
                    "data feed may be silently failing or this league is "
                    "off-season."
                ),
            )
            alarms.append(a)

        for alarm in alarms:
            alarm.add_alarm_action(action)
            alarm.add_ok_action(action)

        # --- Dashboard --------------------------------------------------------
        dashboard = cw.Dashboard(
            self,
            "IngestDashboard",
            dashboard_name="FP-Ingest",
            default_interval=Duration.days(7),
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Step Functions executions",
                left=[
                    executions_started.with_(label="Started", period=Duration.hours(1)),
                    failed_executions.with_(label="Failed", period=Duration.hours(1)),
                ],
                width=12,
            ),
            cw.GraphWidget(
                title="Ingest Lambda errors / duration",
                left=[lambda_errors.with_(label="Errors", period=Duration.hours(1))],
                right=[
                    cw.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Duration",
                        dimensions_map={"FunctionName": ingest_function_name},
                        period=Duration.hours(1),
                        statistic="Average",
                        label="Duration (avg ms)",
                    ),
                ],
                width=12,
            ),
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Fixtures ingested per day",
                left=[
                    fixtures_ingested("club").with_(label="Club"),
                    fixtures_ingested("national").with_(label="National"),
                ],
                width=12,
            ),
            cw.GraphWidget(
                title="API-Football quota remaining",
                left=[quota_remaining.with_(label="Remaining", period=Duration.minutes(15))],
                width=12,
            ),
        )

        self.topic = topic
        CfnOutput(self, "AlertTopicArn", value=topic.topic_arn)
        CfnOutput(self, "DashboardName", value=dashboard.dashboard_name)
