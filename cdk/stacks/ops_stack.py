"""Ops stack — ARCHITECTURE.md §6 phases 8 (reporting) and 9
(guardrails hardening).

- `job-applier-sweeper` (every 30 min): unsticks applications stranded
  by the `starting_position=LATEST` gap in every stream-triggered stage,
  and surfaces NEEDS_REVIEW applications that otherwise reach Matt
  through no channel at all.
- `job-applier-digest` (weekly): funnel stats, framed around the §5
  question of whether the weekly cap has earned a raise.
- A billing alarm, per §5's cost ceiling.
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
)
from constructs import Construct

APPROVAL_EMAIL = "you@example.com"


class OpsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        postings_table: dynamodb.ITableV2,
        applications_table: dynamodb.ITableV2,
        documents_bucket: s3.IBucket,
        monthly_budget_usd: float = 50.0,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common-ops",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )

        ses_send = iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"], resources=["*"]
        )

        # ------------------------------------------------------------------
        # Sweeper
        # ------------------------------------------------------------------
        self.sweeper_fn = lambda_.Function(
            self,
            "Sweeper",
            function_name="job-applier-sweeper",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/sweeper"),
            layers=[common_layer],
            environment={
                "APPLICATIONS_TABLE": applications_table.table_name,
                "POSTINGS_TABLE": postings_table.table_name,
                "STALE_MINUTES": "30",
                # §5 kill switch only (config/ramp.json). The sweeper's
                # whole job is pushing stalled rows onward, which is
                # exactly what a pause has to stop.
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
            },
            timeout=Duration.minutes(5),
            memory_size=256,
        )
        applications_table.grant_read_write_data(self.sweeper_fn)
        postings_table.grant_read_data(self.sweeper_fn)
        documents_bucket.grant_read(self.sweeper_fn, "config/*")
        # No ses_send grant — the sweeper no longer sends its own
        # per-application NEEDS_REVIEW email (removed 2026-09-03, see
        # sweeper/handler.py's module docstring). It still unsticks
        # stalled rows; it just doesn't email about them anymore.

        events.Rule(
            self,
            "SweeperSchedule",
            rule_name="job-applier-sweeper-schedule",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            targets=[targets.LambdaFunction(self.sweeper_fn)],
        )

        # ------------------------------------------------------------------
        # Weekly digest
        # ------------------------------------------------------------------
        self.digest_fn = lambda_.Function(
            self,
            "Digest",
            function_name="job-applier-digest",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/digest"),
            layers=[common_layer],
            environment={
                "APPLICATIONS_TABLE": applications_table.table_name,
                "POSTINGS_TABLE": postings_table.table_name,
                "APPROVAL_TO_EMAIL": APPROVAL_EMAIL,
                "APPROVAL_FROM_EMAIL": APPROVAL_EMAIL,
            },
            timeout=Duration.minutes(5),
            memory_size=256,
        )
        applications_table.grant_read_data(self.digest_fn)
        postings_table.grant_read_data(self.digest_fn)
        self.digest_fn.add_to_role_policy(ses_send)

        events.Rule(
            self,
            "DigestSchedule",
            rule_name="job-applier-digest-schedule",
            # Monday 13:00 UTC — a week's worth of funnel to look at
            # before deciding whether the cap moves.
            schedule=events.Schedule.cron(minute="0", hour="13", week_day="MON"),
            targets=[targets.LambdaFunction(self.digest_fn)],
        )

        # ------------------------------------------------------------------
        # Billing alarm (§5 cost ceiling)
        # ------------------------------------------------------------------
        # AWS publishes EstimatedCharges to us-east-1 only, which is where
        # this stack lives anyway.
        cloudwatch.Alarm(
            self,
            "MonthlySpendAlarm",
            alarm_name="job-applier-monthly-spend",
            alarm_description=(
                f"job-applier estimated AWS charges exceeded ${monthly_budget_usd:.0f}. "
                "Bedrock is the variable cost here — check whether the weekly cap or a "
                "backlog catch-up run is responsible before raising the ceiling."
            ),
            metric=cloudwatch.Metric(
                namespace="AWS/Billing",
                metric_name="EstimatedCharges",
                dimensions_map={"Currency": "USD"},
                statistic="Maximum",
                period=Duration.hours(6),
            ),
            threshold=monthly_budget_usd,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        cdk.CfnOutput(self, "SweeperFunctionName", value=self.sweeper_fn.function_name)
        cdk.CfnOutput(self, "DigestFunctionName", value=self.digest_fn.function_name)
