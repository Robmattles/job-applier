"""Ingestion stack — ARCHITECTURE.md §6 phase 2.

Two Lambdas (see lambda_src/): one polling `known_companies` directly
against Greenhouse/Lever/Ashby (the primary, highest-density source per
the 2026-09-01 live test), one covering the three secondary boards
(Himalayas/Jobicy/RemoteOK) and running name-probing discovery. Both
write into the `postings` table from FoundationStack; the boards Lambda
also writes into `known_companies`.

IAM is entirely `.grant_*()` calls scoped to the exact tables involved —
no hand-written policy, consistent with the "don't pre-guess Lambda
permissions" note in foundation_stack.py.
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
)
from constructs import Construct


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        postings_table: dynamodb.ITableV2,
        known_companies_table: dynamodb.ITableV2,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Shared ATS clients, filters, and DynamoDB store helpers",
        )

        common_env = {
            "POSTINGS_TABLE": postings_table.table_name,
            "KNOWN_COMPANIES_TABLE": known_companies_table.table_name,
        }

        # ------------------------------------------------------------------
        # PRIMARY: direct Greenhouse/Lever/Ashby polling against
        # known_companies (§1) — the highest-density source tested.
        # ------------------------------------------------------------------
        self.ingest_known_companies_fn = lambda_.Function(
            self,
            "IngestKnownCompanies",
            function_name="job-applier-ingest-known-companies",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/ingest_known_companies"),
            layers=[common_layer],
            environment=common_env,
            timeout=Duration.minutes(10),
            memory_size=256,
        )
        postings_table.grant_read_write_data(self.ingest_known_companies_fn)
        known_companies_table.grant_read_data(self.ingest_known_companies_fn)

        # ------------------------------------------------------------------
        # SECONDARY: Himalayas/Jobicy/RemoteOK + name-probing discovery.
        # ------------------------------------------------------------------
        self.ingest_boards_fn = lambda_.Function(
            self,
            "IngestBoards",
            function_name="job-applier-ingest-boards",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/ingest_boards"),
            layers=[common_layer],
            environment=common_env,
            timeout=Duration.minutes(10),
            memory_size=256,
        )
        postings_table.grant_read_write_data(self.ingest_boards_fn)
        known_companies_table.grant_read_write_data(self.ingest_boards_fn)

        # ------------------------------------------------------------------
        # Schedule — every 4 hours. Balances the <48h freshness preference
        # (§1) against not hammering free APIs; easy to tighten later once
        # real volume data (§7) says it's worth it.
        # ------------------------------------------------------------------
        schedule = events.Schedule.rate(Duration.hours(4))

        events.Rule(
            self,
            "IngestKnownCompaniesSchedule",
            rule_name="job-applier-ingest-known-companies-schedule",
            schedule=schedule,
            targets=[targets.LambdaFunction(self.ingest_known_companies_fn)],
        )
        events.Rule(
            self,
            "IngestBoardsSchedule",
            rule_name="job-applier-ingest-boards-schedule",
            schedule=schedule,
            targets=[targets.LambdaFunction(self.ingest_boards_fn)],
        )

        cdk.CfnOutput(
            self,
            "IngestKnownCompaniesFunctionName",
            value=self.ingest_known_companies_fn.function_name,
        )
        cdk.CfnOutput(
            self, "IngestBoardsFunctionName", value=self.ingest_boards_fn.function_name
        )
