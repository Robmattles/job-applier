"""Scoring stack — ARCHITECTURE.md §6 phase 3.

The fit-scoring Lambda's own execution role (auto-created by CDK, distinct
from the `job-applier-agent` deploy-time IAM user) needs Bedrock invoke
permission added explicitly — DynamoDB/S3 have `.grant_*()` convenience
methods, Bedrock doesn't, so this is the one place a raw PolicyStatement
is hand-written rather than granted. Scoped identically to what
`job-applier-agent`'s own policy allows (region-wildcarded foundation-model
ARN + this account's inference profiles — see iam-policy-job-applier-core.json,
"BedrockInvoke" — cross-region inference profiles route to whatever region
Bedrock picks, confirmed 2026-09-02 routing to us-east-2 from a us-east-1 call).
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct


class ScoringStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        postings_table: dynamodb.ITableV2,
        known_companies_table: dynamodb.ITableV2,
        documents_bucket: s3.IBucket,
        generation_queue: sqs.IQueue,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common-scoring",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )

        self.fit_scoring_fn = lambda_.Function(
            self,
            "FitScoring",
            function_name="job-applier-fit-scoring",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/fit_scoring"),
            layers=[common_layer],
            environment={
                "POSTINGS_TABLE": postings_table.table_name,
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
                # TEMPORARY 2026-09-02: Haiku 4.5 (the intended "cheap model" per
                # §2) is gated behind Anthropic's use-case-details form for
                # newly-released models — CLI submission attempts got a bare
                # "Invalid form data" with no further detail; console Playground
                # hit the same wall. Sonnet 4.5 has no such gate right now, so
                # scoring runs on that until the form clears — switch back once
                # it does, since Sonnet is real money at 100/week, not "cheap."
                "BEDROCK_MODEL_ID": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                # Ramp per ARCHITECTURE.md §5 — start at the low end of week 1,
                # raise by hand week to week rather than auto-escalating. This
                # gates promotion to QUALIFIED, not scoring throughput below —
                # raising MAX_POSTINGS_PER_RUN doesn't change how many
                # applications queue for approval.
                "WEEKLY_CAP": "10",
                "FIT_SCORE_THRESHOLD": "60",
                # Points/day subtracted from fit_score for the weekly-cap
                # ranking (§5). 0.5 -> 3 on 2026-09-02, once real scores
                # turned out to cluster in a ~70-82 band that barely
                # differentiates on fit alone. 3 -> 6 on 2026-09-03, Matt:
                # "I want a regular feed of new listings most relevant to
                # me." The entire 739-posting corpus was ingested across two
                # days during the build, and measured against it, 3 pts/day
                # meant a genuinely new listing took 3-5 days to outrank the
                # pile; 6 makes it 1-2. It also permanently favors recency in
                # steady state — a 10-point-better older posting now has to
                # be under ~2 days old to still win.
                "AGE_PENALTY_PER_DAY": "6",
                # Raised 2026-09-02 from 25 to clear a one-time 609-posting
                # backlog (repeated dev-time ingestion runs, not real steady
                # -state volume) that was starving mid-level postings of a
                # scoring turn — DynamoDB Scan order isn't recency-ordered,
                # so the same ~25 items kept getting reprocessed each run.
                # Left high afterward: harmless at steady state (~5-8/day),
                # and gives headroom if a backlog ever piles up again.
                "MAX_POSTINGS_PER_RUN": "60",
                "GENERATION_QUEUE_URL": generation_queue.queue_url,
                # The gate resolves aggregator-sourced postings to a real
                # employer form through the board tokens ingestion
                # discovers — see job_applier_common.submittability.
                "KNOWN_COMPANIES_TABLE": known_companies_table.table_name,
            },
            # Bumped alongside MAX_POSTINGS_PER_RUN — 60 postings at the
            # observed ~13s/posting needs headroom past the old 10-min cap.
            timeout=Duration.minutes(15),
            memory_size=512,
            # Confirmed live 2026-09-02: a manual retry overlapping the :30
            # EventBridge schedule caused two concurrent executions — one
            # posting got scored twice (wasted Bedrock calls), and _pass2_gate's
            # read-then-write weekly-cap check (a plain eventually-consistent
            # Scan, no lock) drifted between the two runs' own counts. It
            # landed under WEEKLY_CAP this time by luck, not guarantee.
            # reserved_concurrent_executions=1 would be the obvious fix but
            # this account's UnreservedConcurrentExecution floor is already
            # at the 10 minimum, so reserving any capacity is rejected —
            # see the DynamoDB lock in handler.py instead (self-releasing,
            # no account-quota dependency).
        )
        postings_table.grant_read_write_data(self.fit_scoring_fn)
        known_companies_table.grant_read_data(self.fit_scoring_fn)
        documents_bucket.grant_read(self.fit_scoring_fn)
        generation_queue.grant_send_messages(self.fit_scoring_fn)
        self.fit_scoring_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    f"arn:aws:bedrock:*::foundation-model/anthropic.claude*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/*claude*",
                ],
            )
        )
        # Anthropic models on Bedrock are served via AWS Marketplace under
        # the hood — confirmed 2026-09-02, live: the invoking principal
        # needs marketplace-subscription permissions to complete a
        # one-time per-model account subscription, even though the
        # console's own docs describe this as "enabled account-wide for
        # all users" once any one principal does it. In practice each
        # new invoking role hit the gate independently, so granting it
        # here rather than assuming it propagates.
        self.fit_scoring_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["aws-marketplace:ViewSubscriptions", "aws-marketplace:Subscribe"],
                resources=["*"],  # marketplace subscription actions aren't resource-scopable
            )
        )

        # Runs after the ingestion schedule (§6 phase 2 is every 4h on the
        # hour); offset by 30 minutes so a scoring run has fresh postings
        # to work from rather than racing the ingestion Lambdas.
        events.Rule(
            self,
            "FitScoringSchedule",
            rule_name="job-applier-fit-scoring-schedule",
            schedule=events.Schedule.cron(minute="30", hour="*/4"),
            targets=[targets.LambdaFunction(self.fit_scoring_fn)],
        )

        cdk.CfnOutput(self, "FitScoringFunctionName", value=self.fit_scoring_fn.function_name)
