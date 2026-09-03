"""Generation stack — ARCHITECTURE.md §6 phase 4.

SQS-triggered off FoundationStack's generation_queue (fit-scoring's
_pass2_gate sends one message per QUALIFIED promotion) rather than a
scheduled poll — generation only ever has work when scoring produces it,
so there's nothing for a schedule to usefully rate-limit here; the
weekly ramp cap (§5) already happened upstream, in scoring.

Uses a higher-quality model than scoring per §1 ("Bedrock (higher-quality
model, e.g. Claude Opus) runs the evidence audit... for the chosen
lane") — affordable specifically because generation only ever runs
against the WEEKLY_CAP-limited QUALIFIED trickle (≤10-50/week per §5's
ramp), not the whole scoring backlog.
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct


class GenerationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        postings_table: dynamodb.ITableV2,
        applications_table: dynamodb.ITableV2,
        documents_bucket: s3.IBucket,
        generation_queue: sqs.IQueue,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common-generation",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )

        self.generation_fn = lambda_.Function(
            self,
            "Generation",
            function_name="job-applier-generation",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/generation"),
            layers=[common_layer],
            environment={
                "POSTINGS_TABLE": postings_table.table_name,
                "APPLICATIONS_TABLE": applications_table.table_name,
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
                # §1's "higher-quality model" for generation, distinct from
                # scoring's cheap-model choice — see module docstring for
                # why the cost trade-off is fine at this stage's volume.
                "BEDROCK_MODEL_ID": "us.anthropic.claude-opus-4-5-20251101-v1:0",
            },
            timeout=Duration.minutes(5),
            memory_size=512,
        )
        postings_table.grant_read_data(self.generation_fn)
        applications_table.grant_write_data(self.generation_fn)
        documents_bucket.grant_read(self.generation_fn)
        self.generation_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    "arn:aws:bedrock:*::foundation-model/anthropic.claude*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/*claude*",
                ],
            )
        )
        # Same one-time-per-model AWS Marketplace subscription gate hit and
        # fixed for scoring's Sonnet 4.5 (ScoringStack) — Opus 4.5 needs its
        # own grant on this Lambda's own execution role.
        self.generation_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["aws-marketplace:ViewSubscriptions", "aws-marketplace:Subscribe"],
                resources=["*"],
            )
        )

        self.generation_fn.add_event_source(
            lambda_events.SqsEventSource(
                generation_queue,
                batch_size=1,  # handler.py relies on this — see its own
                # comment: lets a real error propagate per-message instead
                # of a batch's worth of unrelated postings failing together.
                # Confirmed live 2026-09-02: this account's total Lambda
                # concurrency ceiling is just 10 (UnreservedConcurrentExecution
                # floor rejected any reserved_concurrent_executions at all —
                # see ScoringStack's comment) — a single weekly-cap-sized
                # batch of QUALIFIED promotions scaling this event source
                # unbounded starved a manual scoring invoke of concurrency
                # entirely (ConcurrentInvocationLimitExceeded). Capped at the
                # minimum AWS allows so Generation alone can never exhaust
                # the account's whole pool; SQS just queues the rest.
                max_concurrency=2,
            )
        )

        cdk.CfnOutput(self, "GenerationFunctionName", value=self.generation_fn.function_name)
