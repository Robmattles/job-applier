"""QA stack — ARCHITECTURE.md §6 phase 5 (LLM half; rendering is the
other half, not built here).

DynamoDB-Streams-triggered off applications_table (FoundationStack),
filtered to NEW_IMAGE.status == "GENERATED" so only what Generation just
wrote wakes this up — every status this Lambda itself writes back
(QA_PASSED, NEEDS_REVIEW) fails that filter, so there's no self-trigger
loop to guard against separately.

Own DLQ (unlike Generation's SQS-native one) because a DynamoDB Streams
event source retries a failing record against the *shard* indefinitely
by default — capping retry_attempts and giving it a destination is the
difference between one bad record blocking every application behind it
on the same shard versus failing cleanly and moving on.
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


class QAStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        postings_table: dynamodb.ITableV2,
        applications_table: dynamodb.ITableV2,
        documents_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.qa_dlq = sqs.Queue(
            self,
            "QADLQ",
            queue_name="job-applier-qa-dlq",
            retention_period=Duration.days(14),
        )

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common-qa",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )

        self.qa_fn = lambda_.Function(
            self,
            "QA",
            function_name="job-applier-qa",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/qa"),
            layers=[common_layer],
            environment={
                "POSTINGS_TABLE": postings_table.table_name,
                "APPLICATIONS_TABLE": applications_table.table_name,
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
                # Same higher-quality model as Generation (GenerationStack)
                # — a critic call is exactly as consequential as the draft
                # it's reviewing, and volume here is the same weekly-capped
                # trickle, so there's no cost case for a cheaper model.
                "BEDROCK_MODEL_ID": "us.anthropic.claude-opus-4-5-20251101-v1:0",
                "MAX_REVISION_LOOPS": "2",  # §4 guardrail
            },
            # Worst case per §4's 2-revision-loop cap: ~3 Bedrock calls/loop
            # (pass A, pass B, a targeted revise) — generous headroom for
            # Opus's latency on ~4000-token responses across up to 3 loops.
            timeout=Duration.minutes(10),
            memory_size=512,
        )
        postings_table.grant_read_data(self.qa_fn)
        applications_table.grant_write_data(self.qa_fn)
        documents_bucket.grant_read(self.qa_fn)
        self.qa_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    "arn:aws:bedrock:*::foundation-model/anthropic.claude*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/*claude*",
                ],
            )
        )
        # Same one-time-per-model AWS Marketplace subscription gate hit and
        # fixed for scoring (Sonnet 4.5) and generation (Opus 4.5) — this
        # Lambda has its own execution role, so its own grant.
        self.qa_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["aws-marketplace:ViewSubscriptions", "aws-marketplace:Subscribe"],
                resources=["*"],
            )
        )

        self.qa_fn.add_event_source(
            lambda_events.DynamoEventSource(
                applications_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=1,
                # Default is already 1 — explicit for the same reason
                # GenerationStack caps its own event source's concurrency
                # (see its comment): this account's total Lambda
                # concurrency ceiling is just 10, confirmed live
                # 2026-09-02, so nothing that fans out on its own gets to
                # assume it has room to.
                parallelization_factor=1,
                retry_attempts=3,
                on_failure=lambda_events.SqsDlq(self.qa_dlq),
                filters=[
                    lambda_.FilterCriteria.filter(
                        {"dynamodb": {"NewImage": {"status": {"S": lambda_.FilterRule.is_equal("GENERATED")}}}}
                    )
                ],
            )
        )

        cdk.CfnOutput(self, "QAFunctionName", value=self.qa_fn.function_name)
