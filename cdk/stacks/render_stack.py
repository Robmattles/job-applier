"""Render stack — ARCHITECTURE.md §6 phase 5's rendering half (QAStack is
the other half).

DynamoDB-Streams-triggered off applications_table, filtered to
NEW_IMAGE.status == "QA_PASSED" — the tail end of the content pipeline,
right before Approval (§6 phase 6, not built yet) sends the link Matt
approves. No Bedrock call at all: rendering is deliberately deterministic
(§1: "the one fixed... single-column template"), so this is the one
content-pipeline Lambda with no model IAM grant and no per-model
Marketplace subscription gate to hit.
"""
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct


class RenderStack(Stack):
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

        self.render_dlq = sqs.Queue(
            self,
            "RenderDLQ",
            queue_name="job-applier-render-dlq",
            retention_period=Duration.days(14),
        )

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common-render",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )
        # fpdf2 + fonttools/Pillow/defusedxml, vendored as manylinux2014
        # x86_64 / cp313 wheels (see lambda_src/render_layer — pip install
        # --platform manylinux2014_x86_64 --python-version 3.13
        # --only-binary=:all:, since this account's Lambdas default to
        # x86_64 and a Mac-native `pip install` grabs macOS binaries that
        # silently fail to import on Lambda's Linux runtime).
        pdf_layer = lambda_.LayerVersion(
            self,
            "PdfLayer",
            layer_version_name="job-applier-pdf-libs",
            code=lambda_.Code.from_asset("lambda_src/render_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            compatible_architectures=[lambda_.Architecture.X86_64],
        )

        self.render_fn = lambda_.Function(
            self,
            "Render",
            function_name="job-applier-render",
            runtime=lambda_.Runtime.PYTHON_3_13,
            architecture=lambda_.Architecture.X86_64,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/render"),
            layers=[common_layer, pdf_layer],
            environment={
                "POSTINGS_TABLE": postings_table.table_name,
                "APPLICATIONS_TABLE": applications_table.table_name,
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
            },
            timeout=Duration.minutes(2),  # deterministic rendering, no
            # Bedrock round-trips — this should never be slow
            memory_size=512,
        )
        postings_table.grant_read_data(self.render_fn)
        applications_table.grant_write_data(self.render_fn)
        documents_bucket.grant_read_write(self.render_fn)  # write: the
        # rendered PDFs themselves, under generated/ (FoundationStack's
        # documented prefix)

        self.render_fn.add_event_source(
            lambda_events.DynamoEventSource(
                applications_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=1,
                parallelization_factor=1,  # same account-wide concurrency
                # ceiling reasoning as QAStack/GenerationStack
                retry_attempts=3,
                on_failure=lambda_events.SqsDlq(self.render_dlq),
                filters=[
                    lambda_.FilterCriteria.filter(
                        {"dynamodb": {"NewImage": {"status": {"S": lambda_.FilterRule.is_equal("QA_PASSED")}}}}
                    )
                ],
            )
        )

        cdk.CfnOutput(self, "RenderFunctionName", value=self.render_fn.function_name)
