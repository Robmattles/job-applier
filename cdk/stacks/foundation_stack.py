"""
Foundation stack — ARCHITECTURE.md §6 phase 1.

Data layer only: the four DynamoDB tables and S3 bucket from §2, plus the
two SQS queues (with DLQs) the pipeline diagram in §1 calls for between
scoring→generation and approval→submission. Deliberately does NOT create
Lambda functions or their IAM roles here — those get added stack-by-stack
in later phases, each granted access to exactly the resources it needs via
CDK's `.grant_*()` methods (e.g. `table.grant_read_write_data(fn)`), which
produces a tighter, auto-scoped policy than hand-writing one up front for
functions that don't exist yet.

All resource names are prefixed `job-applier-` to stay inside what
iam-policy-job-applier-{core,ops}.json actually grants — nothing here
should need broader permissions than what's already attached to the
`job-applier-agent` IAM user (bootstrap excepted; see setup-runbook.md).
"""

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct


class FoundationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------
        # DynamoDB — four tables per ARCHITECTURE.md §2
        # ------------------------------------------------------------------

        # Dedup table. Ingest Lambdas (§1) check every posting against this
        # before it proceeds to fit-scoring. TTL keeps it from growing
        # forever — a posting reappearing after ~120 days is effectively a
        # new opportunity, not a duplicate worth remembering.
        self.postings_table = dynamodb.TableV2(
            self,
            "PostingsTable",
            table_name="job-applier-postings",
            partition_key=dynamodb.Attribute(
                name="posting_id", type=dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

        # Company-discovery table (§1, §3 "name-probing" mechanism). Seeded
        # from target-employer-list.md's 19 verified companies, grown
        # continuously by probing Greenhouse/Lever/Ashby with normalized
        # company names surfaced by any ingestion source. No TTL — a
        # confirmed board token doesn't go stale the way a posting does.
        self.known_companies_table = dynamodb.TableV2(
            self,
            "KnownCompaniesTable",
            table_name="job-applier-known-companies",
            partition_key=dynamodb.Attribute(
                name="company_slug", type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

        # Funnel state machine (§1 Funnel tracker, §5 audit trail
        # guardrail). One row per application, alive for the life of that
        # application (screen/interview/offer/reject), long after any
        # pending-approval TTL below would have expired. GSI on status lets
        # the weekly digest (§6 phase 8) and NEEDS_REVIEW handling query
        # without a full table scan.
        self.applications_table = dynamodb.TableV2(
            self,
            "ApplicationsTable",
            table_name="job-applier-applications",
            partition_key=dynamodb.Attribute(
                name="application_id", type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            # Streams QA (§6 phase 5) off Generation's writes — NEW_IMAGE is
            # enough since QAStack's event source filters on the new
            # status itself, not a before/after diff.
            dynamo_stream=dynamodb.StreamViewType.NEW_IMAGE,
        )
        self.applications_table.add_global_secondary_index(
            index_name="status-index",
            partition_key=dynamodb.Attribute(
                name="status", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at", type=dynamodb.AttributeType.STRING
            ),
        )

        # Pending-approval TTL table (§1 Approval Email Lambda: "expire
        # pending approvals after ~5 days"). Deliberately separate from
        # applications_table above — this one is short-lived bookkeeping
        # for "is Matt's 'ok' reply still outstanding", not the permanent
        # funnel record.
        self.pending_approvals_table = dynamodb.TableV2(
            self,
            "PendingApprovalsTable",
            table_name="job-applier-pending-approvals",
            partition_key=dynamodb.Attribute(
                name="application_id", type=dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

        # ------------------------------------------------------------------
        # S3 — one bucket, prefixes instead of separate buckets (§2:
        # "master résumé/accomplishment inventory ... generated résumé/
        # cover-letter PDFs ... submission confirmation screenshots").
        # Private, encrypted, versioned per the §5 data-hygiene guardrail.
        # ------------------------------------------------------------------
        self.documents_bucket = s3.Bucket(
            self,
            "DocumentsBucket",
            bucket_name=f"job-applier-documents-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        # Expected prefixes (created implicitly on first write, not here):
        #   source/       — master résumé + accomplishment-inventory.json
        #   generated/    — per-application résumé/cover-letter PDFs
        #   screenshots/  — submission confirmation screenshots

        # ------------------------------------------------------------------
        # SQS — the two queues the §1 pipeline diagram calls for, each
        # with a DLQ so a failing message doesn't loop forever silently
        # (§5 guardrail: "CloudWatch alarms on ... DLQ depth").
        # ------------------------------------------------------------------
        self.generation_dlq = sqs.Queue(
            self,
            "GenerationDLQ",
            queue_name="job-applier-generation-dlq",
            retention_period=Duration.days(14),
        )
        self.generation_queue = sqs.Queue(
            self,
            "GenerationQueue",
            queue_name="job-applier-generation-queue",
            visibility_timeout=Duration.minutes(5),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=self.generation_dlq
            ),
        )

        self.submission_dlq = sqs.Queue(
            self,
            "SubmissionDLQ",
            queue_name="job-applier-submission-dlq",
            retention_period=Duration.days(14),
        )
        self.submission_queue = sqs.Queue(
            self,
            "SubmissionQueue",
            queue_name="job-applier-submission-queue",
            # Longer visibility timeout: this queue is drained by the local
            # Submission Worker (ARCHITECTURE.md §3), not a Lambda, and
            # includes human-in-the-loop time (Matt completing a CAPTCHA
            # or NEEDS_REVIEW question) before the message is deleted.
            visibility_timeout=Duration.minutes(30),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=self.submission_dlq
            ),
        )

        # ------------------------------------------------------------------
        # Outputs — so later stacks/phases (and `aws cloudformation
        # describe-stacks`) can find these by name without hardcoding ARNs.
        # ------------------------------------------------------------------
        for name, resource, attr in [
            ("PostingsTableName", self.postings_table, "table_name"),
            ("KnownCompaniesTableName", self.known_companies_table, "table_name"),
            ("ApplicationsTableName", self.applications_table, "table_name"),
            ("PendingApprovalsTableName", self.pending_approvals_table, "table_name"),
            ("DocumentsBucketName", self.documents_bucket, "bucket_name"),
            ("GenerationQueueUrl", self.generation_queue, "queue_url"),
            ("SubmissionQueueUrl", self.submission_queue, "queue_url"),
        ]:
            cdk.CfnOutput(self, name, value=getattr(resource, attr))
