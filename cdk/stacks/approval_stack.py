"""Approval stack — ARCHITECTURE.md §6 phase 6.

Two Lambdas, deliberately split by trigger rather than bundled:

- `job-applier-approval-email` is DynamoDB-Streams-triggered off
  applications_table (status == "RENDERED"), so an email goes out the
  moment a document set is ready, with no polling.
- `job-applier-reply-listener` is EventBridge-scheduled, because there
  is no push channel for "Matt replied to an email" — IMAP has to be
  polled (§3: App Password + plain IMAP, not the Gmail API).

This is the human gate the whole pipeline exists to feed: everything
upstream is reversible bookkeeping, everything downstream (§6 phase 7)
sends real applications to real employers under Matt's name.
"""
import os
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
)
from constructs import Construct

APPROVAL_EMAIL = os.environ.get("JOB_APPLIER_EMAIL", "you@example.com")
GMAIL_SECRET_NAME = "job-applier-gmail-app-password"


class ApprovalStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        postings_table: dynamodb.ITableV2,
        applications_table: dynamodb.ITableV2,
        pending_approvals_table: dynamodb.ITableV2,
        documents_bucket: s3.IBucket,
        submission_queue: sqs.IQueue,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.approval_dlq = sqs.Queue(
            self,
            "ApprovalDLQ",
            queue_name="job-applier-approval-dlq",
            retention_period=Duration.days(14),
        )

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="job-applier-common-approval",
            code=lambda_.Code.from_asset("lambda_src/common_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )

        # ------------------------------------------------------------------
        # Approval email — stream-triggered on RENDERED
        # ------------------------------------------------------------------
        self.approval_email_fn = lambda_.Function(
            self,
            "ApprovalEmail",
            function_name="job-applier-approval-email",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/approval_email"),
            layers=[common_layer],
            environment={
                "POSTINGS_TABLE": postings_table.table_name,
                "APPLICATIONS_TABLE": applications_table.table_name,
                "PENDING_APPROVALS_TABLE": pending_approvals_table.table_name,
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
                "APPROVAL_TO_EMAIL": APPROVAL_EMAIL,
                # Same address both ways: SES is in sandbox, which requires
                # sender and recipient to be verified identities, and Matt
                # emailing himself needs exactly one verification.
                "APPROVAL_FROM_EMAIL": APPROVAL_EMAIL,
                "PENDING_APPROVAL_TTL_DAYS": "5",  # §1: postings go stale
            },
            timeout=Duration.minutes(2),
            memory_size=512,
        )
        postings_table.grant_read_data(self.approval_email_fn)
        applications_table.grant_write_data(self.approval_email_fn)
        pending_approvals_table.grant_read_write_data(self.approval_email_fn)
        documents_bucket.grant_read(self.approval_email_fn)
        self.approval_email_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ses:SendRawEmail", "ses:SendEmail"],
                resources=["*"],  # SES identity ARNs aren't known at synth time
            )
        )
        self.approval_email_fn.add_event_source(
            lambda_events.DynamoEventSource(
                applications_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=1,
                parallelization_factor=1,
                retry_attempts=3,
                on_failure=lambda_events.SqsDlq(self.approval_dlq),
                filters=[
                    lambda_.FilterCriteria.filter(
                        {"dynamodb": {"NewImage": {"status": {"S": lambda_.FilterRule.is_equal("RENDERED")}}}}
                    )
                ],
            )
        )

        # ------------------------------------------------------------------
        # Reply listener — scheduled IMAP poll
        # ------------------------------------------------------------------
        gmail_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "GmailSecret", GMAIL_SECRET_NAME
        )

        self.reply_listener_fn = lambda_.Function(
            self,
            "ReplyListener",
            function_name="job-applier-reply-listener",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda_src/reply_listener"),
            layers=[common_layer],
            environment={
                "APPLICATIONS_TABLE": applications_table.table_name,
                "PENDING_APPROVALS_TABLE": pending_approvals_table.table_name,
                "SUBMISSION_QUEUE_URL": submission_queue.queue_url,
                "GMAIL_ADDRESS": APPROVAL_EMAIL,
                "GMAIL_SECRET_ID": GMAIL_SECRET_NAME,
                # §5 kill switch only (config/ramp.json). This Lambda is
                # the gate in front of the submission queue — a pause has
                # to stop approvals landing on it, not just stop the
                # worker draining it.
                "DOCUMENTS_BUCKET": documents_bucket.bucket_name,
            },
            timeout=Duration.minutes(2),
            memory_size=256,
        )
        applications_table.grant_write_data(self.reply_listener_fn)
        pending_approvals_table.grant_read_write_data(self.reply_listener_fn)
        submission_queue.grant_send_messages(self.reply_listener_fn)
        gmail_secret.grant_read(self.reply_listener_fn)
        documents_bucket.grant_read(self.reply_listener_fn, "config/*")

        # A backstop, not the fast path — see submission_worker/watcher.py,
        # which now polls Gmail locally every ~15s and is always faster
        # while it's running (Matt's ask: "worst case latency... needs to
        # be max 30 seconds," which no EventBridge schedule reaches on
        # its own; 1 minute is the platform floor for a rate expression).
        # This exists for when the watcher isn't — laptop closed, crashed,
        # not yet installed — so a reply is never stuck for longer than 5
        # minutes even in that case. Both write through the same
        # idempotent classify_reply gate, so running both is never unsafe.
        events.Rule(
            self,
            "ReplyListenerSchedule",
            rule_name="job-applier-reply-listener-schedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            targets=[targets.LambdaFunction(self.reply_listener_fn)],
        )

        cdk.CfnOutput(self, "ApprovalEmailFunctionName", value=self.approval_email_fn.function_name)
        cdk.CfnOutput(self, "ReplyListenerFunctionName", value=self.reply_listener_fn.function_name)
