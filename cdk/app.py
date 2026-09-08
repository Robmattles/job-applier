#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.approval_stack import ApprovalStack
from stacks.foundation_stack import FoundationStack
from stacks.generation_stack import GenerationStack
from stacks.ingestion_stack import IngestionStack
from stacks.ops_stack import OpsStack
from stacks.qa_stack import QAStack
from stacks.render_stack import RenderStack
from stacks.scoring_stack import ScoringStack

app = cdk.App()

# Explicit account/region rather than relying on ambient profile env vars —
# Account/region come from .env.local (see .env.example) rather than
# being hardcoded, so this repo can be public without naming one
# person's live infrastructure.
env = cdk.Environment(
    account=os.environ.get("JOB_APPLIER_ACCOUNT_ID") or os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ.get("JOB_APPLIER_REGION", "us-east-1"),
)

foundation = FoundationStack(
    app,
    "job-applier-foundation",
    env=env,
    description="job-applier: DynamoDB tables, S3 document bucket, SQS queues (ARCHITECTURE.md §6 phase 1)",
)

ingestion = IngestionStack(
    app,
    "job-applier-ingestion",
    env=env,
    postings_table=foundation.postings_table,
    known_companies_table=foundation.known_companies_table,
    documents_bucket=foundation.documents_bucket,
    description="job-applier: ingestion Lambdas — direct ATS polling + secondary boards + discovery (ARCHITECTURE.md §6 phase 2)",
)
ingestion.add_dependency(foundation)

scoring = ScoringStack(
    app,
    "job-applier-scoring",
    env=env,
    postings_table=foundation.postings_table,
    known_companies_table=foundation.known_companies_table,
    documents_bucket=foundation.documents_bucket,
    generation_queue=foundation.generation_queue,
    description="job-applier: fit-scoring Lambda — Bedrock evidence audit + threshold/cap gate (ARCHITECTURE.md §6 phase 3)",
)
scoring.add_dependency(foundation)

generation = GenerationStack(
    app,
    "job-applier-generation",
    env=env,
    postings_table=foundation.postings_table,
    applications_table=foundation.applications_table,
    documents_bucket=foundation.documents_bucket,
    generation_queue=foundation.generation_queue,
    description="job-applier: generation Lambda — lane-specific résumé/cover-letter content (ARCHITECTURE.md §6 phase 4)",
)
generation.add_dependency(foundation)

qa = QAStack(
    app,
    "job-applier-qa",
    env=env,
    postings_table=foundation.postings_table,
    applications_table=foundation.applications_table,
    documents_bucket=foundation.documents_bucket,
    description="job-applier: QA Lambda — authenticity/grounding/specificity + recruiter/ATS adversarial passes (ARCHITECTURE.md §6 phase 5)",
)
qa.add_dependency(foundation)

render = RenderStack(
    app,
    "job-applier-render",
    env=env,
    postings_table=foundation.postings_table,
    applications_table=foundation.applications_table,
    documents_bucket=foundation.documents_bucket,
    description="job-applier: render Lambda — fixed-template résumé/cover-letter PDF (ARCHITECTURE.md §6 phase 5)",
)
render.add_dependency(foundation)

approval = ApprovalStack(
    app,
    "job-applier-approval",
    env=env,
    postings_table=foundation.postings_table,
    applications_table=foundation.applications_table,
    pending_approvals_table=foundation.pending_approvals_table,
    documents_bucket=foundation.documents_bucket,
    submission_queue=foundation.submission_queue,
    description="job-applier: approval email (SES) + Gmail IMAP reply listener (ARCHITECTURE.md §6 phase 6)",
)
approval.add_dependency(foundation)

ops = OpsStack(
    app,
    "job-applier-ops",
    env=env,
    postings_table=foundation.postings_table,
    applications_table=foundation.applications_table,
    documents_bucket=foundation.documents_bucket,
    description="job-applier: sweeper + weekly digest + billing alarm (ARCHITECTURE.md §6 phases 8-9)",
)
ops.add_dependency(foundation)

cdk.Tags.of(app).add("Project", "job-applier")

app.synth()
