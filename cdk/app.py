#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.foundation_stack import FoundationStack
from stacks.ingestion_stack import IngestionStack

app = cdk.App()

# Explicit account/region rather than relying on ambient profile env vars —
# ARCHITECTURE.md settled on account ACCOUNT_ID, region us-east-1.
env = cdk.Environment(account="ACCOUNT_ID", region="us-east-1")

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
    description="job-applier: ingestion Lambdas — direct ATS polling + secondary boards + discovery (ARCHITECTURE.md §6 phase 2)",
)
ingestion.add_dependency(foundation)

cdk.Tags.of(app).add("Project", "job-applier")

app.synth()
