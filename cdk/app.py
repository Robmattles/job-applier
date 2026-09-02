#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.foundation_stack import FoundationStack

app = cdk.App()

# Explicit account/region rather than relying on ambient profile env vars —
# ARCHITECTURE.md settled on account ACCOUNT_ID, region us-east-1.
env = cdk.Environment(account="ACCOUNT_ID", region="us-east-1")

FoundationStack(
    app,
    "job-applier-foundation",
    env=env,
    description="job-applier: DynamoDB tables, S3 document bucket, SQS queues (ARCHITECTURE.md §6 phase 1)",
)

cdk.Tags.of(app).add("Project", "job-applier")

app.synth()
