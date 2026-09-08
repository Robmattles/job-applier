"""Reads .env.local so worker.py and watcher.py aren't hardcoded to one
person's AWS account and mailbox.

Lambdas don't use this — CDK injects their values as environment
variables at deploy time. This is only for the two processes that run on
Matt's own machine, which launchd starts without a shell to source
anything.
"""
import os
import pathlib

_ENV = pathlib.Path(__file__).resolve().parent.parent / ".env.local"


def _load():
    if not _ENV.exists():
        return
    for line in _ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load()

ACCOUNT_ID = os.environ.get("JOB_APPLIER_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("JOB_APPLIER_REGION", "us-east-1")
EMAIL = os.environ.get("JOB_APPLIER_EMAIL", "you@example.com")
DOCUMENTS_BUCKET = f"job-applier-documents-{ACCOUNT_ID}-{REGION}"
SUBMISSION_QUEUE_URL = (
    f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/job-applier-submission-queue"
)
