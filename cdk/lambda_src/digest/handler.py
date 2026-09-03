"""job-applier-digest — ARCHITECTURE.md §6 phase 8, §1 Funnel tracker
("weekly digest email of funnel stats").

The point isn't the counts, it's calibration. §5 says the weekly cap
only climbs "as far as real data says there are that many genuinely good
matches" — this is that data. A week where most applications stalled in
NEEDS_REVIEW, or where the fit scores that got through cluster at the
threshold, is a week not to raise the cap.
"""
import os
import time
from collections import Counter

import boto3
from boto3.dynamodb.conditions import Attr

from job_applier_common.dynamo_utils import scan_all

APPROVAL_TO = os.environ["APPROVAL_TO_EMAIL"]
APPROVAL_FROM = os.environ["APPROVAL_FROM_EMAIL"]

_applications = None
_postings = None
_ses = None


def _apps():
    global _applications
    if _applications is None:
        _applications = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications


def _postings_table():
    global _postings
    if _postings is None:
        _postings = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _postings


def _get_ses():
    global _ses
    if _ses is None:
        _ses = boto3.client("ses")
    return _ses


def handler(event, context):
    now = int(time.time())
    week_ago = now - 7 * 86400

    applications = scan_all(_apps())
    recent = [a for a in applications if int(a.get("created_at_epoch", a.get("generated_at", 0)) or 0) >= week_ago]

    by_status = Counter(a.get("status", "?") for a in applications)
    submitted = [a for a in applications if a.get("status") == "SUBMITTED"]

    posting_states = Counter(
        p.get("status", "?") for p in scan_all(
            _postings_table(), ProjectionExpression="#s", ExpressionAttributeNames={"#s": "status"}
        )
    )

    lines = [
        "job-applier — weekly funnel",
        "=" * 40,
        "",
        f"Postings in the pipeline: {sum(posting_states.values())}",
    ]
    for status, count in posting_states.most_common():
        lines.append(f"  {count:5d}  {status}")

    lines += ["", "Applications by stage:"]
    for status, count in by_status.most_common():
        lines.append(f"  {count:5d}  {status}")

    lines += ["", f"Generated in the last 7 days: {len(recent)}", ""]

    if submitted:
        lines.append("Submitted:")
        for a in submitted:
            lines.append(f"  {a.get('company_name','')} — {a.get('title','')}")
        lines.append("")

    stuck = by_status.get("NEEDS_REVIEW", 0)
    if stuck:
        lines.append(f"{stuck} in NEEDS_REVIEW awaiting your call.")

    # §5 names two specific criteria for raising the cap — "only if the
    # false-positive rate and QA-failure rate are actually low" — so
    # report those two directly rather than leaving them to be inferred
    # from a pile of counts. The ramp is manual on purpose; the point of
    # this section is to make the manual decision answerable in ten
    # seconds instead of requiring a DynamoDB spelunk.
    approved = by_status.get("APPROVED", 0) + by_status.get("SUBMITTED", 0)
    pending = by_status.get("PENDING_APPROVAL", 0)
    rejected = by_status.get("REJECTED", 0)
    needs_review = by_status.get("NEEDS_REVIEW", 0)
    decided = approved + rejected
    reached_qa = sum(
        by_status.get(s, 0)
        for s in ("QA_PASSED", "RENDERED", "PENDING_APPROVAL", "APPROVED", "SUBMITTED",
                  "NOT_SUBMITTED", "REJECTED", "NEEDS_REVIEW")
    )

    def pct(numerator, denominator):
        return f"{100 * numerator / denominator:.0f}%" if denominator else "n/a"

    lines += [
        "",
        "-" * 40,
        "Should the weekly cap go up? (§5 says only if both of these are low)",
        "",
        f"  QA-failure rate:     {pct(needs_review, reached_qa)}"
        f"   ({needs_review} of {reached_qa} reaching QA landed in NEEDS_REVIEW)",
        f"  False-positive rate: {pct(rejected, decided)}"
        f"   ({rejected} of {decided} you decided on were 'no')",
        f"  Still awaiting you:  {pending}",
        "",
        f"Current cap: {os.environ.get('WEEKLY_CAP_DISPLAY', 'see config/ramp.json')}",
        "Ramp: week 1: 5-10  ->  week 2: 15-25  ->  weeks 3-4: 30-50  ->  toward 100.",
        "Each step is gated on those rates, not on time elapsed — a week where",
        "most applications stalled in NEEDS_REVIEW is a week to leave it alone.",
        "",
        "To change it (takes effect on the next scoring run, no deploy):",
        "  aws s3 cp s3://job-applier-documents-ACCOUNT_ID-us-east-1/config/ramp.json - \\",
        "    --profile job-applier | sed 's/\"weekly_cap\": [0-9]*/\"weekly_cap\": 15/' | \\",
        "    aws s3 cp - s3://job-applier-documents-ACCOUNT_ID-us-east-1/config/ramp.json \\",
        "    --profile job-applier",
    ]

    _get_ses().send_email(
        Source=APPROVAL_FROM,
        Destination={"ToAddresses": [APPROVAL_TO]},
        Message={
            "Subject": {"Data": "job-applier weekly funnel"},
            "Body": {"Text": {"Data": "\n".join(lines)}},
        },
    )
    print("digest sent")
    return {"sent": 1, "applications": len(applications)}
