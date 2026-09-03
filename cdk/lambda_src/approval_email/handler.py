"""job-applier-approval-email — ARCHITECTURE.md §1 Approval Email Lambda,
§6 phase 6.

DynamoDB-Streams-triggered off applications_table, filtered to
NEW_IMAGE.status == "RENDERED" — the tail of the content pipeline. Sends
Matt exactly one email per posting (§1: "sends ONE email per posting"),
writes a PENDING row with a ~5-day TTL (§1: "job postings go stale"),
and moves the application to PENDING_APPROVAL so this Lambda's own write
can't re-trigger it (the stream filter only matches RENDERED).

The PDFs are MIME attachments, not S3 links. A presigned URL is the
obvious alternative and the wrong one here: presigned URLs signed with a
Lambda execution role's temporary credentials stop working when those
credentials expire — hours, well short of the 5-day approval window —
so the link would be dead by the time Matt got to a queued email. The
documents are ~5KB each, far inside SES's 10MB raw-message limit.

Reply matching: each email carries a short token in its subject
(`[JA-xxxxxxxx]`). Gmail preserves the subject line on reply, so the
Reply Listener can match "Re: ... [JA-xxxxxxxx] ..." back to this exact
application without needing per-application reply-to addresses.
"""
import os
import secrets
import time
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.types import TypeDeserializer

from job_applier_common import inventory_store
from job_applier_common.dynamo_utils import scan_all

APPROVAL_TO = os.environ["APPROVAL_TO_EMAIL"]
APPROVAL_FROM = os.environ["APPROVAL_FROM_EMAIL"]
TTL_DAYS = int(os.environ.get("PENDING_APPROVAL_TTL_DAYS", "5"))
DOCUMENTS_BUCKET = os.environ["DOCUMENTS_BUCKET"]

_deserializer = TypeDeserializer()
_applications_table = None
_pending_table = None
_postings_table = None
_s3 = None
_ses = None


def _applications():
    global _applications_table
    if _applications_table is None:
        _applications_table = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications_table


def _pending():
    global _pending_table
    if _pending_table is None:
        _pending_table = boto3.resource("dynamodb").Table(os.environ["PENDING_APPROVALS_TABLE"])
    return _pending_table


def _postings():
    global _postings_table
    if _postings_table is None:
        _postings_table = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _postings_table


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _get_ses():
    global _ses
    if _ses is None:
        _ses = boto3.client("ses")
    return _ses


def _emails_sent_since(epoch: int) -> int:
    """Approval emails actually sent in the window, from the
    pending-approvals table — counted regardless of how each was later
    decided, since an email Matt rejected still cost him the review."""
    rows = scan_all(_pending(), FilterExpression=Attr("sent_at").gte(epoch))
    return len(rows)


def _deserialize(image: dict) -> dict:
    return {k: _deserializer.deserialize(v) for k, v in image.items()}


def _fetch_pdf(key: str) -> bytes:
    return _get_s3().get_object(Bucket=DOCUMENTS_BUCKET, Key=key)["Body"].read()


def _build_body(app: dict, posting: dict, token: str) -> str:
    """Everything Matt needs to decide without opening anything else —
    plus the honest negatives. §4's recruiter pass already produced
    reasons_to_reject; burying those would defeat the point of having a
    human approval gate at all."""
    lines = [
        f"{app.get('company_name', '')} — {app.get('title', '')}",
        "",
        f"Fit score: {app.get('fit_score', '?')}   Lane: {app.get('lane', '')}",
        f"Posting: {app.get('url', '')}",
        "",
    ]

    reasons_for = posting.get("reasons_to_interview") or []
    if reasons_for:
        lines.append("Why this could be a fit:")
        lines += [f"  - {r}" for r in reasons_for[:5]]
        lines.append("")

    reasons_against = posting.get("reasons_to_reject") or []
    if reasons_against:
        lines.append("Where it's weak:")
        lines += [f"  - {r}" for r in reasons_against[:3]]
        lines.append("")

    lines += [
        "Résumé and cover letter are attached.",
        "",
        "-----------------------------------------------------------",
        "Reply \"ok\" to this email to submit the application.",
        "Reply \"no\" to skip it.",
        f"Anything else, or no reply within {TTL_DAYS} days, and nothing happens.",
        "-----------------------------------------------------------",
        "",
        f"(ref {token})",
    ]
    return "\n".join(lines)


def _process_one(application_id: str, app: dict) -> str:
    posting = _postings().get_item(Key={"posting_id": app.get("posting_id", "")}).get("Item") or {}

    # The weekly cap (§5) lives on the posting, not the application. A
    # posting displaced out of QUALIFIED by the re-rank is no longer part
    # of this week's batch, and an application built from it must not
    # reach Matt — confirmed live 2026-09-03, the phase-9 sweeper
    # unstuck 13 stranded applications including several already
    # displaced, which without this guard would have emailed straight
    # past the cap that exists to keep the ramp deliberate.
    if posting.get("status") != "QUALIFIED":
        print(
            f"skipping {application_id}: posting is {posting.get('status')}, not QUALIFIED "
            "— displaced from this week's batch"
        )
        return "skipped_not_qualified"

    # Second, independent check: how many emails have actually gone out
    # this week. Confirmed live 2026-09-03 — Matt asked why he had 11
    # approval emails when the cap is 10, and the answer was that the cap
    # was only ever bounding *concurrent QUALIFIED postings*, not emails.
    # The QUALIFIED set churns: every re-rank swaps a better posting in,
    # and the displaced one has already emailed him. So cumulative emails
    # drift past the cap with nothing noticing. §5's actual intent is
    # "week 1: 5-10, inspect everything manually" — a statement about how
    # much review lands on him, which is emails, so that's what's counted
    # here.
    sent_this_week = _emails_sent_since(int(time.time()) - 7 * 86400)
    cap = inventory_store.load_weekly_cap(int(os.environ.get("WEEKLY_CAP", "10")))
    if sent_this_week >= cap:
        print(
            f"skipping {application_id}: {sent_this_week} approval emails already sent in the "
            f"last 7 days, at or over the cap of {cap}"
        )
        return "skipped_weekly_cap"

    token = f"JA-{secrets.token_hex(4)}"
    subject = f"[{token}] Apply to {app.get('company_name', '')} — {app.get('title', '')}?"

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = APPROVAL_FROM
    msg["To"] = APPROVAL_TO
    msg.attach(MIMEText(_build_body(app, posting, token), "plain", "utf-8"))

    for key, filename in [
        (app.get("resume_pdf_key"), "resume.pdf"),
        (app.get("cover_letter_pdf_key"), "cover_letter.pdf"),
    ]:
        if not key:
            continue
        part = MIMEApplication(_fetch_pdf(key), _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(part)

    _get_ses().send_raw_email(RawMessage={"Data": msg.as_string()})

    now = int(time.time())
    _pending().put_item(
        Item={
            "application_id": application_id,
            "token": token,
            "subject": subject,
            "company_name": app.get("company_name", ""),
            "title": app.get("title", ""),
            "status": "PENDING",
            "sent_at": now,
            "ttl": now + TTL_DAYS * 86400,
        }
    )
    _applications().update_item(
        Key={"application_id": application_id},
        UpdateExpression="SET #s = :s, approval_token = :t, approval_sent_at = :ts",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "PENDING_APPROVAL", ":t": token, ":ts": now},
    )
    print(f"approval email sent for {application_id} token={token}")
    return "sent"


def handler(event, context):
    stats = {"sent": 0, "skipped_not_qualified": 0, "skipped_weekly_cap": 0}
    for record in event.get("Records", []):
        image = record.get("dynamodb", {}).get("NewImage")
        if not image:
            continue
        app = _deserialize(image)
        outcome = _process_one(app["application_id"], app)
        stats[outcome] = stats.get(outcome, 0) + 1
    print(f"job-applier-approval-email stats: {stats}")
    return stats
