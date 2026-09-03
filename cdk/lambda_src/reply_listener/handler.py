"""job-applier-reply-listener — ARCHITECTURE.md §1 Reply Listener, §6
phase 6.

EventBridge-scheduled poll of Gmail over plain IMAP (§3: App Password,
not OAuth — `gmail.readonly` is a Google "sensitive" scope needing a
2-4 week security assessment and a verified domain, absurd for a
single-user tool). Finds Matt's reply to an approval email, flips the
application to APPROVED, and enqueues it to the submission queue that
the local Playwright worker (§6 phase 7) drains.

Two deliberate constraints on what this reads:

1. It searches for one exact token at a time (`SUBJECT "JA-10e15088"`),
   driven by the rows actually awaiting a decision — never a prefix
   search, never a bare UNSEEN fetch. This mailbox is Matt's real
   personal Gmail. Confirmed live 2026-09-03: an earlier version
   searched `SUBJECT "JA-"` and pulled back J.A. Henckels order
   confirmations, a basketball-highlights newsletter, and a stranger's
   homework thread — Gmail's subject search is token-based, so "JA-"
   matches "J.A.". A full token can't collide with real mail, and the
   search is bounded by what's actually pending rather than by whatever
   happens to be in the inbox.

   It also no longer filters on UNSEEN. Gmail marks your own self-sent
   mail as already-read, and these approval emails go from Matt to
   Matt — so his replies are never unseen, and an UNSEEN filter found
   exactly zero of them. Idempotency comes from the PENDING row instead:
   once a decision is recorded the row leaves PENDING, so re-reading the
   same reply on the next poll is a no-op.

2. Approval is deliberately conservative, because this is the gate in
   front of really submitting an application in Matt's name. A reply
   only counts as approval if the new (unquoted) text is short and
   matches an affirmative with no negation in it. Anything else — a
   question, a long reply, "ok but change X" — leaves the row PENDING
   and gets logged, matching §5's "never invent an answer to an
   ambiguous question." A missed approval costs one re-reply; a wrong
   one sends a real application to a real employer.
"""
import email
import imaplib
import json
import os
import re
import time

import boto3
from boto3.dynamodb.conditions import Attr

from job_applier_common.dynamo_utils import scan_all

IMAP_HOST = os.environ.get("IMAP_HOST", "imap.gmail.com")
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_SECRET_ID = os.environ["GMAIL_SECRET_ID"]
SUBMISSION_QUEUE_URL = os.environ["SUBMISSION_QUEUE_URL"]

_TOKEN_RE = re.compile(r"\[?(JA-[0-9a-f]{8})\]?")
# A quoted original starts here — everything from this line down is the
# email being replied to, not Matt's answer. The "On ... wrote:"
# attribution needs two patterns, not one: Gmail wraps it whenever the
# sender's name and address make it long, so "wrote:" lands on its own
# line and an `On .*wrote:` single-line match misses it entirely.
# Confirmed live 2026-09-03 — that miss let the whole quoted original
# count as reply text, which then failed the length check and turned a
# clean "ok" into AMBIGUOUS. Matching `On ...` + an address covers the
# wrapped form without eating a genuine reply like "On second thought,
# no" (no address, doesn't end in "wrote:").
_QUOTE_START_RE = re.compile(
    r"^\s*(?:"
    r">"
    r"|On\b.*wrote:"
    r"|On\b.*<[^>]+@[^>]+>"
    r"|wrote:\s*$"
    r"|-{3,}\s*Original Message"
    r"|From:\s"
    r")",
    re.IGNORECASE,
)

_APPROVE = {
    "ok", "okay", "k", "yes", "y", "yep", "yeah", "yup", "sure",
    "send", "send it", "submit", "submit it", "approved", "approve",
    "go", "go ahead", "do it", "ship it",
}
_REJECT = {
    "no", "nope", "n", "skip", "skip it", "pass", "reject", "decline",
    "no thanks", "not this one", "drop it",
}
_NEGATION_RE = re.compile(r"\b(not|don'?t|do not|never|hold off|wait|stop)\b", re.IGNORECASE)
# An approval has to be terse. A long reply is a conversation, not a
# yes — treat it as ambiguous rather than fishing for "ok" inside it.
_MAX_APPROVAL_CHARS = 40

_pending_table = None
_applications_table = None
_sqs = None
_secrets = None


def _pending():
    global _pending_table
    if _pending_table is None:
        _pending_table = boto3.resource("dynamodb").Table(os.environ["PENDING_APPROVALS_TABLE"])
    return _pending_table


def _applications():
    global _applications_table
    if _applications_table is None:
        _applications_table = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications_table


def _get_sqs():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs")
    return _sqs


def _gmail_password() -> str:
    global _secrets
    if _secrets is None:
        _secrets = boto3.client("secretsmanager")
    return _secrets.get_secret_value(SecretId=GMAIL_SECRET_ID)["SecretString"].strip()


def _reply_text(msg) -> str:
    """The new text of a reply, with the quoted original stripped."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True) or b""
                body = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                break
    else:
        payload = msg.get_payload(decode=True) or b""
        body = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")

    lines = []
    for line in body.splitlines():
        if _QUOTE_START_RE.match(line):
            break
        lines.append(line)
    return "\n".join(lines).strip()


def classify_reply(text: str) -> str:
    """APPROVED / REJECTED / AMBIGUOUS. Pure function, no I/O — the one
    piece of logic here that decides whether a real application gets
    sent, so it stays independently testable."""
    normalized = text.strip().lower()
    normalized = re.sub(r"[.!,\s]+$", "", normalized)
    if not normalized:
        return "AMBIGUOUS"
    if normalized in _REJECT:
        return "REJECTED"
    if _NEGATION_RE.search(normalized):
        return "AMBIGUOUS"
    if len(normalized) > _MAX_APPROVAL_CHARS:
        return "AMBIGUOUS"
    if normalized in _APPROVE:
        return "APPROVED"
    return "AMBIGUOUS"


def _pending_rows():
    """Every approval still awaiting a decision. Drives the IMAP search,
    rather than the inbox driving us — see module docstring."""
    return scan_all(_pending(), FilterExpression=Attr("status").eq("PENDING"))


def _approve(row: dict):
    application_id = row["application_id"]
    now = int(time.time())
    _pending().update_item(
        Key={"application_id": application_id},
        UpdateExpression="SET #s = :s, decided_at = :ts",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "APPROVED", ":ts": now},
    )
    _applications().update_item(
        Key={"application_id": application_id},
        UpdateExpression="SET #s = :s, approved_at = :ts",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "APPROVED", ":ts": now},
    )
    _get_sqs().send_message(
        QueueUrl=SUBMISSION_QUEUE_URL,
        MessageBody=json.dumps({"application_id": application_id}),
    )
    print(f"APPROVED {application_id} — enqueued for submission")


def _reject(row: dict):
    application_id = row["application_id"]
    now = int(time.time())
    for table in (_pending(), _applications()):
        table.update_item(
            Key={"application_id": application_id},
            UpdateExpression="SET #s = :s, decided_at = :ts",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "REJECTED", ":ts": now},
        )
    print(f"REJECTED {application_id} — will not be submitted")


def _select_all_mail(imap):
    """Select Gmail's All Mail, not INBOX.

    Confirmed live 2026-09-03: replying and then archiving is the normal
    way to handle an email, and archiving removes the INBOX label — an
    INBOX-scoped search would stop finding the reply the moment Matt
    tidied his inbox, leaving the approval to sit PENDING until its TTL
    quietly expired it. All Mail holds everything regardless of labels.
    (Deleted mail is genuinely gone even from All Mail, which is the
    right behavior: deleting an approval request should mean "don't
    submit," and it does.)

    The folder is found by its \\All special-use flag rather than the
    literal "[Gmail]/All Mail", since that name is localized per
    account language."""
    status, boxes = imap.list()
    if status == "OK":
        for raw in boxes or []:
            line = raw.decode(errors="replace")
            if "\\All" in line:
                name = line.split(' "/" ')[-1].strip()
                if imap.select(name, readonly=True)[0] == "OK":
                    return name
    imap.select("INBOX", readonly=True)
    return "INBOX"


def _replies_by_token(imap, days: int = 14) -> dict:
    """token -> newest reply message, for every approval thread Matt has
    answered recently.

    Searched by sender+date, then matched to tokens client-side, rather
    than asking Gmail to find each token. Confirmed live 2026-09-03 that
    Gmail's IMAP SUBJECT search can't be trusted with these tokens at
    all: `SUBJECT "JA-9ad6df41"` matched its thread, while
    `SUBJECT "JA-10e15088"` returned nothing for a message that plainly
    exists — Gmail indexes `10e15088` as a number rather than a word, so
    approvals would silently never be seen. `FROM <matt>` is an exact
    header match with no tokenization involved, and it also keeps the
    fetch to Matt's own self-addressed mail rather than anything a third
    party sent him."""
    since = time.strftime("%d-%b-%Y", time.gmtime(time.time() - days * 86400))
    status, data = imap.search(None, f'(SINCE {since} FROM "{GMAIL_ADDRESS}")')
    if status != "OK" or not data or not data[0]:
        return {}

    found = {}
    for num in data[0].split():
        # Headers first — the body is only worth pulling for a message
        # that actually turns out to be a reply on one of our threads.
        status, hdr_data = imap.fetch(num, "(BODY.PEEK[HEADER.FIELDS (SUBJECT IN-REPLY-TO)])")
        if status != "OK" or not hdr_data or not hdr_data[0]:
            continue
        hdr = email.message_from_bytes(hdr_data[0][1])
        if not hdr.get("In-Reply-To"):
            continue  # an approval email we sent, not an answer to one
        subject = str(email.header.make_header(email.header.decode_header(hdr.get("Subject", ""))))
        match = _TOKEN_RE.search(subject)
        if not match:
            continue

        status, msg_data = imap.fetch(num, "(RFC822)")
        if status != "OK" or not msg_data or not msg_data[0]:
            continue
        # Ascending UID order, so a later hit is the newer reply.
        found[match.group(1)] = email.message_from_bytes(msg_data[0][1])
    return found


def handler(event, context):
    stats = {"approved": 0, "rejected": 0, "ambiguous": 0, "awaiting": 0}

    pending = _pending_rows()
    if not pending:
        print("no approvals awaiting a decision")
        return stats

    imap = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        imap.login(GMAIL_ADDRESS, _gmail_password())
        mailbox = _select_all_mail(imap)
        print(f"searching {mailbox}")
        replies = _replies_by_token(imap)
    finally:
        try:
            imap.logout()
        except Exception:  # noqa: BLE001 — logout failure shouldn't mask a real error
            pass

    for row in pending:
        token = row.get("token")
        reply = replies.get(token) if token else None
        if reply is None:
            stats["awaiting"] += 1
            continue

        verdict = classify_reply(_reply_text(reply))
        if verdict == "APPROVED":
            _approve(row)
            stats["approved"] += 1
        elif verdict == "REJECTED":
            _reject(row)
            stats["rejected"] += 1
        else:
            print(f"AMBIGUOUS reply for {token} ({row['application_id']}) — left PENDING")
            stats["ambiguous"] += 1

    print(f"job-applier-reply-listener stats: {stats}")
    return stats
