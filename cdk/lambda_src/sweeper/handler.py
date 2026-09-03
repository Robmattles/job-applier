"""job-applier-sweeper — ARCHITECTURE.md §6 phase 9 (guardrails).

Two jobs, both closing gaps found by running the pipeline for real:

1. UNSTICK. Every stage after generation is DynamoDB-Streams-triggered
   with `starting_position=LATEST`, which means a row that reached a
   trigger state *before* that stage existed — or during any window
   where it was broken, redeploying, or throttled — is never seen by
   anything again. It just sits. This happened three separate times
   during buildout (QA, render, and approval each missed a batch) and
   each was fixed by hand. Nothing retries on its own, so a stalled
   application is silent and permanent. The fix is deliberately dumb:
   re-write the row's own status back onto itself, which emits a fresh
   stream event that the downstream filter matches. No new plumbing, and
   it works for whichever stage happens to be the stalled one.

2. SURFACE NEEDS_REVIEW. QA parks an application in NEEDS_REVIEW when it
   finds an evidence gap it won't paper over — correct behavior, but
   until now those reached Matt through no channel at all and simply
   aged out. A gap QA can't close is often something only he can answer
   ("do I actually have 6 years of X?"), so silently dropping an 82-fit
   posting over one unmet line is a worse failure than asking.
"""
import os
import time

import boto3
from boto3.dynamodb.conditions import Attr

from job_applier_common.dynamo_utils import scan_all

APPROVAL_TO = os.environ["APPROVAL_TO_EMAIL"]
APPROVAL_FROM = os.environ["APPROVAL_FROM_EMAIL"]
# Generous: QA legitimately takes minutes per application and processes
# serially, so anything under this is probably just queued, not stuck.
STALE_MINUTES = int(os.environ.get("STALE_MINUTES", "30"))

# status -> the timestamp field written when the row entered it. A row
# sitting in one of these past STALE_MINUTES never got picked up by the
# stage that should have consumed it.
STALLED_STATES = {
    "GENERATED": "generated_at",
    "QA_PASSED": "qa_completed_at",
    "RENDERED": "rendered_at",
}

_applications = None
_postings_table_ref = None
_ses = None


def _table():
    global _applications
    if _applications is None:
        _applications = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications


def _postings():
    global _postings_table_ref
    if _postings_table_ref is None:
        _postings_table_ref = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _postings_table_ref


def _get_ses():
    global _ses
    if _ses is None:
        _ses = boto3.client("ses")
    return _ses


def _still_in_this_weeks_batch(posting_id: str) -> bool:
    """The weekly cap (§5) lives on the posting. A posting the re-rank
    displaced out of QUALIFIED is no longer part of this week's batch,
    and its half-finished application should stay parked rather than be
    pushed onward. Confirmed live 2026-09-03: the first sweeper run
    unstuck 13 applications, several of them long-displaced, which would
    have spent Bedrock calls re-QA'ing dead work and then emailed Matt
    past the cap he deliberately set."""
    if not posting_id:
        return False
    item = _postings().get_item(Key={"posting_id": posting_id}).get("Item") or {}
    return item.get("status") == "QUALIFIED"


def _unstick(stats: dict):
    table = _table()
    cutoff = int(time.time()) - STALE_MINUTES * 60

    for status, ts_field in STALLED_STATES.items():
        rows = scan_all(table, FilterExpression=Attr("status").eq(status))
        for row in rows:
            entered = int(row.get(ts_field, 0) or 0)
            if entered and entered > cutoff:
                continue  # recent enough to just be queued
            if not _still_in_this_weeks_batch(row.get("posting_id", "")):
                stats["skipped_displaced"] += 1
                continue
            # Re-assert the same status. The value doesn't change, but the
            # write emits a stream event, which is the whole point.
            table.update_item(
                Key={"application_id": row["application_id"]},
                UpdateExpression="SET #s = :s, swept_at = :ts, sweep_count = if_not_exists(sweep_count, :zero) + :one",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": status,
                    ":ts": int(time.time()),
                    ":zero": 0,
                    ":one": 1,
                },
            )
            print(f"swept {row['application_id']} stuck in {status} since {entered}")
            stats["unstuck"] += 1


def _notify_needs_review(stats: dict):
    table = _table()
    rows = [
        r
        for r in scan_all(table, FilterExpression=Attr("status").eq("NEEDS_REVIEW"))
        if not r.get("needs_review_notified_at")
    ]
    if not rows:
        return

    lines = [
        "These applications stopped at QA — it found a gap it wouldn't write around,",
        "so nothing was sent. Each one needs your call.",
        "",
    ]
    for r in rows:
        lines.append(f"{r.get('company_name','')} — {r.get('title','')}")
        lines.append(f"  fit {r.get('fit_score','?')}   {r.get('url','')}")
        for entry in r.get("qa_audit_trail", []) or []:
            for reason in (entry.get("reasons_to_reject") or [])[:3]:
                lines.append(f"  - {reason}")
            break
        lines.append("")
    lines += [
        "Nothing happens to these automatically. If one is worth pursuing, the gap is",
        "usually something only you can answer — reply to me and we'll fix the record",
        "or apply manually.",
    ]

    _get_ses().send_email(
        Source=APPROVAL_FROM,
        Destination={"ToAddresses": [APPROVAL_TO]},
        Message={
            "Subject": {"Data": f"{len(rows)} application(s) need your review"},
            "Body": {"Text": {"Data": "\n".join(lines)}},
        },
    )

    now = int(time.time())
    for r in rows:
        table.update_item(
            Key={"application_id": r["application_id"]},
            UpdateExpression="SET needs_review_notified_at = :ts",
            ExpressionAttributeValues={":ts": now},
        )
    stats["needs_review_notified"] = len(rows)


def handler(event, context):
    stats = {"unstuck": 0, "skipped_displaced": 0, "needs_review_notified": 0}
    _unstick(stats)
    _notify_needs_review(stats)
    print(f"job-applier-sweeper stats: {stats}")
    return stats
