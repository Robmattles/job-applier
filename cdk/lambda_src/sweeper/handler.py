"""job-applier-sweeper — ARCHITECTURE.md §6 phase 9 (guardrails).

UNSTICK. Every stage after generation is DynamoDB-Streams-triggered with
`starting_position=LATEST`, which means a row that reached a trigger
state *before* that stage existed — or during any window where it was
broken, redeploying, or throttled — is never seen by anything again. It
just sits. This happened three separate times during buildout (QA,
render, and approval each missed a batch) and each was fixed by hand.
Nothing retries on its own, so a stalled application is silent and
permanent. The fix is deliberately dumb: re-write the row's own status
back onto itself, which emits a fresh stream event that the downstream
filter matches. No new plumbing, and it works for whichever stage
happens to be the stalled one.

A per-application "N applications need your call" email used to live
here too, one for each NEEDS_REVIEW application QA couldn't clear.
Removed 2026-09-03, Matt's call: QA's pass B (cdk/lambda_src/qa/handler.py)
was treating every JD line as an equally hard gate, when most postings
list far more "requirements" than any real hire actually clears — so
this email was mostly noise pointing at gaps a real recruiter wouldn't
screen on, dressed up as "reply and we'll fix it" when nothing actually
processed a reply. Fixed at the source instead: pass B now separates
HARD_GATE lines from SOFT/wishlist ones and only escalates to
NEEDS_REVIEW on a hard gate, so far fewer applications land here at all,
and the honest caveats about the soft gaps still ride along in a normal
approval email rather than blocking it. What does still land in
NEEDS_REVIEW (a real hard gate — e.g. a stated years-of-experience
threshold, a required clearance) surfaces in the weekly digest's
aggregate count instead of a dedicated email — visible without a
notification for every one, individually, going out again."""
import os
import time

import boto3

from boto3.dynamodb.conditions import Attr

from job_applier_common import inventory_store
from job_applier_common.dynamo_utils import scan_all

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


def handler(event, context):
    if inventory_store.halt_if_paused("sweeper"):
        return {"halted": "kill_switch"}

    stats = {"unstuck": 0, "skipped_displaced": 0}
    _unstick(stats)
    print(f"job-applier-sweeper stats: {stats}")
    return stats
