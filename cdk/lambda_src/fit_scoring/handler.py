"""job-applier-fit-scoring — ARCHITECTURE.md §1 Fit-Scoring Lambda +
Threshold gate. First real (unsupervised) Bedrock call in this pipeline —
everything before this point (the QA passes in §4) was hand-simulated by
Claude with Matt catching misses each round, per the §7 caveat. This is
where that actually gets tested.

Two passes:
  1. Score every `status=NEW` posting with a Bedrock evidence audit
     against accomplishment-inventory.json: fit score, lane, reasons,
     resolves "ambiguous" remote status from JD text, checks the $130k
     comp floor. Hard filters (not_remote, below comp floor when stated)
     reject here rather than scoring low — they're not fit questions.
  2. Threshold + ramped weekly cap (§5): rank SCORED postings by fit
     score blended with freshness, promote the top N to QUALIFIED where
     N is what's left of this week's cap.
"""
import json
import os
import re
import time

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from job_applier_common import (
    applicant_profile_store,
    bedrock_client,
    inventory_store,
    submittability,
)

# Confirmed live 2026-09-02: a manual invoke overlapping the EventBridge
# schedule caused two concurrent executions of this handler — one posting
# got scored twice (wasted Bedrock calls) and _pass2_gate's weekly-cap
# check (a plain eventually-consistent Scan, no lock) drifted between the
# two runs' own counts. It stayed under WEEKLY_CAP that time by luck, not
# guarantee. reserved_concurrent_executions=1 in CDK would be the obvious
# fix but this account's UnreservedConcurrentExecution floor is already at
# its 10 minimum, so a self-releasing DynamoDB lock stands in instead.
LOCK_ID = "__LOCK__fit-scoring"
LOCK_TTL_SECONDS = 900  # a bit over the 10-min function timeout, so an
# ungraceful timeout (lock never explicitly released) still self-clears soon


class _AlreadyRunning(Exception):
    pass


def _acquire_lock(table):
    now = int(time.time())
    try:
        table.put_item(
            Item={"posting_id": LOCK_ID, "ttl": now + LOCK_TTL_SECONDS},
            ConditionExpression="attribute_not_exists(posting_id) OR #ttl < :now",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":now": now},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise _AlreadyRunning() from None
        raise


def _release_lock(table):
    table.delete_item(Key={"posting_id": LOCK_ID})

GENERATION_QUEUE_URL = os.environ.get("GENERATION_QUEUE_URL")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
FIT_SCORE_THRESHOLD = int(os.environ.get("FIT_SCORE_THRESHOLD", "60"))
WEEKLY_CAP_DEFAULT = int(os.environ.get("WEEKLY_CAP", "10"))  # fallback only
COMP_FLOOR = 130_000  # §3 — keep in sync with applicant-profile.json
MAX_POSTINGS_PER_RUN = int(os.environ.get("MAX_POSTINGS_PER_RUN", "25"))  # Lambda has a 15-min
# cap and each Bedrock call takes a few seconds; the 4-hour schedule catches up on any backlog
# over subsequent runs rather than one invocation trying to drain everything at once.
AGE_PENALTY_PER_DAY = float(os.environ.get("AGE_PENALTY_PER_DAY", "3"))  # §5 composite ranking

_LEVEL_WORDS_RE = re.compile(r"\b(senior|sr|staff|principal|lead|jr|junior)\b\.?", re.I)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _dedup_key(posting: dict) -> tuple:
    """Confirmed live 2026-09-02: phData had 4 distinct Greenhouse job ids
    ("Principal Applied AI Solutions Architect" / "Applied AI solutions
    Architect" / "Applied AI Solutions Architect" / "Applied AI Solution
    Architect", 3 of them updated within the same second) that are
    almost certainly one req reposted, not 4 real openings — and they
    ate 4 of that week's 10 cap slots. Same company + same title once
    leveling words/case/punctuation are stripped out counts as one slot
    in the ranking below, not four."""
    company = posting.get("company_slug", "")
    title = (posting.get("title") or "").lower()
    title = _LEVEL_WORDS_RE.sub("", title)
    title = _NON_ALNUM_RE.sub("", title)
    return (company, title)

_table = None
_sqs = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _table


def _get_sqs():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs")
    return _sqs


_known_companies_table = None
_known_company_cache = {}


def _known_company(company_slug: str):
    """The ATS board ingestion already discovered for this employer, if
    any — cached per container, since the gate looks up the same handful
    of companies every run."""
    if not company_slug:
        return None
    if company_slug in _known_company_cache:
        return _known_company_cache[company_slug]
    global _known_companies_table
    if _known_companies_table is None:
        name = os.environ.get("KNOWN_COMPANIES_TABLE")
        if not name:
            return None
        _known_companies_table = boto3.resource("dynamodb").Table(name)
    try:
        item = _known_companies_table.get_item(Key={"company_slug": company_slug}).get("Item")
    except Exception as e:  # noqa: BLE001 — a lookup failure shouldn't kill the gate
        print(f"known_companies lookup failed for {company_slug}: {e}")
        item = None
    _known_company_cache[company_slug] = item
    return item


SYSTEM_PROMPT = """You are a hiring manager deciding whether this candidate is worth \
interviewing for this specific posting. You are given the posting, the candidate's career \
facts (degrees, coursework, years, certifications), and their tagged accomplishment \
inventory (records with resume-ready text, metrics, skill tags, lane tags, and a priority \
from 1=flagship to 3=minor).

The question is "could this person credibly do this job and would a reasonable hiring \
manager want to talk to them" — NOT "how much of the inventory literally matches the JD \
text." Those are different questions and the second one is wrong.

Calibrated against 25 real postings the candidate labeled by hand on 2026-09-03, where an \
earlier evidence-matching version of this prompt got a 20% false-negative rate — it scored \
roles the candidate actively wanted at 25-42 and would have discarded them unseen. What it \
got wrong, and what you must get right:

- The inventory is skewed toward recent GenAI/LLM work because of how it was built, NOT \
  because that is the whole of the candidate's ability. Senior data-science work that isn't \
  GenAI — product analytics, experimentation and causal inference, recommender/ranking \
  systems, operations research, pricing, forecasting — is squarely in scope, and the \
  candidate said yes to every such role the old prompt rejected. Formal training counts as \
  real evidence here: the career facts include graduate coursework in experimental design \
  and causal inference, and an earlier version scored a consumer-experimentation role a 25 \
  while never seeing that.
- Absence of a named tool is not inability. A strong senior practitioner picks up a specific \
  framework; judge the underlying capability, not keyword overlap.
- Fifteen years of analytical work across the FBI and NICB is the candidate's real base. \
  Weigh that breadth, not only the last two years of it.

Two things the candidate does NOT want, learned from the same labeling exercise — score \
these low even when the skills line up well:
- Forward-deployed / solutions-engineer / customer-facing-delivery roles. They rejected \
  every one in the sample (two "Forward Deployed AI Engineer" roles and a "Solutions \
  Architect") despite strong technical overlap. Working AT a consultancy is fine — being \
  the customer-facing delivery engineer is not.
- People-management roles (Engineering Manager, Program Manager). They want to build.

Use the full 0-100 range and make your scores actually discriminate. The earlier prompt \
collapsed onto about five distinct values with a dead zone between 46 and 71, which made \
ranking meaningless for the ~99 postings that all landed on the same number. If two \
postings differ in how good a match they are, their scores should differ.

Rough anchors: 85+ = would be a strong candidate, want to interview. 70-84 = solid, worth \
applying. 55-69 = plausible but a real stretch or a partial mismatch. Below 55 = wrong role \
type, wrong seniority, or a gap that actually disqualifies.

Respond with ONLY a JSON object, no markdown fences, no other text, with exactly these keys:
{
  "fit_score": <integer 0-100, using the full range>,
  "lane": "<one of: senior_ds, applied_mle, applied_ai>",
  "reasons_to_interview": [<1-5 short strings, each citing the inventory record id or career fact behind it>],
  "reasons_to_reject": [<1-3 short strings naming genuine gaps or mismatches>],
  "role_type_flag": "<one of: none, forward_deployed, people_management — set when the posting is one of the two kinds above>",
  "remote_resolution": "<one of: confirmed_remote, not_remote, still_ambiguous — only meaningful if asked below>",
  "comp_meets_floor": <true, false, or null if the posting states no comp figure at all>
}"""


def _build_user_prompt(posting: dict, inventory: dict, career_facts: dict) -> str:
    records = inventory_store.compact_records(inventory)
    remote_note = ""
    if posting.get("remote_status") == "ambiguous":
        remote_note = (
            "\nThe ingestion pipeline could not determine from structured fields whether "
            "this posting is genuinely remote. Read the job description text below and "
            "resolve remote_resolution yourself — if it's not clearly remote, say not_remote."
        )
    return f"""JOB POSTING:
Title: {posting.get('title', '')}
Company: {posting.get('company_name', posting.get('company_slug', ''))}
{remote_note}

Description:
{posting.get('description', '(no description available)')[:8000]}

CANDIDATE CAREER FACTS (degrees and coursework included — formal training in a subject is
real evidence of capability in it, not a footnote):
{career_facts}

CANDIDATE ACCOMPLISHMENT INVENTORY ({len(records)} records):
{records}

Also check: does the posting state a compensation figure (base or total)? If so, is it at \
or above ${COMP_FLOOR:,}? Set comp_meets_floor accordingly (null if no figure is stated at all)."""


def _score_posting(posting: dict, inventory: dict, career_facts: dict) -> dict:
    user_prompt = _build_user_prompt(posting, inventory, career_facts)
    return bedrock_client.invoke_json(MODEL_ID, SYSTEM_PROMPT, user_prompt)


def _scan_by_status(status: str, limit: int = None):
    table = _get_table()
    items = []
    kwargs = {"FilterExpression": Attr("status").eq(status)}
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp and (limit is None or len(items) < limit):
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items


def _pass1_score(stats: dict):
    inventory = inventory_store.load_inventory()
    career_facts = applicant_profile_store.career_summary_facts(
        applicant_profile_store.load_profile()
    )
    new_postings = _scan_by_status("NEW")[:MAX_POSTINGS_PER_RUN]
    table = _get_table()

    for posting in new_postings:
        stats["scored"] += 1
        try:
            result = _score_posting(posting, inventory, career_facts)
        except Exception as e:  # noqa: BLE001 — one bad posting shouldn't kill the batch
            stats["score_errors"] += 1
            print(f"ERROR scoring {posting['posting_id']}: {e}")
            continue

        # Hard filters — these aren't fit questions, they reject regardless of score.
        remote_status = posting.get("remote_status")
        if remote_status == "ambiguous":
            resolved = result.get("remote_resolution")
            if resolved == "not_remote":
                table.update_item(
                    Key={"posting_id": posting["posting_id"]},
                    UpdateExpression="SET #s = :s, remote_status = :r",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":s": "REJECTED_NOT_REMOTE", ":r": "not_remote"},
                )
                stats["rejected_not_remote"] += 1
                continue
            remote_status = resolved or "ambiguous"

        if result.get("comp_meets_floor") is False:
            table.update_item(
                Key={"posting_id": posting["posting_id"]},
                UpdateExpression="SET #s = :s",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "REJECTED_COMP_FLOOR"},
            )
            stats["rejected_comp"] += 1
            continue

        table.update_item(
            Key={"posting_id": posting["posting_id"]},
            UpdateExpression=(
                "SET #s = :s, fit_score = :fs, #lane = :lane, "
                "reasons_to_interview = :rti, reasons_to_reject = :rtr, "
                "remote_status = :rs, scored_at = :ts"
            ),
            ExpressionAttributeNames={"#s": "status", "#lane": "lane"},
            ExpressionAttributeValues={
                ":s": "SCORED",
                ":fs": result.get("fit_score", 0),
                ":lane": result.get("lane", "senior_ds"),
                ":rti": result.get("reasons_to_interview", []),
                ":rtr": result.get("reasons_to_reject", []),
                ":rs": remote_status,
                ":ts": int(time.time()),
            },
        )
        stats["scored_ok"] += 1


def _pass2_gate(stats: dict):
    """§5: "Always highest fit-score (blended with freshness) first" — a
    strict re-rank each run, not just filling empty slots. Confirmed live
    2026-09-02: an earlier fill-empty-slots-only version let a posting
    qualify simply for being scored before a genuinely stronger one
    reached the front of the backlog, and then never revisited that
    choice — a plain GitLab "AI Engineer" posting later scored 82, higher
    than 8 of the 9 postings already sitting in that week's cap at 72, and
    the weaker ones would have sat there un-displaced for a full 7 days.
    Safe to fix now rather than leave sticky: nothing QUALIFIED has been
    shown to Matt yet (§6 phase 6, approval emails, doesn't exist yet) —
    once it does, silently un-QUALIFYING something already surfaced to
    him would be real UX cost, not a free correction like this is today.
    """
    table = _get_table()
    now = int(time.time())
    seven_days_ago = now - 7 * 86400

    # Read per-run, not at import: a warm container would otherwise hold
    # a stale cap for as long as it lives, and lowering the cap in a
    # hurry is exactly when the delay would hurt.
    weekly_cap = inventory_store.load_weekly_cap(WEEKLY_CAP_DEFAULT)
    stats["weekly_cap"] = weekly_cap

    already_qualified_this_week = [
        p for p in _scan_by_status("QUALIFIED") if p.get("qualified_at", 0) >= seven_days_ago
    ]
    scored = [p for p in _scan_by_status("SCORED") if int(p.get("fit_score", 0)) >= FIT_SCORE_THRESHOLD]
    stats["remaining_weekly_slots_before_gate"] = max(
        0, weekly_cap - len(already_qualified_this_week)
    )

    def composite(p):
        first_seen = int(p.get("first_seen_at", now))
        age_days = max(0, (now - first_seen) / 86400)
        # Raised 2026-09-02 from 0.5 to 3 points/day (Matt's call, after
        # seeing real scores cluster in a ~70-82 band that doesn't
        # differentiate much) — a 10-point score gap now takes ~3-4 days
        # of extra freshness to overturn, not 20. Recency clearly
        # dominates for similar-quality matches; a genuinely much better
        # match (20+ points) can still beat a very fresh mediocre one.
        return int(p.get("fit_score", 0)) - AGE_PENALTY_PER_DAY * age_days

    contenders = already_qualified_this_week + scored
    contenders.sort(key=composite, reverse=True)

    deduped = []
    seen_keys = set()
    for posting in contenders:
        key = _dedup_key(posting)
        if key in seen_keys:
            stats["deduped_near_duplicate_title"] = stats.get("deduped_near_duplicate_title", 0) + 1
            continue
        seen_keys.add(key)
        deduped.append(posting)

    # Submittability, checked here rather than discovered at the browser.
    # Confirmed live 2026-09-03: a WorkWave posting from Jobicy scored 88,
    # cleared QA, generated documents, took a cap slot and emailed Matt —
    # for a role with no reachable form anywhere and nothing matching on
    # the employer's own Lever board. Matt's call: "if it's not
    # submittable, it's not worth worrying about." Walk the ranked list
    # and fill the cap with postings that can actually be applied to,
    # rather than truncating first and losing slots to dead listings.
    promotable, checked = [], 0
    for posting in deduped:
        if len(promotable) >= weekly_cap:
            break
        # Anything already QUALIFIED passed this check when it was
        # promoted; don't re-hit the ATS APIs for it every run.
        if posting["posting_id"] in {p["posting_id"] for p in already_qualified_this_week}:
            promotable.append(posting)
            continue
        checked += 1
        company = posting["posting_id"].split("#", 2)[1] if "#" in posting["posting_id"] else ""
        known = _known_company(company)
        apply_url, reason = submittability.resolve(posting, known)
        if not apply_url:
            table.update_item(
                Key={"posting_id": posting["posting_id"]},
                UpdateExpression="SET #s = :s, unsubmittable_reason = :r, checked_at = :ts",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": "REJECTED_UNSUBMITTABLE", ":r": reason, ":ts": now
                },
            )
            print(f"unsubmittable {posting['posting_id']}: {reason}")
            stats["rejected_unsubmittable"] = stats.get("rejected_unsubmittable", 0) + 1
            continue
        posting["_apply_url"] = apply_url
        promotable.append(posting)
    stats["submittability_checked"] = checked

    top_ids = {p["posting_id"] for p in promotable}
    already_qualified_ids = {p["posting_id"] for p in already_qualified_this_week}

    for posting in promotable:
        posting_id = posting["posting_id"]
        if posting_id in already_qualified_ids:
            continue  # already QUALIFIED and still earns its slot — leave
            # qualified_at untouched so its 7-day window doesn't reset
            # just for remaining the best pick on every 4-hour run.
        table.update_item(
            Key={"posting_id": posting_id},
            # apply_url is stored now rather than re-derived at submission
            # time — it's the URL this posting was *verified reachable at*,
            # and for an aggregator-sourced posting it's the resolved
            # employer form rather than the listing article we ingested.
            UpdateExpression="SET #s = :s, qualified_at = :ts, apply_url = :u",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "QUALIFIED", ":ts": now, ":u": posting.get("_apply_url", "")
            },
        )
        stats["qualified"] += 1
        # Hand off to Generation (§1) rather than making it poll/scan for
        # QUALIFIED postings itself — event-driven, and the SQS visibility
        # timeout + DLQ (FoundationStack) give it retry/failure handling
        # for free instead of reimplementing that here.
        if GENERATION_QUEUE_URL:
            _get_sqs().send_message(
                QueueUrl=GENERATION_QUEUE_URL,
                MessageBody=json.dumps({"posting_id": posting_id}),
            )

    for posting in already_qualified_this_week:
        if posting["posting_id"] not in top_ids:
            # Bumped by something that scored strictly better — demote
            # back to SCORED so it re-enters next run's ranking rather
            # than staying permanently qualified off an earlier, weaker
            # comparison set. Its applications_table row (if Generation
            # already ran) is just a harmless orphaned draft, never shown
            # to Matt — safe since phase 6 doesn't exist yet.
            table.update_item(
                Key={"posting_id": posting["posting_id"]},
                UpdateExpression="SET #s = :s REMOVE qualified_at",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "SCORED"},
            )
            stats["displaced"] = stats.get("displaced", 0) + 1


def handler(event, context):
    table = _get_table()
    try:
        _acquire_lock(table)
    except _AlreadyRunning:
        print("job-applier-fit-scoring: another invocation holds the lock, skipping this run")
        return {"skipped": "already_running"}

    stats = {
        "scored": 0,
        "scored_ok": 0,
        "score_errors": 0,
        "rejected_not_remote": 0,
        "rejected_comp": 0,
        "qualified": 0,
        "displaced": 0,
        "deduped_near_duplicate_title": 0,
    }
    try:
        _pass1_score(stats)
        _pass2_gate(stats)
    finally:
        _release_lock(table)
    print(f"job-applier-fit-scoring stats: {stats}")
    return stats
