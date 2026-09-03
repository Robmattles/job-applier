"""job-applier-qa — ARCHITECTURE.md §1 QA Lambdas (authenticity+grounding+
specificity, then recruiter/ATS adversarial), §6 phase 5's LLM half
(rendering is the other half, not built here).

DynamoDB-Streams-triggered off applications_table, filtered (QAStack's
event source FilterCriteria) to NEW_IMAGE.status == "GENERATED" — so a
row Generation just wrote is the only thing that wakes this up. Every
status this Lambda itself writes (QA_PASSED, NEEDS_REVIEW) fails that
filter, so there's no risk of re-triggering itself off its own writes.

Two separately-framed critic calls per §4, not four — passes 1-3
(authenticity/grounding/specificity) share a frame (a critic reviewing
the draft against a checklist, empowered to rewrite in place) distinct
from pass 4 (a critic roleplaying a skeptical recruiter/ATS screen
against the actual posting). Splitting into four separate round-trips
would mean four times the latency/cost for the same "separately-framed
from the generator" property the architecture actually asks for.
"""
import json
import os
import time

import boto3
from boto3.dynamodb.types import TypeDeserializer

from job_applier_common import applicant_profile_store, bedrock_client, inventory_store

MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-opus-4-5-20251101-v1:0")
MAX_REVISION_LOOPS = int(os.environ.get("MAX_REVISION_LOOPS", "2"))  # §4 guardrail

_applications_table = None
_postings_table = None
_deserializer = TypeDeserializer()


def _get_applications_table():
    global _applications_table
    if _applications_table is None:
        _applications_table = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications_table


def _get_postings_table():
    global _postings_table
    if _postings_table is None:
        _postings_table = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _postings_table


def _deserialize_stream_image(image: dict) -> dict:
    return {k: _deserializer.deserialize(v) for k, v in image.items()}


# --------------------------------------------------------------------------
# Pass A — authenticity + grounding + specificity (§4 passes 1-3)
# --------------------------------------------------------------------------

PASS_A_SYSTEM_PROMPT = """You are a skeptical editor reviewing AI-generated resume and cover \
letter content before it reaches a candidate or an employer. You check three distinct things \
and, where fixable, rewrite in place rather than just flagging:

1. AUTHENTICITY — rewrite away from concrete LLM writing tells: rhetorical em-dash overuse;
   rule-of-three/triadic listing ("fast, reliable, and scalable"); stock transitions and hedges
   ("It's worth noting," "Moreover," "Furthermore," "In today's fast-paced environment");
   buzzword-as-filler ("leverage," "robust," "seamless," "cutting-edge," "results-driven,"
   "passionate about") unless backed by a specific number; uniform sentence rhythm where every
   bullet has the same [verb][object][result] shape; excessive hedging; perfectly balanced
   "not only X but also Y" constructions; generic opening lines ("As a highly motivated
   professional…"); mechanical keyword-stuffing; Title-Case Headers Everywhere as filler.
   ALSO check every sentence — especially the very last one in the summary and in the cover
   letter's closing — actually ends coherently and grammatically. Confirmed live 2026-09-02: a
   cover letter closing shipped as "Thank you for considering my application—HEADWAY." — a
   stray, non-sequitur word tacked onto an otherwise-correct sentence, generation-time noise
   that isn't a grounding problem (it's not a false claim) or a stock phrase, just broken text.
   Read every sentence as a human would and fix or remove anything that doesn't parse.

2. GROUNDING — every factual/quantified claim must trace to either the specific inventory
   record(s) cited for it, or the separately-given career-summary facts (total years of
   experience, degree, certifications — these are Matt-confirmed, not project evidence, and
   don't carry a source_record_ids citation the way a bullet does). Check: does the record's
   own text/metrics, or a career-summary fact, actually support what's claimed? Catch drift —
   an inflated number, a tool the record never mentions, a claim nudged stronger than the
   source, a years-of-experience figure that doesn't match the career-summary facts. A bullet
   citing a record id that isn't in what you were given is an automatic grounding failure.
   Anything that doesn't trace cleanly gets dropped or corrected, never guessed into
   plausibility. EXCEPTION: a bullet with an empty source_record_ids list is a role-summary
   line pulled directly from the candidate's employment-history roster (a real role that
   didn't have lane-relevant evidence for this specific posting, included anyway so the résumé
   never silently omits real employment history) — it's already Matt-confirmed fact, not a
   claim to ground or drop. Leave it as-is (light authenticity polish only, same as any other
   line) rather than flagging it as ungrounded or removing it.

3. SPECIFICITY — catch true, on-topic bullets that say nothing: abstracted nouns ("a validation
   threshold," "a deprecated service") standing in for the real system/technique/domain named in
   the source record. Ask: would someone with zero context know what this is actually about? If
   the record itself has the concrete detail (often sitting in its `metrics` field even when the
   bullet text is vague), pull it in.

Rewrite what's fixable directly. Only flag something as unfixable if the underlying evidence
genuinely doesn't support what's needed — not fixable by better wording.

Respond with ONLY a JSON object, no markdown fences, no other text:
{
  "verdict": "PASS" or "REVISED" or "NEEDS_REVIEW",
  "findings": [{"type": "authenticity|grounding|specificity", "location": "<e.g. bullet 2, summary, cover_letter.opening>", "issue": "<what's wrong>"}],
  "revised_content": <the full content object, same shape as given (including each bullet's "role" field, unchanged unless the fix itself moves a claim to a different record; skills_section can be left as-is either way, it's discarded and replaced regardless), with fixes applied — identical to the input if verdict is PASS>,
  "unfixable_gaps": ["<description, only if verdict is NEEDS_REVIEW>"]
}"""


def _cited_records(content: dict, inventory: dict) -> dict:
    """Every record id cited anywhere in the draft, looked up from the
    inventory — a missing id is itself a grounding failure worth
    surfacing to the model rather than silently dropping."""
    ids = set()
    for bullet in content.get("resume_bullets", []):
        ids.update(bullet.get("source_record_ids", []))
    by_id = {r["id"]: r for r in inventory.get("records", [])}
    return {i: by_id.get(i, "RECORD ID NOT FOUND IN INVENTORY") for i in ids}


def _run_pass_a(content: dict, inventory: dict, career_facts: dict) -> dict:
    cited = _cited_records(content, inventory)
    user_prompt = f"""DRAFT CONTENT:
{json.dumps(content, indent=2)}

CITED INVENTORY RECORDS (every source_record_id used anywhere above, looked up):
{json.dumps(cited, indent=2)}

CAREER-SUMMARY FACTS (Matt-confirmed aggregate facts — total years of experience, degree,
certifications — not tied to a specific bullet's source_record_ids. A claim like "7+ years of
data science experience" traces here, not to a project record; it's still grounded, not drift,
as long as it matches these facts):
{json.dumps(career_facts, indent=2)}"""
    return bedrock_client.invoke_json(MODEL_ID, PASS_A_SYSTEM_PROMPT, user_prompt, max_tokens=4000)


# --------------------------------------------------------------------------
# Pass B — recruiter/ATS adversarial review (§4 pass 4)
# --------------------------------------------------------------------------

PASS_B_SYSTEM_PROMPT = """You are a skeptical technical recruiter running an ATS keyword/\
requirement screen on a candidate's resume and cover letter, checked against one specific job \
posting. This is a different check from an authenticity/grounding pass — you're not checking \
if it sounds AI-written or if claims are true, you're checking whether this document actually \
gets the candidate through to a human, and whether it should.

Do this:
1. List the JD's REQUIRED lines (not nice-to-haves) individually — don't bundle two requirements
   into one finding even if they feel adjacent (e.g. "mentoring" and "stakeholder communication"
   are two separate lines to check, not one). For each, mark covered true/false with a one-line
   note pointing at what in the document covers it (or doesn't).
2. The 5 strongest reasons to interview, each grounded in something actually on the page.
3. The 3 most likely reasons to reject, each naming the specific JD line it fails — every finding
   here must trace to a specific line in the posting, not a generic resume-best-practices note.
4. Basic ATS-parseability notes (consistent date formats, no obviously unparseable structure) —
   informational only, the fixed template is expected to already guarantee this.

Then decide:
- PASS: no required-line gaps that would sink this application.
- FIXABLE_GAP: a required line isn't covered, but the candidate's inventory likely has evidence
  for it that generation didn't select — give a specific instruction for what to re-select or
  rewrite (name the record/skill/angle, not "improve the resume").
- NEEDS_REVIEW: a required line has a genuine evidence gap — not fixable by rewriting, a human
  needs to see this before it goes out.

Respond with ONLY a JSON object, no markdown fences, no other text:
{
  "verdict": "PASS" or "FIXABLE_GAP" or "NEEDS_REVIEW",
  "required_line_coverage": [{"jd_line": "<text>", "covered": true, "note": "<...>"}],
  "reasons_to_interview": ["<...>"],
  "reasons_to_reject": ["<names a specific JD line>"],
  "ats_parseability_notes": ["<...>"],
  "fix_instructions": "<specific, only if FIXABLE_GAP>",
  "unfixable_reason": "<specific, only if NEEDS_REVIEW>"
}"""


def _run_pass_b(content: dict, posting: dict) -> dict:
    user_prompt = f"""JOB POSTING:
Title: {posting.get('title', '')}
Company: {posting.get('company_name', posting.get('company_slug', ''))}

Description:
{posting.get('description', '(no description available)')[:8000]}

CANDIDATE DOCUMENT:
{json.dumps(content, indent=2)}"""
    return bedrock_client.invoke_json(MODEL_ID, PASS_B_SYSTEM_PROMPT, user_prompt, max_tokens=3000)


# --------------------------------------------------------------------------
# Targeted revision — used when pass A leaves findings unfixed in its own
# rewrite, or pass B returns FIXABLE_GAP with a specific instruction.
# --------------------------------------------------------------------------

REVISE_SYSTEM_PROMPT = """You are revising resume/cover-letter content that already exists, \
based on a specific reviewer instruction. You are given the current content, the fix \
instruction, and the full accomplishment inventory to re-select evidence from. Apply exactly \
what the instruction asks — re-select or rewrite the specific bullet(s)/section named, leave \
everything else untouched. Same grounding rule as always: every bullet must cite the inventory \
record id(s) it draws from, never invent a claim beyond what a record's text/metrics state. \
Every resume bullet keeps (or gets, if newly added) a "role" field — the exact "Company — \
Title (Dates)" string from whichever record it draws from — the renderer groups bullets under \
that header the way a normal reverse-chronological resume does. Ignore skills_section entirely
— it's computed separately from the real inventory tags, not authored, and whatever you put
there is discarded and replaced regardless.

Respond with ONLY the full revised content object, same shape as given, no markdown fences, no \
other text."""


def _revise(content: dict, instruction: str, inventory: dict, lane: str, career_facts: dict) -> dict:
    records = inventory_store.compact_records(inventory, lane=lane, include_role=True)
    user_prompt = f"""CURRENT CONTENT:
{json.dumps(content, indent=2)}

FIX INSTRUCTION:
{instruction}

CAREER-SUMMARY FACTS (Matt-confirmed aggregate facts, citable directly, no source_record_ids needed):
{json.dumps(career_facts, indent=2)}

ACCOMPLISHMENT INVENTORY, filtered to the "{lane}" lane ({len(records)} records):
{records}"""
    return bedrock_client.invoke_json(MODEL_ID, REVISE_SYSTEM_PROMPT, user_prompt, max_tokens=4000)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def _run_qa_cycle(posting: dict, content: dict, inventory: dict, lane: str, career_facts: dict) -> tuple:
    """Returns (final_content, status, audit_trail). status is one of
    QA_PASSED / NEEDS_REVIEW. Capped at MAX_REVISION_LOOPS (§4 guardrail:
    "anything still unresolved holds the application in NEEDS_REVIEW for
    Matt rather than silently shipping or discarding it")."""
    # Re-asserted after every Bedrock round-trip below, not just set once
    # — a revise() call hands the model the full content object and asks
    # for "the same shape back," which is exactly the kind of opening
    # that let fabricated skills and dropped roles back in before either
    # was enforced deterministically (see inventory_store's
    # compute_skills_section / apply_role_history_fallback).
    # Belt-and-suspenders with generation's own re-assertion: QA runs on
    # content that already has both applied, but nothing stops a
    # revise() call from touching them anyway.
    jd_text = f"{posting.get('title', '')} {posting.get('description', '')}"
    skills_section = inventory_store.compute_skills_section(inventory, lane, jd_text=jd_text)

    def _reassert(c: dict) -> dict:
        c["skills_section"] = skills_section
        return inventory_store.apply_role_history_fallback(c, inventory)

    content = _reassert(content)

    audit_trail = []
    for loop_num in range(MAX_REVISION_LOOPS + 1):
        pass_a = _run_pass_a(content, inventory, career_facts)
        content = _reassert(pass_a.get("revised_content", content))
        audit_trail.append({"loop": loop_num, "pass": "A", "verdict": pass_a.get("verdict"),
                             "findings": pass_a.get("findings", [])})
        if pass_a.get("verdict") == "NEEDS_REVIEW":
            if loop_num >= MAX_REVISION_LOOPS:
                return content, "NEEDS_REVIEW", audit_trail
            fix = "; ".join(pass_a.get("unfixable_gaps", [])) or "resolve the flagged grounding/authenticity gap"
            content = _reassert(_revise(content, fix, inventory, lane, career_facts))
            continue

        pass_b = _run_pass_b(content, posting)
        audit_trail.append({"loop": loop_num, "pass": "B", "verdict": pass_b.get("verdict"),
                             "reasons_to_reject": pass_b.get("reasons_to_reject", []),
                             "required_line_coverage": pass_b.get("required_line_coverage", [])})

        if pass_b.get("verdict") == "PASS":
            return content, "QA_PASSED", audit_trail
        if pass_b.get("verdict") == "NEEDS_REVIEW" or loop_num >= MAX_REVISION_LOOPS:
            return content, "NEEDS_REVIEW", audit_trail
        # FIXABLE_GAP with loops remaining
        content = _reassert(_revise(content, pass_b.get("fix_instructions", ""), inventory, lane, career_facts))

    return content, "NEEDS_REVIEW", audit_trail


def _process_one(application_id: str, new_image: dict):
    applications_table = _get_applications_table()
    posting = _get_postings_table().get_item(Key={"posting_id": new_image["posting_id"]}).get("Item")
    if posting is None:
        print(f"WARNING qa: posting for application {application_id} not found, skipping")
        return "not_found"

    inventory = inventory_store.load_inventory()
    career_facts = applicant_profile_store.career_summary_facts(applicant_profile_store.load_profile())
    lane = new_image.get("lane", "senior_ds")
    content = new_image["generated_content"]

    final_content, status, audit_trail = _run_qa_cycle(posting, content, inventory, lane, career_facts)

    applications_table.update_item(
        Key={"application_id": application_id},
        UpdateExpression="SET #s = :s, generated_content = :c, qa_audit_trail = :a, qa_completed_at = :ts",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": status,
            ":c": final_content,
            ":a": audit_trail,
            ":ts": int(time.time()),
        },
    )
    return status


def handler(event, context):
    stats = {"QA_PASSED": 0, "NEEDS_REVIEW": 0, "not_found": 0}
    for record in event.get("Records", []):
        new_image_raw = record.get("dynamodb", {}).get("NewImage")
        if not new_image_raw:
            continue
        new_image = _deserialize_stream_image(new_image_raw)
        outcome = _process_one(new_image["application_id"], new_image)
        stats[outcome] = stats.get(outcome, 0) + 1
    print(f"job-applier-qa stats: {stats}")
    return stats
