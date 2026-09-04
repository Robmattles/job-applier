"""job-applier-generation — ARCHITECTURE.md §1 Résumé/Letter Generation
Lambda, §6 phase 4.

SQS-triggered (FoundationStack's generation_queue, fed by fit-scoring's
_pass2_gate on every QUALIFIED promotion) rather than polling — one
message per posting, batch size 1, so one bad posting's failure (a
malformed Bedrock response, a missing record) doesn't block or fail its
neighbors in the same batch.

Produces structured content only — headline, summary, résumé bullets
(each carrying which inventory record ids it drew from, per the
inventory's own `records_are_evidence_not_output` rule), and a cover
letter — never a laid-out document. That's §6 phase 5's job (QA passes +
fixed-template rendering), which reads what this Lambda writes to
`applications_table` and either passes it through or loops back here
once on a fixable gap (§4). The skills section is NOT Bedrock's job —
see inventory_store.compute_skills_section — and role-balance across the
candidate's whole career gets a mechanical detect-and-correct pass (see
_underrepresented_roles) rather than trusting a prompt instruction alone;
both are direct fixes for real failures confirmed live 2026-09-02.

This call is intentionally not the grounding-QA pass itself — it's asked
to stay grounded as it writes, but §4 pass 2 is the actual backstop that
verifies every claim traces to a record id, the same way fit-scoring
asks for an honest score but isn't itself the audit of that score.
"""
import json
import os
import time

import boto3

from job_applier_common import applicant_profile_store, bedrock_client, inventory_store

MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-opus-4-5-20251101-v1:0")

_postings_table = None
_applications_table = None


def _get_postings_table():
    global _postings_table
    if _postings_table is None:
        _postings_table = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _postings_table


def _get_applications_table():
    global _applications_table
    if _applications_table is None:
        _applications_table = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications_table


SYSTEM_PROMPT = """You are a professional resume writer producing tailored application \
content for one specific candidate applying to one specific job posting. You are given \
the posting, the candidate's chosen lane, their tagged accomplishment inventory (a list of \
atomic evidence records — not a 1:1 map to output bullets: combine, split, or drop records as \
this posting calls for), and a small set of Matt-confirmed career-summary facts (total years \
of experience, degree, certifications) separate from the project-level records — safe to cite \
in the summary or cover letter the same as any other grounded fact, just not tied to a \
specific bullet's source_record_ids the way project evidence is. Never invent a claim, number, \
tool, or outcome beyond what a record's text/metrics or the career-summary facts actually state.

Hard rules:
- Every resume bullet must cite the inventory record id(s) it drew from — a bullet can
  cite more than one record if it combines them.
- One bullet per overarching project by default. Split into a second bullet only when a
  sub-story is independently headline-grade (a large scale number, dramatic stakes, or a
  distinct capability this posting specifically asks for) — not just because the
  underlying work is interesting.
- Lead each bullet with what was built/decided/owned — scale, business stakes, ownership
  breadth. Methodology nuance and access/title framing ("held admin access," "validated
  X before shipping") are true and can appear as supporting clauses, but never as what
  the bullet leads with. A record whose own text says "maintained" or "supported" stays a
  maintenance/support claim in the bullet too — never upgrade it to "built" or "owned."
- Represent the candidate's whole career, not just the single most-recent or most-JD-
  relevant role. Confirmed live 2026-09-02: every draft before this rule existed put 7-11
  bullets under the current role and 0-1 under every other role combined, including an
  entire 11-year, 4-position prior career with real accomplishment records — that reads as
  a novice mistake (hiding most of the candidate's experience), not tailored evidence
  selection. You are given records from every role the candidate has evidence for in this
  lane. For every role that has genuinely lane-relevant records, include at least one
  bullet from it, unless you can honestly say none of that role's evidence applies to this
  specific posting — the correct number for a role that legitimately has nothing relevant
  is zero, but zero must be a real judgment, not a default.
- The headline describes the candidate as they ACTUALLY ARE, not as the posting wishes.
  It may name capability and domain ("Machine Learning Engineer | Entity Resolution at
  Scale"), but it must never assert a seniority level or title the candidate has not
  actually held. The titles held are in meta.role_history — currently topping out at
  Senior Data Scientist. Confirmed live 2026-09-04, Matt: "I don't like how frequently the
  top line of my resume deviates from my experience." Generated headlines were simply
  restating the target job's own title back at it — "Principal AI Solutions Architect" for
  a Principal posting, "Senior Staff Machine Learning Engineer" for a Staff one, "Staff
  Data Scientist" for a Staff DS one. That is a seniority overclaim in the single most
  prominent line of the document, and "tailored to this posting" was the instruction
  producing it. Tailor the emphasis, never the rank.
- Education is education. Degrees and coursework in the career-summary facts establish
  formal training, and may be described as such — they never become a claimed professional
  competency, a headline theme, or evidence of having practiced something on the job.
  Confirmed live 2026-09-04: a single coursework line ("Experimental Design Principles &
  Causal Inference") became the headline "Senior Data Scientist | Experimentation & Causal
  Inference | Networked Systems," implying professional causal-inference work the candidate
  has never done — his only exposure is graduate school. If the inventory holds no record
  of practicing something, it does not belong in the headline or the summary, whatever the
  posting asks for.
- The support/maintenance rule above applies to the headline, summary, and cover letter
  exactly as it does to bullets. A record qualified in its own text ("a supporting role,
  not the original design or build") cannot become a headline theme or a summary claim.
  Confirmed live 2026-09-04, Matt: "nicb assistant keeps popping up even though I had
  little to do with that." That record — explicitly support-only, priority 3 — was driving
  the headline in 14 of 60 applications and the summary in 23, because the honest framing
  was enforced only inside bullets. Being the candidate's only substantial record in a
  capability the posting wants is not a reason to promote it; it is a reason to be honest
  that the capability is thin.
- Do NOT write a "skills_section" — that is computed separately from the actual inventory
  tags, not from what you write. Leave that key out of your response entirely.
- The cover letter must reference specifics from the actual posting (company, team,
  something concrete about the role) rather than generic enthusiasm, and every factual
  claim in it must trace to the inventory the same as resume bullets.
- Every sentence — especially the last one in the summary and the cover letter's closing —
  must end coherently and grammatically. Confirmed live 2026-09-02: a closing shipped as
  "Thank you for considering my application—HEADWAY." — read every sentence you write
  before finishing and make sure nothing is a stray fragment or non-sequitur.
- This draft goes through a separate authenticity/grounding/specificity QA pass after
  you — write naturally and specifically, but don't perform "sounds human" tricks; that
  pass handles it. Your job here is real evidence, honest fit, specific language, and a
  draft that's actually tailored to this posting rather than generic.

Each resume bullet also needs a "role" field — the exact "Company — Title (Dates)" string
from whichever record it draws from, so the renderer can group bullets under the right
employer/date header the way a normal reverse-chronological resume does. If a bullet combines
records from more than one role, use the role of whichever record it leans on most.

Respond with ONLY a JSON object, no markdown fences, no other text, with exactly these keys:
{
  "headline": "<one line — real seniority, capability tailored to the posting; never the posting's own title>",
  "summary": "<2-4 sentence professional summary, tailored to this posting>",
  "resume_bullets": [
    {"text": "<bullet prose>", "source_record_ids": ["<id>", "..."], "role": "<exact role string from the record(s) cited>"}
  ],
  "cover_letter": {
    "opening": "<1-2 sentences, specific to this company/role>",
    "body_paragraphs": ["<paragraph>", "..."],
    "closing": "<1-2 sentences>"
  }
}"""

ROLE_BALANCE_FIX_PROMPT = """Your previous draft used zero bullets from at least one role that \
has genuinely lane-relevant evidence available. Revise resume_bullets to add at least one bullet \
from each of the underrepresented roles listed below, IF a real record in that role is relevant \
to this posting — pick the strongest genuinely-relevant record(s) for each. If, having looked \
again, a listed role truly has nothing relevant to this specific posting, you may leave it out —
but that has to be an honest per-role judgment, not the same default that produced the gap. Keep \
everything else about the draft (headline, summary, other bullets, cover letter) unless a change \
there is needed to stay consistent. Same grounding and role-field rules as before. Respond with \
ONLY the full revised content object, same shape as before (no skills_section key), no markdown \
fences, no other text."""


def _build_user_prompt(posting: dict, inventory: dict, lane: str, career_facts: dict) -> str:
    records = inventory_store.compact_records(inventory, lane=lane, include_role=True)
    if not records:
        # Defensive fallback — shouldn't happen (every lane has records),
        # but an empty pool would starve the model of evidence rather
        # than erroring, so widen back out instead.
        records = inventory_store.compact_records(inventory, include_role=True)
    return f"""JOB POSTING:
Title: {posting.get('title', '')}
Company: {posting.get('company_name', posting.get('company_slug', ''))}
Chosen lane: {lane}

Description:
{posting.get('description', '(no description available)')[:8000]}

CAREER-SUMMARY FACTS (Matt-confirmed aggregate facts, not tied to any one project — safe to
cite directly in the summary or cover letter, e.g. total years of experience; these don't need
a source_record_ids citation the way project bullets do):
{career_facts}

CANDIDATE ACCOMPLISHMENT INVENTORY, filtered to the "{lane}" lane ({len(records)} records):
{records}"""


def _role_pool(inventory: dict, lane: str) -> dict:
    """role string -> list of compact records in that role, for this
    lane. Used both to build the prompt and to check role-balance after
    the model responds."""
    records = inventory_store.compact_records(inventory, lane=lane, include_role=True)
    pool = {}
    for r in records:
        pool.setdefault(r.get("role", ""), []).append(r)
    return pool


def _underrepresented_roles(content: dict, role_pool: dict) -> dict:
    """Roles with real evidence in this lane but zero bullets in the
    draft — see SYSTEM_PROMPT's role-balance rule for why this check
    exists as code, not just a prompt instruction (the skills_section
    failure above is exactly why a second failure mode on the same
    model doesn't get the benefit of the doubt from prompting alone)."""
    represented = {b.get("role", "") for b in content.get("resume_bullets", [])}
    return {role: records for role, records in role_pool.items() if role and role not in represented}



def _generate(posting: dict, inventory: dict, lane: str, career_facts: dict) -> dict:
    role_pool = _role_pool(inventory, lane)
    user_prompt = _build_user_prompt(posting, inventory, lane, career_facts)
    content = bedrock_client.invoke_json(MODEL_ID, SYSTEM_PROMPT, user_prompt, max_tokens=4000)

    gap = _underrepresented_roles(content, role_pool)
    if gap:
        fix_prompt = f"""{ROLE_BALANCE_FIX_PROMPT}

JOB POSTING (for judging relevance — same one your draft below was written for):
Title: {posting.get('title', '')}
Company: {posting.get('company_name', posting.get('company_slug', ''))}
Description:
{posting.get('description', '(no description available)')[:8000]}

UNDERREPRESENTED ROLES (zero bullets in your draft, each with real evidence available):
{gap}

YOUR PREVIOUS DRAFT:
{json.dumps(content, indent=2)}"""
        content = bedrock_client.invoke_json(MODEL_ID, SYSTEM_PROMPT, fix_prompt, max_tokens=4000)

    content = inventory_store.apply_role_history_fallback(content, inventory)
    jd_text = f"{posting.get('title', '')} {posting.get('description', '')}"
    content["skills_section"] = inventory_store.compute_skills_section(inventory, lane, jd_text=jd_text)
    return content


def _process_one(posting_id: str) -> str:
    postings_table = _get_postings_table()
    posting = postings_table.get_item(Key={"posting_id": posting_id}).get("Item")
    if posting is None:
        print(f"WARNING generation: posting {posting_id} not found, skipping")
        return "not_found"
    if posting.get("status") != "QUALIFIED":
        # Idempotency guard — SQS's at-least-once delivery, or a retried
        # send, could hand this posting_id back more than once. Only the
        # first should do real work; a redelivery just overwrites the same
        # applications_table row, harmless, but no reason to spend a
        # second Bedrock call on it.
        print(f"generation: {posting_id} status is {posting.get('status')}, not QUALIFIED, skipping")
        return "skipped_not_qualified"

    inventory = inventory_store.load_inventory()
    career_facts = applicant_profile_store.career_summary_facts(applicant_profile_store.load_profile())
    lane = posting.get("lane", "senior_ds")
    content = _generate(posting, inventory, lane, career_facts)

    now = int(time.time())
    _get_applications_table().put_item(
        Item={
            "application_id": posting_id,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            "status": "GENERATED",
            "posting_id": posting_id,
            "company_name": posting.get("company_name", posting.get("company_slug", "")),
            "title": posting.get("title", ""),
            "url": posting.get("url", ""),
            # The submittability gate (§5) already resolved this at
            # QUALIFIED-promotion time — for an aggregator-sourced
            # posting it's the real employer ATS form, not the listing
            # article `url` above. Confirmed live 2026-09-03: it was
            # computed and stored on the posting but never carried
            # forward here, so the worker never saw it and fell back to
            # opening the raw listing even on postings the gate had
            # already matched to a fillable form.
            "apply_url": posting.get("apply_url", ""),
            "lane": lane,
            "fit_score": posting.get("fit_score"),
            "generated_content": content,
            "generated_at": now,
        }
    )
    return "generated"


def handler(event, context):
    # batch_size=1 on the SQS event source (GenerationStack) makes this a
    # one-message-per-invocation Lambda deliberately — unlike fit-scoring's
    # scan-many-postings-per-run loop, there's no reason to catch and
    # swallow a per-item exception here. Letting a real error propagate is
    # what makes SQS's own visibility-timeout retry and DLQ (FoundationStack,
    # max_receive_count=3) actually work as designed instead of every
    # failure silently vanishing as a caught-and-logged no-op.
    stats = {"generated": 0, "skipped_not_qualified": 0, "not_found": 0}
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        outcome = _process_one(body["posting_id"])
        stats[outcome] = stats.get(outcome, 0) + 1
    print(f"job-applier-generation stats: {stats}")
    return stats
