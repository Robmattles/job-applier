"""Grounded drafting for custom application questions — ARCHITECTURE.md
§1 Submission Worker ("drafts GENERATE_GROUNDED answers for open-ended
questions, grounding-checked against the accomplishment inventory same
as résumé bullets") and §5's hard rule: never invent an answer to an
ambiguous or legally-meaningful question.

Every question lands in one of three buckets, mirroring
applicant-profile.json's own `_meta.classification_rule`:

  SAFE_AUTOFILL     answered directly from applicant-profile.json, no LLM
  GENERATE_GROUNDED Bedrock drafts it from the accomplishment inventory
  NEEDS_REVIEW      Matt answers it himself, full stop

The classifier is deliberately biased toward NEEDS_REVIEW. A question
this module doesn't recognize is not a question it should be guessing
at — these answers go into a real application under Matt's name, and
unlike a résumé bullet nobody reviews them again downstream.

Drafts are never filled automatically. worker.py shows each one and Matt
accepts, edits, or skips it.
"""
import json
import re

MODEL_ID = "us.anthropic.claude-opus-4-5-20251101-v1:0"

# Legally meaningful, identity-related, or negotiation-sensitive. Never
# drafted, never auto-filled — §5.
NEEDS_REVIEW_PATTERNS = (
    "sponsor", "visa", "work authorization", "authorized to work", "citizen",
    "criminal", "felony", "conviction", "background check", "security clearance",
    "gender", "race", "ethnicity", "veteran", "disability", "self-identif", "eeo",
    "salary", "compensation", "desired pay", "expected pay", "pay range", "rate",
    "certify", "attest", "i agree", "acknowledge", "consent", "terms",
    "reference", "reason for leaving", "terminated", "non-compete",
    "start date", "available to start", "notice period",
)

# Deliberately no SAFE_AUTOFILL bucket for custom questions. A
# years-of-experience question looks autofillable and isn't: confirmed
# live 2026-09-03 against a real phData form asking "Do you have 6+ years
# experience deploying Machine Learning models into production?" —
# applicant-profile.json answers 7, but its own `_inferred` note says
# reasonable people could count that as ~5 (from the NICB title only).
# Pasting "7" as though it were a settled fact is exactly the kind of
# quiet overstatement §5 exists to prevent, so these go to
# GENERATE_GROUNDED where the caveat travels with the number and Matt
# sees the reasoning before it goes in.

_WS_RE = re.compile(r"\s+")


def classify_question(question: str) -> str:
    q = _WS_RE.sub(" ", (question or "").strip().lower())
    if not q:
        return "NEEDS_REVIEW"
    if any(p in q for p in NEEDS_REVIEW_PATTERNS):
        return "NEEDS_REVIEW"
    # Open-ended prompts and factual-capability questions both get a
    # grounded draft that Matt reviews before anything is filled.
    if any(p in q for p in (
        "why", "describe", "tell us", "what interests", "experience",
        "have you", "do you have", "how would", "explain", "share",
        "years", "familiar", "comfortable", "worked with",
    )):
        return "GENERATE_GROUNDED"
    return "NEEDS_REVIEW"


SYSTEM_PROMPT = """You are drafting an answer to one question on a job application, on behalf \
of a specific candidate, using only their real accomplishment inventory and career facts. \
The answer goes into a real application under their name.

Rules:
- Never claim experience, a tool, a number, or a credential that isn't in the evidence given.
- If the honest answer is "no" or "limited," say so plainly and briefly. Do not spin a gap into \
  a strength, and do not pad a negative answer with adjacent experience to make it look positive. \
  A candidate caught overstating on an application is worse off than one who answered "no."
- Adjacent is not the same. When a question asks about a specific professional context — \
  consulting or professional services, agency work, startup experience, management, a named \
  industry — analogous work in a different context does not qualify, and arguing that it does is \
  the overstatement this rule exists to prevent. Running product pilots with external \
  organizations is not consulting; leading a project is not managing people; using a vendor's \
  product is not working for that vendor. If the literal thing asked about isn't in the \
  evidence, the answer is no, even when something nearby looks close enough to argue for.
- Judge the question against the candidate's whole record, not just the parts that support a \
  flattering answer. Understating is as damaging as overstating: answering "no" to a threshold \
  the candidate actually clears can disqualify them outright.
- Match the question's expected form: a yes/no question gets a direct yes or no first, then at \
  most a sentence or two of substantiation. An open-ended question gets a short paragraph.
- Write plainly, first person, no buzzwords, no enthusiasm-performance.

Respond with ONLY a JSON object, no markdown fences:
{
  "answer": "<the drafted answer, ready to paste>",
  "grounded_in": ["<inventory record id or career fact this rests on>"],
  "is_honest_negative": <true if this answer concedes a gap or says no>,
  "confidence": "<high|medium|low — low means Matt really should rewrite this>"
}"""


def draft_answer(bedrock, question: str, records: list, career_facts: dict, posting: dict) -> dict:
    """One Bedrock call per question. `records` should already be the
    lane-filtered compact inventory."""
    user_prompt = f"""QUESTION ON THE APPLICATION:
{question}

ROLE BEING APPLIED FOR:
{posting.get('title', '')} at {posting.get('company_name', '')}

CANDIDATE CAREER FACTS (confirmed, citable directly):
{json.dumps(career_facts, indent=2)}

CANDIDATE ACCOMPLISHMENT INVENTORY ({len(records)} records):
{records}"""

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1200,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    resp = bedrock.invoke_model(modelId=MODEL_ID, body=json.dumps(body))
    payload = json.loads(resp["body"].read())
    text = "".join(b.get("text", "") for b in payload.get("content", []) if b.get("type") == "text")

    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON in model response: {text[:200]!r}")
    return json.loads(text[start : end + 1])
