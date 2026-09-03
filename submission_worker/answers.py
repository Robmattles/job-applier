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

Grounding rules:
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

Writing rules — this is a real person answering a real question, not marketing copy, and it \
gets read right next to the résumé these same rules already govern.

Do not use the em-dash character anywhere in the answer. Not one, not for an aside, not for a \
list, not for a pause before a conclusion. This is an absolute rule, not a style preference — \
break a sentence into two, use a comma, a colon, or parentheses instead. Models default to \
em-dashes constantly and it is the single most recognizable tell in generated text; a soft \
"avoid overusing" instruction does not work, so treat every em-dash you're about to write as a \
mistake to fix before answering.

Also avoid, specifically:
- Narrative/spatial metaphors and definitional flourishes for plain facts ("where those threads \
  converge," "sits squarely in the domain of," "is exactly what X is," "that's what X work is"). \
  Just state the overlap or the fact.
- Rule-of-three listing ("fast, reliable, and scalable") and stock transitions ("moreover," \
  "additionally," "it's worth noting").
- A contrastive close that sounds like a tagline ("I'd rather do X at a company where Y than Z as \
  one workstream among many"). End on a fact or a plain statement of interest instead.
- Buzzwords without a specific number or system behind them ("passionate," "excited," \
  "leverage," "robust," "cutting-edge").
- Uniform sentence rhythm — vary length and structure the way someone actually talking does, \
  not [claim][evidence][claim][evidence] on repeat.

Write the way a competent person writes when they're not performing enthusiasm: plain, first \
person, specific, a little understated. Naming the actual system or number beats naming the \
abstract category it belongs to. Before you finalize the answer, reread it once for em-dashes \
and rewrite any you find.

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
    parsed = json.loads(text[start : end + 1])

    # The prompt states the em-dash ban as absolute, and empirically it
    # mostly holds (0/4 in a live retest after the ban was added, versus
    # 2-5/answer before it) — but "mostly" isn't the same as "absolute,"
    # and Matt named this specific tell directly. A deterministic rescue
    # costs nothing and makes the rule actually unbreakable rather than
    # just usually-followed: any stray em-dash becomes a comma, which
    # reads fine in the parenthetical/list contexts the model uses it for.
    if "—" in (parsed.get("answer") or ""):
        parsed["answer"] = parsed["answer"].replace(" — ", ", ").replace("—", ", ")
    return parsed
