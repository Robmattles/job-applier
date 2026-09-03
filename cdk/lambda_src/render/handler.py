"""job-applier-render — ARCHITECTURE.md §1 Render step, §6 phase 5's
rendering half (the QA Lambda is the other half).

DynamoDB-Streams-triggered off applications_table, filtered to
NEW_IMAGE.status == "QA_PASSED" — only content that's already cleared
both QA passes gets turned into a PDF. Deliberately not LLM-driven at
all: a fixed, deterministic single-column template is what §1/§4
actually call for ("the one fixed, visually appealing and machine-
readable single-column template" / "basic ATS-parseability sanity
checks... largely guaranteed by the fixed template"). Real embedded
text via fpdf2's core Helvetica font (no image rasterization, no custom
TTF embedding) — an ATS text-extraction pass sees the same text a human
does.

Two PDFs per application: resume.pdf (grouped by role, reverse-
chronological — bullets from Generation carry a `role` field for
exactly this) and cover_letter.pdf. Both uploaded to
`generated/<application_id>/` in the documents bucket.
"""
import json
import os
import re
import time

import boto3
from boto3.dynamodb.types import TypeDeserializer
from fpdf import FPDF
from fpdf.enums import XPos, YPos

from job_applier_common import applicant_profile_store

_applications_table = None
_s3 = None
_deserializer = TypeDeserializer()

DOCUMENTS_BUCKET = os.environ.get("DOCUMENTS_BUCKET")

FONT = "Helvetica"
NAVY = (30, 41, 59)
GRAY = (100, 100, 100)


def _get_applications_table():
    global _applications_table
    if _applications_table is None:
        _applications_table = boto3.resource("dynamodb").Table(os.environ["APPLICATIONS_TABLE"])
    return _applications_table


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _deserialize_stream_image(image: dict) -> dict:
    return {k: _deserializer.deserialize(v) for k, v in image.items()}


# --------------------------------------------------------------------------
# Role-group ordering — reverse-chronological, parsed from the "Company —
# Title (Dates)" role string every record (and now every generated bullet)
# carries. The date range is always the LAST parenthetical in the string
# (some roles have an extra parenthetical earlier, e.g. "Program Manager
# (Mgmt & Program Analyst), Public Source Program Office (Aug 2010-Jan
# 2014)"), so grab all of them and parse the last.
# --------------------------------------------------------------------------

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_PAREN_RE = re.compile(r"\(([^)]+)\)")
_DATE_START_RE = re.compile(r"([A-Za-z]{3,9})\.?\s+(\d{4})")


def _role_sort_key(role: str) -> tuple:
    parens = _PAREN_RE.findall(role or "")
    if not parens:
        return (0, 0)
    m = _DATE_START_RE.search(parens[-1])
    if not m:
        return (0, 0)
    month = _MONTHS.get(m.group(1)[:3].lower(), 0)
    year = int(m.group(2))
    return (year, month)


def _group_bullets_by_role(resume_bullets: list) -> list:
    """Returns [(role_string, [bullet_text, ...]), ...] ordered most-
    recent-role-first, preserving each role's own bullet order as
    Generation wrote it."""
    groups = {}
    order = []
    for bullet in resume_bullets:
        role = bullet.get("role") or "(role not specified)"
        if role not in groups:
            groups[role] = []
            order.append(role)
        groups[role].append(bullet.get("text", ""))
    order.sort(key=_role_sort_key, reverse=True)
    return [(role, groups[role]) for role in order]


# --------------------------------------------------------------------------
# PDF builders
# --------------------------------------------------------------------------


_CHAR_MAP = str.maketrans({
    "—": "-",  # em dash — every role string uses this ("NICB — ...")
    "–": "-",  # en dash
    "‘": "'", "’": "'",  # curly single quotes
    "“": '"', "”": '"',  # curly double quotes
    "•": "-",  # bullet
    "…": "...",  # ellipsis
    # Confirmed live 2026-09-02: a pipeline-architecture bullet ("anchor
    # matching → graph expansion → ...") silently became "anchor matching
    # ? graph expansion ? ..." via the encode(errors="replace") fallback
    # below — technically not a crash, but wrong and easy to miss on
    # review. Anything LLM-generated technical writing plausibly uses
    # gets an explicit mapping instead of trusting that fallback.
    "→": "->", "←": "<-", "↔": "<->",
    "×": "x", "÷": "/", "≈": "~", "≠": "!=", "≤": "<=", "≥": ">=",
})


def _sanitize(text) -> str:
    """Core Helvetica (the standard-14 PDF font, no font-file embedding —
    kept deliberately for ATS-parseable real text with zero rendering
    fragility) doesn't cover full Unicode. Confirmed live 2026-09-02:
    plain "•" alone crashed the render. LLM-generated text and the
    inventory's own role strings ("NICB — Senior...") routinely carry
    smart punctuation, so every piece of text is normalized through here
    rather than trusting any one source to stay ASCII-safe — anything
    still outside latin-1 after the explicit swaps degrades to a
    replacement character instead of crashing the whole render."""
    if text is None:
        return ""
    return str(text).translate(_CHAR_MAP).encode("latin-1", errors="replace").decode("latin-1")


class _SafeFPDF(FPDF):
    """cell()/multi_cell() sanitized at the source rather than trusting
    every call site to remember — one missed call site is exactly how the
    live crash above happened.

    Also confirmed live 2026-09-02: multi_cell's/cell's default
    new_x=XPos.RIGHT leaves the cursor at the right edge of whatever was
    just drawn, not back at the left margin — fine for table-style
    layouts, wrong for this document's plain flowing paragraphs. An
    indented bullet using nearly the full page width left the cursor a
    hair from the right margin, and the next full-width multi_cell(0,
    ...) call computed "remaining space to the right margin from here"
    as ~0, raising "Not enough horizontal space to render a single
    character" on a section that had nothing wrong with its own text.
    Only overriding new_x here, not new_y — each method's own default Y
    behavior (cell: stays put; multi_cell: advances) is what the
    explicit .ln() calls throughout this file were already written
    assuming; changing that too would double-count line advances."""

    def cell(self, w=None, h=None, text="", *args, **kwargs):
        kwargs.setdefault("new_x", XPos.LMARGIN)
        return super().cell(w, h, _sanitize(text), *args, **kwargs)

    def multi_cell(self, w, h=None, text="", *args, **kwargs):
        kwargs.setdefault("new_x", XPos.LMARGIN)
        return super().multi_cell(w, h, _sanitize(text), *args, **kwargs)


def _new_pdf() -> FPDF:
    pdf = _SafeFPDF(format="Letter", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.set_margins(18, 16, 18)
    pdf.add_page()
    return pdf


def _header(pdf: FPDF, identity: dict):
    pdf.set_font(FONT, "B", 18)
    pdf.set_text_color(*NAVY)
    pdf.cell(0, 9, identity.get("full_name", ""))
    pdf.ln(9)
    pdf.set_font(FONT, "", 10)
    pdf.set_text_color(*GRAY)
    contact_parts = [
        p for p in [
            identity.get("phone"),
            identity.get("email"),
            f"{identity.get('city', '')}, {identity.get('state', '')}".strip(", "),
        ] if p
    ]
    pdf.cell(0, 6, "  |  ".join(contact_parts))
    pdf.ln(9)
    pdf.set_draw_color(*GRAY)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(4)


def render_resume_pdf(content: dict, identity: dict, career_facts: dict) -> bytes:
    pdf = _new_pdf()
    _header(pdf, identity)

    pdf.set_font(FONT, "B", 13)
    pdf.set_text_color(*NAVY)
    pdf.multi_cell(0, 7, content.get("headline", ""))
    pdf.ln(1)

    pdf.set_font(FONT, "", 10.5)
    pdf.set_text_color(0, 0, 0)
    pdf.multi_cell(0, 5.5, content.get("summary", ""))
    pdf.ln(3)

    pdf.set_font(FONT, "B", 11)
    pdf.set_text_color(*NAVY)
    pdf.cell(0, 6, "Experience")
    pdf.ln(7)

    for role, bullets in _group_bullets_by_role(content.get("resume_bullets", [])):
        pdf.set_font(FONT, "B", 10.5)
        pdf.set_text_color(0, 0, 0)
        pdf.multi_cell(0, 5.5, role)
        pdf.set_font(FONT, "", 10)
        for bullet_text in bullets:
            pdf.set_x(pdf.l_margin + 4)
            pdf.multi_cell(pdf.w - pdf.l_margin - pdf.r_margin - 4, 5.2, f"-  {bullet_text}")
        pdf.ln(2)

    education = career_facts.get("education") or []
    certs = career_facts.get("notable_certifications") or []
    if education or certs:
        pdf.ln(1)
        pdf.set_font(FONT, "B", 11)
        pdf.set_text_color(*NAVY)
        pdf.cell(0, 6, "Education & Certifications")
        pdf.ln(6)
        pdf.set_font(FONT, "", 10)
        pdf.set_text_color(0, 0, 0)
        # Every entry, not just the highest degree — confirmed live
        # 2026-09-02: printing only the single highest_degree string
        # silently dropped Yale off the résumé entirely.
        for entry in education:
            line = f"{entry.get('degree', '')} - {entry.get('institution', '')}"
            if entry.get("honors"):
                line += f" ({entry['honors']})"
            pdf.multi_cell(0, 5.5, line)
        if certs:
            pdf.multi_cell(0, 5.5, ", ".join(certs))

    # Last section, deliberately — Matt's call 2026-09-02: "it's clearly
    # the weakest section, designed just for keyword hits." Real
    # narrative (Experience) and credentials (Education) lead; the
    # keyword list is a backstop for ATS parsing, not the pitch.
    skills = content.get("skills_section", [])
    if skills:
        pdf.ln(3)
        pdf.set_font(FONT, "B", 11)
        pdf.set_text_color(*NAVY)
        pdf.cell(0, 6, "Skills")
        pdf.ln(6)
        pdf.set_font(FONT, "", 10)
        pdf.set_text_color(0, 0, 0)
        pdf.multi_cell(0, 5.5, "  •  ".join(skills))

    return bytes(pdf.output())


def render_cover_letter_pdf(content: dict, identity: dict, posting: dict) -> bytes:
    pdf = _new_pdf()
    _header(pdf, identity)

    pdf.set_font(FONT, "", 10)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, time.strftime("%B %-d, %Y"))
    pdf.ln(10)

    pdf.set_font(FONT, "", 10.5)
    pdf.set_text_color(0, 0, 0)
    company = posting.get("company_name", posting.get("company_slug", "the team"))
    pdf.multi_cell(0, 5.8, f"Dear Hiring Team at {company},")
    pdf.ln(3)

    letter = content.get("cover_letter", {})
    paragraphs = [letter.get("opening", "")] + letter.get("body_paragraphs", []) + [letter.get("closing", "")]
    for para in paragraphs:
        if not para:
            continue
        pdf.multi_cell(0, 5.8, para)
        pdf.ln(3)

    pdf.multi_cell(0, 5.8, f"Sincerely,\n{identity.get('full_name', '')}")

    return bytes(pdf.output())


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def _process_one(application_id: str, new_image: dict) -> str:
    postings_table = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    posting = postings_table.get_item(Key={"posting_id": new_image["posting_id"]}).get("Item") or {}

    profile = applicant_profile_store.load_profile()
    identity = profile.get("identity", {})
    career_facts = applicant_profile_store.career_summary_facts(profile)
    content = new_image["generated_content"]

    resume_pdf = render_resume_pdf(content, identity, career_facts)
    cover_letter_pdf = render_cover_letter_pdf(content, identity, posting)

    s3 = _get_s3()
    resume_key = f"generated/{application_id}/resume.pdf"
    cover_letter_key = f"generated/{application_id}/cover_letter.pdf"
    s3.put_object(Bucket=DOCUMENTS_BUCKET, Key=resume_key, Body=resume_pdf, ContentType="application/pdf")
    s3.put_object(Bucket=DOCUMENTS_BUCKET, Key=cover_letter_key, Body=cover_letter_pdf, ContentType="application/pdf")

    _get_applications_table().update_item(
        Key={"application_id": application_id},
        UpdateExpression=(
            "SET #s = :s, resume_pdf_key = :r, cover_letter_pdf_key = :c, rendered_at = :ts"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": "RENDERED",
            ":r": resume_key,
            ":c": cover_letter_key,
            ":ts": int(time.time()),
        },
    )
    return "rendered"


def handler(event, context):
    stats = {"rendered": 0}
    for record in event.get("Records", []):
        new_image_raw = record.get("dynamodb", {}).get("NewImage")
        if not new_image_raw:
            continue
        new_image = _deserialize_stream_image(new_image_raw)
        outcome = _process_one(new_image["application_id"], new_image)
        stats[outcome] = stats.get(outcome, 0) + 1
    print(f"job-applier-render stats: {stats}")
    return stats
