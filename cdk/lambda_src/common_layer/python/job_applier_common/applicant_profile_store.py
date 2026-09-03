"""Loads applicant-profile.json from S3, cached per warm Lambda container
— same pattern as inventory_store.load_inventory().

Generation and QA need this in addition to the accomplishment inventory:
confirmed live 2026-09-02, a phData posting requiring "6+ years AI/ML
experience" went to NEEDS_REVIEW because the atomic accomplishment
records (individual project bullets) don't carry an aggregate years-of-
-experience fact — but applicant-profile.json's own experience_summary
does (`years_data_science_ml: 7`), Matt-confirmed, not an LLM guess. The
render Lambda also needs `identity` for the PDF header (name/contact)."""
import json
import os

import boto3

_cache = None


def load_profile() -> dict:
    global _cache
    if _cache is None:
        s3 = boto3.client("s3")
        bucket = os.environ["DOCUMENTS_BUCKET"]
        key = os.environ.get("APPLICANT_PROFILE_KEY", "source/applicant-profile.json")
        obj = s3.get_object(Bucket=bucket, Key=key)
        _cache = json.loads(obj["Body"].read().decode("utf-8"))
    return _cache


def career_summary_facts(profile: dict) -> dict:
    """The subset of the profile that's safe and appropriate to cite in
    generated résumé/cover-letter content — aggregate career facts, not
    per-project evidence. Deliberately excludes `voluntary_self_
    identification` (that section's own note: "never something an LLM
    should generate") and anything else not resume-appropriate (comp
    figures, work authorization, logistics) — this is a grounding source
    for content generation, not the full form-autofill profile."""
    exp = profile.get("experience_summary", {})
    return {
        "total_years_professional_experience": exp.get("total_years_professional_experience"),
        "years_data_science_ml": exp.get("years_data_science_ml"),
        # education (list) is the full record — highest_degree is kept
        # only as a single-string convenience for old callers, but the
        # renderer prints every entry in education, not just this one.
        # Confirmed live 2026-09-02: highest_degree alone silently
        # dropped Yale (B.A., Magna Cum Laude, Phi Beta Kappa) off every
        # rendered résumé's Education section — Matt caught it, not QA.
        "highest_degree": exp.get("highest_degree"),
        "education": exp.get("education", []),
        "notable_certifications": exp.get("notable_certifications", []),
        "current_employer_name": profile.get("current_employment", {}).get("current_employer_name"),
    }
