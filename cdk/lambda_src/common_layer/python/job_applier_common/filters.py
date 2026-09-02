"""Title matching and remote detection — ARCHITECTURE.md §1 Dedup+Filter.

Shared by both ingestion Lambdas so the filtering rule lives in exactly
one place. Stdlib only (no third-party deps) so the layer needs no
bundling/compilation step.
"""
import re

TITLE_PATTERN = re.compile(
    r"data scientist|machine learning|ml engineer|ai engineer|applied scientist"
    r"|data science|\bai\b|\bllm\b|artificial intelligence",
    re.IGNORECASE,
)

# Layer 2 of the three-layer remote check (§1): expanded string-match
# beyond the literal word "remote", with explicit negatives so "hybrid" or
# "remote flexible" don't get miscounted just for containing "remote".
_REMOTE_POSITIVE = re.compile(
    r"\bremote\b|\bdistributed\b|work[\s-]from[\s-]home|\bwfh\b"
    r"|remote[\s-]first|\banywhere\b",
    re.IGNORECASE,
)
_REMOTE_NEGATIVE = re.compile(
    r"\bhybrid\b|remote[\s-]?(days?|flexible|optional)|on[\s-]?site|in[\s-]?office",
    re.IGNORECASE,
)


def title_matches(title: str) -> bool:
    return bool(title and TITLE_PATTERN.search(title))


def detect_remote(location_text: str, is_remote_flag=None) -> str:
    """Three-layer remote detection. Returns "confirmed_remote",
    "not_remote", or "ambiguous" (left for the fit-scoring Lambda, §1, to
    resolve from full JD text once phase 3 exists — not dropped here).

    Layer 1: a structured field, when the source provides one (Ashby's
    isRemote is authoritative). Layer 2: expanded string-match on a
    free-text location field. Layer 3 is the caller treating "ambiguous"
    as "don't drop, don't confirm yet."
    """
    if is_remote_flag is not None:
        return "confirmed_remote" if is_remote_flag else "not_remote"
    text = location_text or ""
    if _REMOTE_NEGATIVE.search(text):
        return "not_remote"
    if _REMOTE_POSITIVE.search(text):
        return "confirmed_remote"
    return "ambiguous"


def normalize_company_slug(name: str) -> str:
    """Greenhouse/Ashby-style guess: lowercase, no separators at all."""
    if not name:
        return ""
    slug = name.lower().strip()
    slug = re.sub(r"[,.]", "", slug)
    return re.sub(r"[^a-z0-9]+", "", slug)


def normalize_company_slug_hyphenated(name: str) -> str:
    """Alternate guess using hyphens — some boards (Lever especially, some
    Ashby too) use hyphenated slugs instead of a bare concatenation."""
    if not name:
        return ""
    slug = name.lower().strip()
    slug = re.sub(r"[,.]", "", slug)
    return re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
