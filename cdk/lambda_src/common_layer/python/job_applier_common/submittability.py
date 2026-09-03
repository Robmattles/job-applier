"""Can this posting actually be applied to? — ARCHITECTURE.md §5.

Confirmed live 2026-09-03, and the reason this module exists: a WorkWave
posting sourced from Jobicy scored 88, cleared both QA passes, generated
a résumé and cover letter, consumed one of that week's ten cap slots,
and reached Matt's inbox for approval — for a job with no reachable
application form anywhere. Jobicy exposes no employer link (it wants an
account on Jobicy), and WorkWave's own Lever board had nine open roles,
none of them in data science or ML. The listing was stale or a ghost.

Nothing upstream had any notion of whether a posting could be applied
to, so the entire pipeline ran end to end on a job that didn't exist.
Matt's call: "if it's not submittable, it's not worth worrying about."

So submittability is checked before a posting is promoted, and an
unreachable posting never spends a cap slot:

- greenhouse / lever / ashby postings carry their own board coordinates
  in the posting_id, so the application URL is constructed directly.
- himalayas / remoteok / jobicy are aggregator listings whose stored URL
  is an article, not a form. Those resolve through `known_companies` —
  the board tokens ingestion is already discovering by name-probing —
  by pulling that employer's live board and matching the title. A
  posting that doesn't match anything currently live on the employer's
  own board is exactly the stale-listing case above, and is rejected.
"""
import re

from . import ats_clients

AGGREGATOR_SOURCES = {"himalayas", "remoteok", "jobicy"}
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_LEVEL_RE = re.compile(r"\b(senior|sr|staff|principal|lead|ii|iii|i{1,3})\b\.?", re.I)


def _title_key(title: str) -> str:
    """Normalized title for cross-board matching. Aggregators routinely
    reword titles ("Applied Data Scientist / Machine Learning Engineer
    (Decision Intelligence)" vs the employer's own wording), so seniority
    words and punctuation come out before comparing."""
    t = _LEVEL_RE.sub("", (title or "").lower())
    return _NON_ALNUM_RE.sub("", t)


def _overlap(a: str, b: str) -> float:
    """Cheap token-overlap score, since exact title equality across
    boards is rare enough to be useless as a test."""
    ta = {w for w in re.split(r"[^a-z0-9]+", (a or "").lower()) if len(w) > 2}
    tb = {w for w in re.split(r"[^a-z0-9]+", (b or "").lower()) if len(w) > 2}
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / min(len(ta), len(tb))


_FETCHERS = {
    "greenhouse": ats_clients.fetch_greenhouse_jobs,
    "lever": ats_clients.fetch_lever_jobs,
    "ashby": ats_clients.fetch_ashby_jobs,
}


def direct_apply_url(source: str, company: str, external_id: str) -> str:
    if source == "greenhouse":
        return f"https://job-boards.greenhouse.io/{company}/jobs/{external_id}"
    if source == "ashby":
        # The Ashby job page has no form; the application is its own
        # route (confirmed live 2026-09-03 — the job page renders one
        # stray search box and nothing else).
        return f"https://jobs.ashbyhq.com/{company}/{external_id}/application"
    if source == "lever":
        return f"https://jobs.lever.co/{company}/{external_id}/apply"
    return ""


def resolve(posting: dict, known_company: dict = None) -> tuple:
    """Returns (apply_url, reason). apply_url empty means don't promote,
    and `reason` says why in words worth putting in front of Matt."""
    posting_id = posting.get("posting_id", "")
    parts = posting_id.split("#", 2)
    source, company, external_id = (parts + ["", ""])[:3]

    if source in _FETCHERS:
        return direct_apply_url(source, company, external_id), "direct ATS posting"

    if source not in AGGREGATOR_SOURCES:
        return "", f"unknown source '{source}'"

    if not known_company or not known_company.get("board_token"):
        return "", (
            f"{source} listing and no known ATS board for '{company}' — the stored URL is an "
            "aggregator article, not an application form"
        )

    platform = known_company.get("ats_platform", "")
    token = known_company["board_token"]
    fetcher = _FETCHERS.get(platform)
    if fetcher is None:
        return "", f"known board for '{company}' is on unsupported platform '{platform}'"

    title = posting.get("title", "")
    want = _title_key(title)
    best, best_score = None, 0.0
    try:
        for job in fetcher(token):
            if _title_key(job.get("title", "")) == want:
                return job.get("url") or direct_apply_url(
                    platform, token, job.get("external_id", "")
                ), f"matched exactly on {platform} board '{token}'"
            score = _overlap(title, job.get("title", ""))
            if score > best_score:
                best, best_score = job, score
    except Exception as e:  # noqa: BLE001 — a board that won't load is not submittable today
        return "", f"could not read {platform} board '{token}': {e}"

    # 0.6 is deliberately demanding. A loose match here means applying to
    # the wrong req at the right company, which is worse than not applying.
    if best and best_score >= 0.6:
        return best.get("url") or direct_apply_url(
            platform, token, best.get("external_id", "")
        ), f"matched '{best.get('title')}' on {platform} board '{token}' ({best_score:.0%})"

    return "", (
        f"'{title}' is not on {company}'s live {platform} board "
        f"(closest was {best_score:.0%}) — stale or ghost listing"
    )
