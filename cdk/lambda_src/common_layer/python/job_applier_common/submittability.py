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


def _tokens(title: str) -> set:
    return {w for w in re.split(r"[^a-z0-9]+", (title or "").lower()) if len(w) > 2}


def _overlap(a: str, b: str) -> float:
    """Cheap token-overlap score — used only for ranking/logging now, not
    for the accept/reject decision. See _subset_match for why."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / min(len(ta), len(tb))


def _subset_match(a: str, b: str) -> bool:
    """True only if one title's tokens sit entirely inside the other's —
    every word in the shorter title has to actually appear in the
    longer one, no exceptions.

    Confirmed live 2026-09-03 this is the check that actually matters,
    not a ratio: "Senior Data Scientist, RC Capital" against RevenueCat's
    real, live "Senior Data Scientist, Product" req scored 0.75 overlap
    — comfortably clearing any reasonable ratio threshold — because both
    are short titles sharing "data" and "scientist," and a 2-of-3-tokens
    ratio doesn't care which specific word supplies the third. A ratio
    can't distinguish "these are the same role, reworded" from "these
    are two different roles that happen to share their boilerplate,"
    because in both cases most of the tokens overlap and it's the *one*
    differentiating word doing all the work. Subset containment asks the
    right question directly: is there a word in either title that's
    simply absent from the other? "capital" isn't in the Product req's
    title and "product" isn't in the RC Capital listing's — that
    asymmetry is exactly a differentiator, not paraphrasing, and no
    ratio threshold catches it because the ratio was never measuring
    that.

    A real paraphrase looks different under this test: an aggregator's
    "Applied Data Scientist / Machine Learning Engineer (Decision
    Intelligence)" against an employer's own plainer "Machine Learning
    Engineer" has every one of the employer's (fewer) tokens present in
    the aggregator's (more verbose) title — true containment, correctly
    accepted — because the extra words are elaboration, not a different
    business unit's name standing in for another."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return False
    return ta <= tb or tb <= ta


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
    best, best_score, second_score = None, 0.0, 0.0
    try:
        for job in fetcher(token):
            if _title_key(job.get("title", "")) == want:
                return job.get("url") or direct_apply_url(
                    platform, token, job.get("external_id", "")
                ), f"matched exactly on {platform} board '{token}'"
            score = _overlap(title, job.get("title", ""))
            if score > best_score:
                best, best_score, second_score = job, score, best_score
            elif score > second_score:
                second_score = score
    except Exception as e:  # noqa: BLE001 — a board that won't load is not submittable today
        return "", f"could not read {platform} board '{token}': {e}"

    # Both conditions are load-bearing, not redundant. _subset_match is
    # the actual accept/reject gate (see its docstring — a ratio alone
    # confidently matched "RC Capital" to RevenueCat's real "Product" req
    # at 0.75, a wrong-specific-req match worse than rejecting). The
    # score/margin pair on top of it still matters when a title
    # genuinely IS a token subset of more than one open req (a company
    # running several near-identically-worded reqs, e.g. "Data Scientist"
    # postings for three different teams that only differ by a suffix
    # short enough to also get swallowed by containment) — in that case
    # prefer the strongest, clearly-separated candidate over a coin flip.
    if (
        best
        and best_score >= 0.6
        and (best_score - second_score) >= 0.15
        and _subset_match(title, best.get("title", ""))
    ):
        return best.get("url") or direct_apply_url(
            platform, token, best.get("external_id", "")
        ), f"matched '{best.get('title')}' on {platform} board '{token}' ({best_score:.0%})"

    if best and best_score >= 0.6:
        return "", (
            f"'{title}' is close to but not confidently the same req as "
            f"'{best.get('title')}' on {company}'s {platform} board ({best_score:.0%} overlap, "
            f"runner-up {second_score:.0%}) — applying to the wrong specific req is worse than "
            "not applying"
        )

    return "", (
        f"'{title}' is not on {company}'s live {platform} board "
        f"(closest was {best_score:.0%}) — stale or ghost listing"
    )
