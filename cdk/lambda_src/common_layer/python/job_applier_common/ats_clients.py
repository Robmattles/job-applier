"""HTTP clients for every ingestion source — ARCHITECTURE.md §1.

All endpoints and field names below were live-tested 2026-09-01/02, not
taken from docs alone (see git history around that date for the raw
test output). Stdlib `urllib.request` only, deliberately — no `requests`
dependency means no bundling step for the Lambda layer.
"""
import json
import urllib.error
import urllib.parse
import urllib.request

USER_AGENT = "job-applier/1.0 (personal automation; contact: you@example.com)"

# Confirmed 2026-09-01: RemoteOK's `data-science` tag silently ignores the
# filter and returns the unfiltered firehose instead of erroring — do not
# add tags here without testing the actual response first.
REMOTEOK_VERIFIED_TAGS = {"machine-learning"}


def _get_json(url: str, timeout: int = 15):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def probe_ats_board(platform: str, slug: str) -> bool:
    """Name-probing discovery (§3): does `slug` resolve to a real board?"""
    try:
        if platform == "greenhouse":
            data = _get_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            return "jobs" in data
        if platform == "lever":
            data = _get_json(f"https://api.lever.co/v0/postings/{slug}?mode=json")
            return isinstance(data, list)
        if platform == "ashby":
            data = _get_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            return "jobs" in data
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError):
        return False
    return False


def fetch_greenhouse_jobs(board_token: str):
    data = _get_json(f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs")
    for j in data.get("jobs", []):
        yield {
            "external_id": str(j.get("id")),
            "title": j.get("title", "") or "",
            "location_text": (j.get("location") or {}).get("name", "") or "",
            "url": j.get("absolute_url", "") or "",
            "is_remote_flag": None,
            "source_updated_at": j.get("updated_at"),
        }


def fetch_lever_jobs(company: str):
    data = _get_json(f"https://api.lever.co/v0/postings/{company}?mode=json")
    for j in data:
        cats = j.get("categories") or {}
        yield {
            "external_id": str(j.get("id")),
            "title": j.get("text", "") or "",
            "location_text": cats.get("location", "") or "",
            "url": j.get("hostedUrl", "") or "",
            "is_remote_flag": None,
            "source_updated_at": None,
        }


def fetch_ashby_jobs(board_name: str):
    data = _get_json(f"https://api.ashbyhq.com/posting-api/job-board/{board_name}")
    for j in data.get("jobs", []):
        yield {
            "external_id": str(j.get("id")),
            "title": j.get("title", "") or "",
            "location_text": j.get("location", "") or "",
            "url": j.get("jobUrl") or j.get("applyUrl", "") or "",
            "is_remote_flag": j.get("isRemote"),  # authoritative, layer 1
            "source_updated_at": None,
        }


def fetch_himalayas_jobs(query: str, seniority: str = "Senior,Manager,Director"):
    q = urllib.parse.quote(query)
    url = (
        f"https://himalayas.app/jobs/api/search?q={q}"
        f"&seniority={urllib.parse.quote(seniority)}&sort=recent"
    )
    data = _get_json(url)
    for j in data.get("jobs", []):
        yield {
            "external_id": j.get("guid") or j.get("applicationLink", ""),
            "title": j.get("title", "") or "",
            "company_name": j.get("companyName", "") or "",
            "url": j.get("applicationLink", "") or "",
            "source_published_at": j.get("pubDate"),
        }


def fetch_jobicy_jobs(tag: str = "data", count: int = 100):
    url = f"https://jobicy.com/api/v2/remote-jobs?count={count}&tag={urllib.parse.quote(tag)}"
    data = _get_json(url)
    for j in data.get("jobs", []):
        yield {
            "external_id": str(j.get("id")),
            "title": j.get("jobTitle", "") or "",
            "company_name": j.get("companyName", "") or "",
            "url": j.get("url", "") or "",
            "source_published_at": j.get("pubDate"),
        }


def fetch_remoteok_jobs(tag: str = "machine-learning"):
    if tag not in REMOTEOK_VERIFIED_TAGS:
        raise ValueError(
            f"RemoteOK tag {tag!r} has not been verified to actually filter "
            f"(see REMOTEOK_VERIFIED_TAGS) — test it manually before adding it."
        )
    data = _get_json(f"https://remoteok.com/api?tag={urllib.parse.quote(tag)}")
    for j in data:
        if "position" not in j:
            continue  # first array item is RemoteOK's legal notice, not a job
        yield {
            "external_id": str(j.get("id")),
            "title": j.get("position", "") or "",
            "company_name": j.get("company", "") or "",
            "url": j.get("url", "") or "",
            "source_published_at": j.get("date"),
        }
