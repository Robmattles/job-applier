"""job-applier-ingest-boards — ARCHITECTURE.md §1 Ingest Lambdas,
SECONDARY sources (Himalayas, Jobicy, RemoteOK — all remote-only
platforms by construction, verified 2026-09-01, so no per-posting remote
detection needed here the way the ATS poller needs it). Also runs
name-probing discovery (§3): every company name surfaced by these
sources is a candidate for a direct Greenhouse/Lever/Ashby board too.
"""
from job_applier_common import ats_clients, companies_store, filters, postings_store

HIMALAYAS_QUERIES = [
    "data scientist",
    "machine learning engineer",
    "AI engineer",
    "applied scientist",
]
JOBICY_TAGS = ["data"]
REMOTEOK_TAGS = ["machine-learning"]  # see ats_clients.REMOTEOK_VERIFIED_TAGS


def _process(job: dict, source: str, stats: dict) -> None:
    stats["jobs_seen"] += 1
    if not filters.title_matches(job["title"]):
        return
    stats["title_matched"] += 1

    company_name = job.get("company_name", "")
    company_slug = filters.normalize_company_slug(company_name)

    posting_id = postings_store.make_posting_id(source, company_slug, job["external_id"])
    item = {
        "source": source,
        "company_name": company_name,
        "title": job["title"],
        "url": job.get("url", ""),
        "remote_status": "confirmed_remote",
        "source_published_at": job.get("source_published_at"),
        "status": "NEW",
    }
    if postings_store.put_if_new(posting_id, item):
        stats["new_postings"] += 1

    # Discovery (§3): does this company also have a direct ATS board?
    if company_slug and not companies_store.is_known(company_slug):
        try:
            if companies_store.discover_and_record(company_name):
                stats["companies_discovered"] += 1
        except Exception as e:  # noqa: BLE001
            stats["errors"] += 1
            print(f"ERROR discovering company={company_name!r}: {e}")


def handler(event, context):
    stats = {
        "jobs_seen": 0,
        "title_matched": 0,
        "new_postings": 0,
        "companies_discovered": 0,
        "errors": 0,
    }

    for q in HIMALAYAS_QUERIES:
        try:
            for job in ats_clients.fetch_himalayas_jobs(q):
                _process(job, "himalayas", stats)
        except Exception as e:  # noqa: BLE001
            stats["errors"] += 1
            print(f"ERROR himalayas query={q!r}: {e}")

    for tag in JOBICY_TAGS:
        try:
            for job in ats_clients.fetch_jobicy_jobs(tag=tag):
                _process(job, "jobicy", stats)
        except Exception as e:  # noqa: BLE001
            stats["errors"] += 1
            print(f"ERROR jobicy tag={tag!r}: {e}")

    for tag in REMOTEOK_TAGS:
        try:
            for job in ats_clients.fetch_remoteok_jobs(tag=tag):
                _process(job, "remoteok", stats)
        except Exception as e:  # noqa: BLE001
            stats["errors"] += 1
            print(f"ERROR remoteok tag={tag!r}: {e}")

    print(f"job-applier-ingest-boards stats: {stats}")
    return stats
