"""job-applier-ingest-known-companies — ARCHITECTURE.md §1 Ingest Lambdas,
PRIMARY source. Polls every row in `known_companies` directly against its
ATS platform's free public API, applies the title + remote filter, and
dedups into `postings`.
"""
from job_applier_common import ats_clients, companies_store, filters, postings_store

FETCHERS = {
    "greenhouse": ats_clients.fetch_greenhouse_jobs,
    "lever": ats_clients.fetch_lever_jobs,
    "ashby": ats_clients.fetch_ashby_jobs,
}


def handler(event, context):
    stats = {
        "companies_checked": 0,
        "jobs_seen": 0,
        "title_matched": 0,
        "dropped_not_remote": 0,
        "new_postings": 0,
        "errors": 0,
    }

    for company in companies_store.list_known_companies():
        platform = company.get("ats_platform")
        token = company.get("board_token")
        fetcher = FETCHERS.get(platform)
        if not fetcher or not token:
            continue
        stats["companies_checked"] += 1
        try:
            for job in fetcher(token):
                stats["jobs_seen"] += 1
                if not filters.title_matches(job["title"]):
                    continue
                stats["title_matched"] += 1

                remote_status = filters.detect_remote(
                    job.get("location_text", ""), job.get("is_remote_flag")
                )
                if remote_status == "not_remote":
                    stats["dropped_not_remote"] += 1
                    continue

                posting_id = postings_store.make_posting_id(
                    platform, token, job["external_id"]
                )
                item = {
                    "source": platform,
                    "company_slug": token,
                    "company_name": company.get("display_name", token),
                    "title": job["title"],
                    "url": job.get("url", ""),
                    "remote_status": remote_status,
                    "source_updated_at": job.get("source_updated_at"),
                    "status": "NEW",
                }
                if postings_store.put_if_new(posting_id, item):
                    stats["new_postings"] += 1
        except Exception as e:  # noqa: BLE001 — one bad company shouldn't kill the run
            stats["errors"] += 1
            print(f"ERROR fetching {platform}/{token}: {e}")

    print(f"job-applier-ingest-known-companies stats: {stats}")
    return stats
