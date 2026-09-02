#!/usr/bin/env python3
"""One-time seed for `known_companies` — the 19 companies live-tested
2026-09-01 (see target-employer-list.md and ARCHITECTURE.md §1). Not part
of the CDK stack itself since seeding data isn't infrastructure — this is
an operational script, run once after the ingestion stack deploys.

Usage: python3 seed_known_companies.py --profile job-applier
"""
import argparse
import time

import boto3

# (company_slug, ats_platform, board_token, display_name)
SEED_COMPANIES = [
    # Greenhouse (14) — board token == slug for all of these
    ("coinbase", "greenhouse", "coinbase", "Coinbase"),
    ("stripe", "greenhouse", "stripe", "Stripe"),
    ("airbnb", "greenhouse", "airbnb", "Airbnb"),
    ("affirm", "greenhouse", "affirm", "Affirm"),
    ("robinhood", "greenhouse", "robinhood", "Robinhood"),
    ("reddit", "greenhouse", "reddit", "Reddit"),
    ("pinterest", "greenhouse", "pinterest", "Pinterest"),
    ("instacart", "greenhouse", "instacart", "Instacart"),
    ("twilio", "greenhouse", "twilio", "Twilio"),
    ("dropbox", "greenhouse", "dropbox", "Dropbox"),
    ("okta", "greenhouse", "okta", "Okta"),
    ("gitlab", "greenhouse", "gitlab", "GitLab"),
    ("brex", "greenhouse", "brex", "Brex"),
    ("chime", "greenhouse", "chime", "Chime"),
    # Lever (1)
    ("palantir", "lever", "palantir", "Palantir"),
    # Ashby (4)
    ("socure", "ashby", "socure", "Socure"),
    ("ramp", "ashby", "ramp", "Ramp"),
    ("notion", "ashby", "notion", "Notion"),
    ("linear", "ashby", "linear", "Linear"),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="job-applier")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--table", default="job-applier-known-companies")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    table = session.resource("dynamodb").Table(args.table)

    now = int(time.time())
    for slug, platform, token, display_name in SEED_COMPANIES:
        table.put_item(
            Item={
                "company_slug": slug,
                "ats_platform": platform,
                "board_token": token,
                "display_name": display_name,
                "discovered_via": "seed-2026-09-01-live-test",
                "verified_at": now,
                "active": True,
            }
        )
        print(f"seeded: {slug} ({platform})")

    print(f"\nDone — {len(SEED_COMPANIES)} companies seeded into {args.table}.")


if __name__ == "__main__":
    main()
