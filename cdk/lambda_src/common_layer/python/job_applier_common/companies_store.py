"""`known_companies` table access + name-probing discovery — §1/§3."""
import os
import time

import boto3

from . import ats_clients, filters

_table = None
ATS_PLATFORMS = ("greenhouse", "lever", "ashby")


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["KNOWN_COMPANIES_TABLE"])
    return _table


def is_known(company_slug: str) -> bool:
    if not company_slug:
        return True  # nothing to probe for an empty slug; treat as "skip"
    resp = _get_table().get_item(Key={"company_slug": company_slug})
    return "Item" in resp


def list_known_companies() -> list:
    """Scan is fine here — this table is small (tens to low hundreds of
    rows) and read a handful of times a day, not a hot path."""
    table = _get_table()
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items


def discover_and_record(display_name: str):
    """Try to resolve `display_name` to a real Greenhouse/Lever/Ashby
    board via name-probing (§3). Returns the recorded item, or None."""
    candidates = {
        filters.normalize_company_slug(display_name),
        filters.normalize_company_slug_hyphenated(display_name),
    }
    for slug in candidates:
        if not slug:
            continue
        for platform in ATS_PLATFORMS:
            if ats_clients.probe_ats_board(platform, slug):
                item = {
                    "company_slug": slug,
                    "display_name": display_name,
                    "ats_platform": platform,
                    "board_token": slug,
                    "discovered_via": "name-probing",
                    "verified_at": int(time.time()),
                    "active": True,
                }
                _get_table().put_item(Item=item)
                return item
    return None
