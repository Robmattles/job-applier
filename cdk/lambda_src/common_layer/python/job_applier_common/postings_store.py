"""Dedup + write against the `postings` table — ARCHITECTURE.md §1/§2."""
import os
import time

import boto3
from botocore.exceptions import ClientError

_table = None
DEDUP_RETENTION_DAYS = 120  # §1: "a posting reappearing after ~120 days is
                             # effectively a new opportunity, not a duplicate"


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["POSTINGS_TABLE"])
    return _table


def make_posting_id(source: str, company_key: str, external_id: str) -> str:
    return f"{source}#{company_key or 'unknown'}#{external_id}"


def put_if_new(posting_id: str, item: dict) -> bool:
    """Conditional put keyed on posting_id. Returns True if this posting
    was genuinely new, False if already seen (the actual dedup check)."""
    table = _get_table()
    item = dict(item)
    item["posting_id"] = posting_id
    item.setdefault("first_seen_at", int(time.time()))
    item["ttl"] = int(time.time()) + DEDUP_RETENTION_DAYS * 86400
    try:
        table.put_item(
            Item=item, ConditionExpression="attribute_not_exists(posting_id)"
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
