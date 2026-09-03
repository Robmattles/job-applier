"""Shared DynamoDB helpers.

`scan_all` exists because a bare `table.scan()` returns at most 1MB of
*scanned* data and stops — with a FilterExpression that limit applies to
rows examined, not rows returned, so a filtered scan over a large table
silently returns a partial answer with no error and no signal that it
was truncated.

Confirmed live 2026-09-03: an un-paginated filtered scan of the ~660-row
postings table reported 2 QUALIFIED postings when there were in fact 10,
which is exactly the kind of quiet wrongness that makes a guardrail
worse than useless — the weekly digest was reporting funnel counts off
the same broken pattern.
"""


def scan_all(table, **kwargs) -> list:
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items
