#!/usr/bin/env python3
"""Re-score the existing SCORED backlog against the rewritten fit-scoring
prompt (HANDOFF.md §5).

Why this exists as a script rather than a few aws-cli calls: the blind
calibration exercise showed the old prompt rejecting 20% of the roles
Matt actually wants — five postings he labeled YES scored 25-42, all
below the threshold of 60, so they were never promoted and never seen.
Those postings are still sitting in the table carrying those scores. The
funnel cannot surface them; only a re-score can.

Three phases, in this order for a reason:

  1. Reset SCORED -> NEW, stashing the old score in `prev_fit_score` so
     the rewrite can actually be evaluated afterward instead of taken on
     faith. QUALIFIED postings are left alone — they have live approval
     emails out, and silently un-qualifying something already sitting in
     Matt's inbox is worse than a stale score.

  2. Score in batches with `skip_gate`, so pass 2 never runs between
     batches. Without that, whatever gets re-scored first takes the cap
     slots, kicks off an Opus generation run, and then gets displaced
     when a better posting comes back two batches later — paying for a
     draft nobody sees.

  3. One normal invocation at the end, gating the whole re-scored pool at
     once. That is the comparison §5's "always highest fit-score first"
     is actually asking for.

    ./cdk/scripts/rescore_backlog.py --dry-run   # what would change
    ./cdk/scripts/rescore_backlog.py             # do it
"""
import argparse
import json
import subprocess
import sys
import time

import boto3

PROFILE = "job-applier"
REGION = "us-east-1"
TABLE = "job-applier-postings"
FUNCTION = "job-applier-fit-scoring"
RULE = "job-applier-fit-scoring-schedule"
MAX_ITERS = 20


def session():
    return boto3.Session(profile_name=PROFILE, region_name=REGION)


def scan_all(ddb, **kw):
    """Paginated, always. A bare scan() returns partial results with no
    error — that produced a '2 QUALIFIED' reading when there were 10."""
    items, lek = [], None
    while True:
        if lek:
            kw["ExclusiveStartKey"] = lek
        resp = ddb.scan(TableName=TABLE, **kw)
        items += resp.get("Items", [])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            return items


def by_status(ddb, status):
    return scan_all(
        ddb,
        FilterExpression="#s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": {"S": status}},
    )


def reset(ddb, dry_run):
    scored = by_status(ddb, "SCORED")
    print(f"phase 1: {len(scored)} SCORED postings to reset")
    if dry_run:
        for p in scored[:5]:
            print(f"  would reset {p['posting_id']['S']} (was {p.get('fit_score',{}).get('N','?')})")
        print(f"  ... and {max(0, len(scored)-5)} more")
        return len(scored)
    for i, p in enumerate(scored, 1):
        old = p.get("fit_score", {}).get("N")
        expr = "SET #s = :new REMOVE fit_score"
        names = {"#s": "status"}
        values = {":new": {"S": "NEW"}}
        if old is not None:
            expr = "SET #s = :new, prev_fit_score = :prev REMOVE fit_score"
            values[":prev"] = {"N": old}
        ddb.update_item(
            TableName=TABLE,
            Key={"posting_id": p["posting_id"]},
            UpdateExpression=expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        if i % 50 == 0:
            print(f"  reset {i}/{len(scored)}")
    print(f"  reset {len(scored)} -> NEW")
    return len(scored)


def invoke(payload):
    cmd = [
        "aws", "lambda", "invoke", "--function-name", FUNCTION,
        "--profile", PROFILE, "--region", REGION,
        "--cli-read-timeout", "950",
        "--payload", json.dumps(payload), "--cli-binary-format", "raw-in-base64-out",
        "/tmp/rescore_result.json",
    ]
    subprocess.run(cmd, capture_output=True, text=True, timeout=1000)
    try:
        return json.load(open("/tmp/rescore_result.json"))
    except Exception:  # noqa: BLE001
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    s = session()
    ddb = s.client("dynamodb")
    events = s.client("events")

    n = reset(ddb, args.dry_run)
    if args.dry_run:
        pending = len(by_status(ddb, "NEW"))
        print(f"\nphase 2 would score {n + pending} postings ({pending} already NEW)")
        print("phase 3 would run the gate once over the whole re-scored pool")
        return 0

    # The 4-hour schedule firing mid-re-score would run pass 2 on a
    # half-re-scored pool — the exact thing skip_gate exists to prevent.
    disabled = False
    try:
        events.disable_rule(Name=RULE)
        disabled = True
        print(f"phase 2: disabled {RULE} for the duration")
    except Exception as e:  # noqa: BLE001
        print(f"phase 2: could not disable {RULE} ({e}); continuing — a "
              f"scheduled run would only cost a few wasted generations, "
              f"since phase 3 re-ranks everything anyway")

    try:
        prev = -1
        for i in range(1, MAX_ITERS + 1):
            remaining = len(by_status(ddb, "NEW"))
            print(f"  iteration {i}: NEW={remaining}")
            if remaining == 0:
                print("  backlog scored")
                break
            if remaining == prev:
                print(f"  NEW unchanged at {remaining} — remaining ones are erroring, stopping")
                break
            prev = remaining
            stats = invoke({"skip_gate": True})
            print(f"    -> {stats}")
            time.sleep(2)
    finally:
        if disabled:
            events.enable_rule(Name=RULE)
            print(f"  re-enabled {RULE}")

    print("phase 3: gating the full re-scored pool")
    print(f"  -> {invoke({'only_gate': True})}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
