"""job-applier submission watcher — closes the last gap in HANDOFF.md §6.

Matt's words: "A response of ok, followed by a manual local python script
launch, is unnecessarily burdensome... the user should not need to do
more than hit ok, fill out a few fields that need review in an
automatically launched browser, a captcha, and hit submit."

So this sits in the background and does the launching. Reply "ok" in
Gmail; within about ten minutes the reply listener flips the row to
APPROVED, and within a minute of that a Terminal window and a Chrome
window open by themselves with the form already filled.

Two design choices worth stating, both forced by things that already went
wrong in this build:

  * It polls DynamoDB, never SQS. Every `receive_message` on the
    submission queue counts as a delivery attempt, and maxReceiveCount is
    3 — a watcher polling the queue every minute would dead-letter every
    real approval within three minutes. The applications table is the
    authoritative state anyway; the queue is a doorbell, and only the
    worker is allowed to ring it.

  * It launches the worker in a real Terminal window rather than running
    it in-process. The worker asks real questions ("did you submit it?",
    "accept / edit / skip this drafted answer?") and needs a tty to do
    it. A launchd job has no tty, so a worker running inside this process
    would block forever on the first input() with nobody watching.

    python3 watcher.py            # run in the foreground, ctrl-C to stop
    python3 watcher.py --once     # one check, for testing
    ./install-watcher.sh          # run it at login, via launchd
"""
import argparse
import json
import os
import subprocess
import sys
import time

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from worker import (  # noqa: E402
    APPLICATIONS_TABLE,
    RUN_LOCK,
    is_paused,
)

REGION = "us-east-1"
PROFILE = os.environ.get("AWS_PROFILE", "job-applier")
POLL_SECONDS = int(os.environ.get("JOB_APPLIER_POLL_SECONDS", "60"))
# --review until the first real submission has actually gone through; flip
# to "auto" once it has. Steady state per ARCHITECTURE.md §3 is auto — the
# approval email is the human gate, and stopping again at the browser
# gates one decision twice.
MODE = os.environ.get("JOB_APPLIER_WORKER_MODE", "review")
HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.join(HERE, ".venv", "bin", "python")


def _notify(title: str, message: str):
    try:
        subprocess.run(
            ["osascript", "-e",
             f'display notification {json.dumps(message)} with title {json.dumps(title)} '
             f'sound name "Glass"'],
            capture_output=True, timeout=10,
        )
    except Exception:  # noqa: BLE001
        pass  # a missing notification is not a reason to skip the submission


def _worker_running() -> bool:
    try:
        os.kill(int(open(RUN_LOCK).read().strip()), 0)
        return True
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        return False


def _pending_approvals(applications) -> list:
    """APPROVED rows whose deferral (if any) has passed. Paginated —
    a bare scan() returns partial results with no error."""
    now = int(time.time())
    items, kwargs = [], {
        "FilterExpression": "#s = :approved",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":approved": "APPROVED"},
    }
    while True:
        resp = applications.scan(**kwargs)
        items += resp.get("Items", [])
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return [a for a in items if int(a.get("deferred_until", 0)) <= now]


def _launch_worker(count: int):
    flags = "--loop" + (" --review" if MODE == "review" else "")
    command = f"cd {HERE!r} && {PYTHON!r} worker.py {flags}"
    script = (
        f'tell application "Terminal"\n'
        f'  activate\n'
        f'  do script {json.dumps(command)}\n'
        f'end tell'
    )
    subprocess.run(["osascript", "-e", script], capture_output=True, timeout=30)
    _notify(
        "job-applier",
        f"{count} approved application{'s' if count > 1 else ''} — opening the form.",
    )
    print(f"launched the worker for {count} approved application(s), mode={MODE}")


def check_once(applications) -> int:
    if is_paused():
        print("kill switch ON — not launching anything")
        return 0
    if _worker_running():
        return 0
    pending = _pending_approvals(applications)
    if not pending:
        return 0
    for app in pending:
        print(f"  APPROVED: {app.get('company_name','?')} — {app.get('title','?')}")
    _launch_worker(len(pending))
    return len(pending)


def main() -> int:
    ap = argparse.ArgumentParser(description="watch for approved applications and open the form")
    ap.add_argument("--once", action="store_true", help="check once and exit")
    args = ap.parse_args()

    applications = boto3.Session(
        profile_name=PROFILE, region_name=REGION
    ).resource("dynamodb").Table(APPLICATIONS_TABLE)

    if args.once:
        found = check_once(applications)
        print("nothing approved and waiting" if not found else f"{found} waiting")
        return 0

    print(f"watching for approved applications every {POLL_SECONDS}s (mode={MODE}); ctrl-C to stop")
    while True:
        try:
            check_once(applications)
        except Exception as e:  # noqa: BLE001
            # A watcher that dies on a transient AWS error is a watcher
            # that silently stops watching — which is how an approval
            # goes unnoticed for a week.
            print(f"check failed ({type(e).__name__}: {e}); retrying next tick")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
