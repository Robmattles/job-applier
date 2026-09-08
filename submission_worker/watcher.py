"""job-applier submission watcher — closes the last gap in HANDOFF.md §6.

Matt's words: "A response of ok, followed by a manual local python script
launch, is unnecessarily burdensome... the user should not need to do
more than hit ok, fill out a few fields that need review in an
automatically launched browser, a captcha, and hit submit." And when the
first version of this shipped at ~11 minutes worst case (the cloud
reply-listener's own 10-minute EventBridge schedule, plus this watcher's
poll): "that worst case latency to reply is unacceptable. needs to be
max 30 seconds."

10 minutes was a cloud-Lambda-on-a-schedule number, chosen when nothing
else was polling Gmail. It was never actually a floor — IMAP tolerates
polling every few seconds fine for one mailbox, EventBridge's schedule
granularity was the only reason it was 10 minutes rather than 1. But even
1-minute EventBridge (its actual floor) plus this watcher's own poll
doesn't reach a 30-second worst case, and every extra hop (EventBridge ->
Lambda cold start -> this watcher's next tick) is latency this doesn't
need: the reply has to be actually launched from Matt's own machine
anyway (§3 — a real visible browser, not a remote-desktop session), so
that machine might as well be the one checking Gmail too, at whatever
interval it wants, with no scheduler in between.

So this watcher now does the whole loop itself, every ~15s: check Gmail
over IMAP for a new reply on any PENDING approval (calling
reply_listener.handler.handler — the exact tested module the cloud
Lambda runs, not a reimplementation), and in the same tick check for
anything APPROVED and launch the worker for it. Reply "ok"; worst case
is one tick, ~15s, typically faster.

The cloud job-applier-reply-listener Lambda is deliberately left running
too, on its own (now 5-minute) EventBridge schedule — not for speed, this
watcher's local loop is always faster while it's running, but as the
backstop for when it isn't: a closed laptop, `--once` testing, the
watcher crashed. Both write the same DynamoDB rows through the same
idempotent classify_reply gate, so nothing about running both is unsafe;
worst case they both see the same reply and both no-op the second time,
since a decided PENDING row is a no-op to reprocess.

Two design choices worth stating, both forced by things that already went
wrong in this build:

  * The worker-launch check polls DynamoDB, never SQS. Every
    `receive_message` on the submission queue counts as a delivery
    attempt, and maxReceiveCount is 3 — a watcher polling the queue every
    tick would dead-letter every real approval within a few ticks. The
    applications table is the authoritative state anyway; the queue is a
    doorbell, and only the worker is allowed to ring it.

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
import threading
import time

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import localconfig  # noqa: E402
from worker import (  # noqa: E402
    APPLICATIONS_TABLE,
    RUN_LOCK,
    is_paused,
)

REGION = "us-east-1"
PROFILE = os.environ.get("AWS_PROFILE", "job-applier")
DOCUMENTS_BUCKET = localconfig.DOCUMENTS_BUCKET
PENDING_APPROVALS_TABLE = "job-applier-pending-approvals"
SUBMISSION_QUEUE_URL = localconfig.SUBMISSION_QUEUE_URL
GMAIL_ADDRESS = localconfig.EMAIL
GMAIL_SECRET_NAME = "job-applier-gmail-app-password"

# 15s, not 30s: a 30s POLL_SECONDS gives a 30s *average* wait, not a 30s
# worst case — a reply landing right after a tick starts would sit until
# the next one. Half the target keeps the worst case comfortably under
# it (a live IMAP round-trip for one mailbox is well under a second, so
# 15s of actual idle headroom per tick is not tight).
POLL_SECONDS = int(os.environ.get("JOB_APPLIER_POLL_SECONDS", "15"))

# The exact tested module the cloud Lambda runs (28 cases in
# cdk/tests/test_reply_classification.py cover classify_reply
# specifically) — imported and called directly rather than
# reimplemented, so there is exactly one IMAP-polling/reply-classifying
# implementation regardless of where it runs.
_LAMBDA_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cdk", "lambda_src")
sys.path.insert(0, os.path.join(_LAMBDA_SRC, "reply_listener"))
sys.path.insert(0, os.path.join(_LAMBDA_SRC, "common_layer", "python"))
os.environ.setdefault("GMAIL_ADDRESS", GMAIL_ADDRESS)
os.environ.setdefault("GMAIL_SECRET_ID", GMAIL_SECRET_NAME)
os.environ.setdefault("SUBMISSION_QUEUE_URL", SUBMISSION_QUEUE_URL)
os.environ.setdefault("APPLICATIONS_TABLE", APPLICATIONS_TABLE)
os.environ.setdefault("PENDING_APPROVALS_TABLE", PENDING_APPROVALS_TABLE)
os.environ.setdefault("DOCUMENTS_BUCKET", DOCUMENTS_BUCKET)
import handler as reply_listener  # noqa: E402  — cdk/lambda_src/reply_listener/handler.py
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


# In-process cooldown, on top of worker.py's own now-atomic RUN_LOCK
# (§ worker.py `_take_run_lock` docstring), keyed per application_id
# rather than a single global timestamp.
#
# A global debounce was tried first and confirmed live 2026-09-03 not to
# hold: a real tick (Gmail IMAP round-trip plus the DynamoDB scan) can
# itself take several seconds, so a 20s window measured from the *start*
# of the previous launch had already mostly elapsed by the time the next
# tick reached its own check — it fired again almost every cycle. Worse,
# the actual trigger that day wasn't really the timing math: it was one
# specific application (an aggregator listing with nothing to fill) whose
# worker process kept exiting or getting closed without the row ever
# leaving APPROVED, so every subsequent tick saw "still approved, nothing
# running" and launched it again — a real repeat-launch loop, once every
# ~15-25s, popping a new Terminal window each time.
#
# Per-application cooldown fixes the actual failure mode directly: once
# THIS application has been launched, don't launch it again for a while,
# regardless of whether its worker process is still alive, already
# exited, or got closed by hand. 10 minutes is deliberately much longer
# than the reply-detection latency this file exists for (§ module
# docstring) — it only throttles *retrying the same already-launched
# application*, never the first launch of a new approval, so it doesn't
# trade away the speed this was built for.
_LAUNCH_COOLDOWN_SECONDS = 600
_last_launched: dict = {}  # application_id -> monotonic time of last launch


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


def _launch_worker(pending: list):
    now = time.time()
    for app in pending:
        _last_launched[app["application_id"]] = now
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
        f"{len(pending)} approved application{'s' if len(pending) > 1 else ''} — opening the form.",
    )
    print(f"launched the worker for {len(pending)} approved application(s), mode={MODE}")


def _check_gmail() -> dict:
    """One pass of the exact IMAP-poll-and-classify logic the cloud
    reply-listener runs, called in-process. Its own early-outs make this
    cheap on a quiet tick: `_pending_rows()` scans pending_approvals
    first and does zero IMAP work at all when nothing is awaiting a
    decision, which is the common case between replies."""
    return reply_listener.handler({}, None)


def check_once(applications) -> int:
    if is_paused():
        print("kill switch ON — not checking anything")
        return 0

    try:
        gmail_stats = _check_gmail()
    except Exception as e:  # noqa: BLE001
        # IMAP hiccups happen (a transient Gmail error, a network blip).
        # One failed tick 15s before the next is not worth crashing the
        # whole watcher over — see the same reasoning in main()'s loop.
        print(f"Gmail check failed ({type(e).__name__}: {e}); retrying next tick")
        gmail_stats = {}
    if gmail_stats.get("approved"):
        print(f"  reply detected -> {gmail_stats['approved']} approved")

    if _worker_running():
        return 0
    now = time.time()
    pending = [
        a for a in _pending_approvals(applications)
        if now - _last_launched.get(a["application_id"], 0) >= _LAUNCH_COOLDOWN_SECONDS
    ]
    if not pending:
        return 0
    for app in pending:
        print(f"  APPROVED: {app.get('company_name','?')} — {app.get('title','?')}")
    _launch_worker(pending)
    return len(pending)


_last_tick = time.time()
_WATCHDOG_TIMEOUT = int(os.environ.get("JOB_APPLIER_WATCHDOG_SECONDS", "300"))


def _heartbeat():
    global _last_tick
    _last_tick = time.time()


def _start_watchdog():
    """Force-exits the process if a tick ever wedges, so launchd restarts it.

    Confirmed live 2026-09-04: the watcher spent 8 hours alive but frozen.
    Matt's laptop slept mid-IMAP-search, the TCP connection died without
    raising, and imaplib blocked on the socket read indefinitely — process
    running, `launchctl list` reporting it healthy, log frozen on
    "searching [Gmail]/All Mail". He replied "ok" and no browser opened.

    A socket timeout on the IMAP connection (reply_listener.IMAP_TIMEOUT)
    fixes that specific hang, but the general problem is that KeepAlive
    only restarts a process that *exits* — a hang is invisible to it, and
    strictly worse than a crash. This thread is the backstop for any future
    wedge, wherever it happens: it runs independently of the blocked main
    loop, so it still fires when the loop cannot.

    os._exit rather than sys.exit deliberately — sys.exit only raises in
    this thread, which the wedged main thread would never notice."""
    def _watch():
        while True:
            time.sleep(30)
            stalled = time.time() - _last_tick
            if stalled > _WATCHDOG_TIMEOUT:
                print(f"WATCHDOG: no completed tick in {stalled:.0f}s — exiting so launchd restarts")
                sys.stdout.flush()
                os._exit(1)
    threading.Thread(target=_watch, daemon=True).start()


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

    print(f"watching Gmail + approved applications every {POLL_SECONDS}s (mode={MODE}); ctrl-C to stop")
    _start_watchdog()
    while True:
        try:
            check_once(applications)
            _heartbeat()
        except Exception as e:  # noqa: BLE001
            # A watcher that dies on a transient AWS error is a watcher
            # that silently stops watching — which is how an approval
            # goes unnoticed for a week.
            print(f"check failed ({type(e).__name__}: {e}); retrying next tick")
            _heartbeat()  # it failed, but it's alive and looping — not wedged
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
