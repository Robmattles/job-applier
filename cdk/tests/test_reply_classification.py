"""Tests for reply_listener.classify_reply — ARCHITECTURE.md §6 phase 6.

This is the one function standing between a typo in Matt's inbox and a
real application going to a real employer under his name, so it gets
real tests even though nothing else in this repo does yet. The cases
that matter most are the near-misses: "ok but change X" and "not ok"
both contain "ok" and must not approve.

Run: python3 cdk/tests/test_reply_classification.py
"""
import os
import sys
import types

_HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(_HERE, "..", "lambda_src", "reply_listener"))
# The Lambda gets job_applier_common from its layer; locally it has to be
# put on the path explicitly.
sys.path.insert(0, os.path.join(_HERE, "..", "lambda_src", "common_layer", "python"))

# classify_reply is pure, but the module imports boto3 at load time.
sys.modules.setdefault("boto3", types.SimpleNamespace(resource=lambda *a, **k: None, client=lambda *a, **k: None))
_dynamodb = types.ModuleType("boto3.dynamodb")
_conditions = types.ModuleType("boto3.dynamodb.conditions")
_conditions.Attr = object
sys.modules.setdefault("boto3.dynamodb", _dynamodb)
sys.modules.setdefault("boto3.dynamodb.conditions", _conditions)
os.environ.setdefault("GMAIL_ADDRESS", "x")
os.environ.setdefault("GMAIL_SECRET_ID", "x")
os.environ.setdefault("SUBMISSION_QUEUE_URL", "x")
os.environ.setdefault("PENDING_APPROVALS_TABLE", "x")
os.environ.setdefault("APPLICATIONS_TABLE", "x")

import handler  # noqa: E402

CASES = [
    ("ok", "APPROVED"),
    ("OK", "APPROVED"),
    ("ok!", "APPROVED"),
    ("Okay.", "APPROVED"),
    ("yes", "APPROVED"),
    ("y", "APPROVED"),
    ("send it", "APPROVED"),
    ("ship it", "APPROVED"),
    ("Approved", "APPROVED"),
    ("no", "REJECTED"),
    ("nope", "REJECTED"),
    ("skip", "REJECTED"),
    ("pass", "REJECTED"),
    ("not this one", "REJECTED"),
    # Near-misses: every one of these contains an affirmative token and
    # must still not approve.
    ("not ok", "AMBIGUOUS"),
    ("don't send", "AMBIGUOUS"),
    ("ok but change the summary first", "AMBIGUOUS"),
    ("ok, but hold off until I look at the cover letter", "AMBIGUOUS"),
    ("looks good to me, go ahead and send it after you fix the bullet", "AMBIGUOUS"),
    ("hold off", "AMBIGUOUS"),
    ("wait", "AMBIGUOUS"),
    ("is the salary listed anywhere?", "AMBIGUOUS"),
    ("maybe", "AMBIGUOUS"),
    ("", "AMBIGUOUS"),
]


# Real Gmail reply bodies, quoted original included. Confirmed live
# 2026-09-03: Gmail wraps the "On ... wrote:" attribution when the
# sender's name makes it long, which defeated a single-line
# `On .*wrote:` match — the entire quoted original then counted as
# reply text and a clean "ok" classified as AMBIGUOUS.
QUOTE_CASES = [
    (
        "ok\r\n\r\nOn Thu, Sep 3, 2026 at 3:53 AM <you@example.com> wrote:\r\n\r\n"
        "> WorkWave — Applied Data Scientist\r\n>\r\n> Fit score: 88   Lane: applied_mle\r\n",
        "APPROVED",
    ),
    (
        # the wrapped form that actually broke
        "ok\r\n\r\nOn Thu, Sep 3, 2026 at 3:59 AM Matthew Barr <you@example.com>\r\n"
        "wrote:\r\n\r\n> ok\r\n>\r\n>> Fit score: 88\r\n",
        "APPROVED",
    ),
    (
        "no\r\n\r\nOn Thu, Sep 3, 2026 at 3:59 AM Matthew Barr <you@example.com>\r\n"
        "wrote:\r\n\r\n> Apply to X?\r\n",
        "REJECTED",
    ),
    (
        "ok but fix the summary first\r\n\r\nOn Thu, Sep 3, 2026 at 3:59 AM Matthew Barr "
        "<you@example.com>\r\nwrote:\r\n\r\n> Apply to X?\r\n",
        "AMBIGUOUS",
    ),
]


class _FakeMessage:
    """Minimal stand-in for an email.message.Message with a plain body."""

    def __init__(self, body):
        self._body = body

    def is_multipart(self):
        return False

    def get_payload(self, decode=False):
        return self._body.encode("utf-8") if decode else self._body

    def get_content_charset(self):
        return "utf-8"


def main() -> int:
    failures = 0
    for text, expected in CASES:
        got = handler.classify_reply(text)
        if got != expected:
            failures += 1
            print(f"FAIL classify {text!r} -> {got} (expected {expected})")

    for raw, expected in QUOTE_CASES:
        extracted = handler._reply_text(_FakeMessage(raw))
        got = handler.classify_reply(extracted)
        if got != expected:
            failures += 1
            print(f"FAIL quoted -> extracted {extracted!r} -> {got} (expected {expected})")

    total = len(CASES) + len(QUOTE_CASES)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
