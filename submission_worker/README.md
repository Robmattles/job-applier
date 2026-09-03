# Submission worker (local)

ARCHITECTURE.md §6 phase 7. Runs on your machine, not AWS — a real
visible Chrome window needs no remote-desktop session, which is the
whole reason §3 chose local execution.

## Run it

```bash
cd submission_worker
./.venv/bin/python worker.py              # process one approved application
./.venv/bin/python worker.py --loop       # keep draining the queue
./.venv/bin/python worker.py --dry-run    # open and inspect, fill nothing
```

It reads the `job-applier` AWS profile. Nothing runs unless you've
already replied "ok" to an approval email — the worker only ever sees
applications the reply listener marked APPROVED and enqueued.

## What it does, and doesn't

Fills, from `applicant-profile.json`: first/last name, email, phone,
location, and the résumé + cover-letter file uploads.

**Never fills**, by design:

- Voluntary self-identification (gender, ethnicity, veteran, disability).
  `applicant-profile.json` marks these decline-by-default and "never
  auto-filled with a guess, never LLM-generated." Verified live against
  a real Greenhouse form — all four were correctly refused.
- Salary/compensation, visa/work-authorization phrasing, criminal
  history, security clearance, reason for leaving. Each varies enough by
  form wording that a templated answer is a bad idea.
- Custom screening questions. It lists them for you rather than
  answering — these are where a wrong answer is worst.
- CAPTCHAs. Greenhouse forms carry a live reCAPTCHA; the worker never
  touches it.
- **The submit button.** It fills the form and hands it to you. An
  application sent to a real employer can't be recalled, so the last
  click is yours.

After you submit (or don't), answer the prompt — `y` records SUBMITTED,
`n` records NOT_SUBMITTED, `s` leaves it queued for later.

## Forms it understands

| Source | Path |
|---|---|
| `greenhouse` | Canonical board URL, which redirects to the employer page and embeds the real form in an `/embed/job_app` iframe. The worker scans every frame — a top-document-only scan finds zero fields on a page with nineteen. |
| `ashby` | `jobs.ashbyhq.com/{company}/{id}` |
| `himalayas`, `remoteok`, `jobicy` | Aggregator listings with no resolvable form. Opens the listing for you to navigate to the employer's real application yourself, per §3 rather than guessing at a form structure. |

## Setup (already done)

```bash
python3.11 -m venv .venv
./.venv/bin/pip install playwright boto3
./.venv/bin/playwright install chromium
```

Contained entirely in `submission_worker/.venv` — `rm -rf` it to undo.
