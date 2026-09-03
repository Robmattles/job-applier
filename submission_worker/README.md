# Submission worker (local)

ARCHITECTURE.md §6 phase 7. Runs on your machine, not AWS — a real
visible Chrome window needs no remote-desktop session, which is the whole
reason §3 chose local execution.

## The short version

Install the watcher once. After that, replying "ok" to an approval email
is the entire job:

```bash
./install-watcher.sh
```

Reply "ok" → the reply listener picks it up within 10 minutes → the
watcher notices within a minute → a Terminal window and a Chrome window
open by themselves, form already filled. You handle the CAPTCHA and
anything it deliberately left blank, and it's done.

```bash
./install-watcher.sh status      # is it watching
./install-watcher.sh uninstall   # stop watching
tail -f ~/Library/Logs/job-applier-watcher.log
```

The watcher polls DynamoDB, never SQS. Every `receive_message` on the
submission queue counts as a delivery attempt against `maxReceiveCount=3`,
so a watcher polling the queue would dead-letter every real approval
within three minutes.

## Running it by hand

```bash
./.venv/bin/python worker.py              # process one approved application
./.venv/bin/python worker.py --loop       # keep draining the queue
./.venv/bin/python worker.py --review     # fill it, but you click submit
./.venv/bin/python worker.py --dry-run    # open and inspect, fill nothing
```

It reads the `job-applier` AWS profile. Nothing runs unless you've already
replied "ok" — the worker only ever sees applications the reply listener
marked APPROVED and enqueued.

## Two modes, and which one you're in

**`--review`** fills the form and stops. You check every field and click
submit yourself.

**Default (auto)** fills the form and clicks submit, unless one of three
things stops it (`_submit_blockers`): a CAPTCHA is present, a required
field is still empty, or the résumé didn't attach. Any of those hands the
browser back to you with the reason printed.

Auto is the §3 steady state — the approval email is the human gate, and
stopping again at the browser gates one decision twice. The watcher ships
in `review` mode until a first real submission has actually gone through;
flip `JOB_APPLIER_WORKER_MODE` to `auto` in the plist and re-run
`install-watcher.sh`.

## What it fills, and what it refuses

Fills, from `applicant-profile.json`: first/last name, email, phone,
location, and the résumé + cover-letter uploads.

**Never fills**, by design:

- Voluntary self-identification (gender, ethnicity, veteran, disability).
  `applicant-profile.json` marks these decline-by-default and "never
  auto-filled with a guess, never LLM-generated." Verified live against a
  real Greenhouse form — all four correctly refused.
- Salary/compensation, visa/work-authorization phrasing, criminal
  history, security clearance, reason for leaving. Each varies enough by
  form wording that a templated answer is a bad idea.
- CAPTCHAs. §5 is absolute on this.

Custom screening questions get drafted (grounded against the
accomplishment inventory, same as résumé bullets) and offered to you one
at a time — accept, edit, or skip. Nothing goes in without you seeing it.

## Kill switch

`../cdk/scripts/kill_switch.sh on` stops the worker along with everything
else. It's re-read per application, so flipping it mid-`--loop` stops the
next one rather than only the next process.

## Forms it understands

| Source | Path |
|---|---|
| `greenhouse` | Canonical board URL, which redirects to the employer page and embeds the real form in an `/embed/job_app` iframe. The worker scans every frame — a top-document-only scan finds zero fields on a page with nineteen. |
| `ashby` | `jobs.ashbyhq.com/{company}/{id}/application` — the job page itself has no form. Custom fields are named with bare UUIDs, so matching is on the rendered label. Its "Autofill from resume" input is skipped deliberately: uploading there fires Ashby's parser, which overwrites everything already filled. |
| `himalayas`, `remoteok`, `jobicy` | Aggregator listings with no resolvable form. Opens the listing for you to navigate to the employer's real application yourself, per §3 rather than guessing at a form structure. |

## What happens when you skip

Answering `s` doesn't leave the message on the queue — `maxReceiveCount`
is 3, so a third skip would dead-letter a real approval, and nothing
re-enqueues from the DLQ. Instead the message is deleted, the row stays
APPROVED, and `deferred_until` holds it for an hour. `_recover_orphans`
puts it back on the queue after that — and also catches any approval
whose message was lost some other way, which is the failure this pipeline
has hit at every other stage.

## Setup (already done)

```bash
python3.11 -m venv .venv
./.venv/bin/pip install playwright boto3
./.venv/bin/playwright install chromium
```

Contained entirely in `submission_worker/.venv` — `rm -rf` it to undo.
