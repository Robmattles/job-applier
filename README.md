# job-applier

An autonomous job-application pipeline. It finds relevant postings, writes a tailored résumé and
cover letter for each one, checks its own output for fabrication, emails me a single approval
request, and — when I reply "ok" — opens a browser with the application form already filled in.

The only human step is the word "ok".

```
EventBridge ─▶ Ingest ─▶ Score ─▶ Gate ─▶ Generate ─▶ QA ─▶ Render ─▶ Email
   (4h)        2 Lambdas   Sonnet   cap/rank    Opus     2 passes   PDF     SES
                                                                             │
                        Browser ◀── Worker ◀── SQS ◀── Reply listener ◀──────┘
                        (local)                          "ok" via IMAP
```

Ten Lambdas across seven CDK stacks, plus a local Playwright worker. Everything except the
browser runs unattended on AWS.

## Why it's interesting

Most "AI applies to jobs for you" tools are a mail merge with a language model bolted on. The
hard part isn't generating text — it's generating text that is **true**, and knowing when it
isn't. Roughly half the engineering here is adversarial checking of the pipeline's own output.

Some of what that took:

**A résumé generator that argues with itself.** Generated content goes through two
separately-framed critic passes before a human sees it: one for authenticity, grounding, and
specificity, another that roleplays a skeptical recruiter screening against the actual posting.
Every factual claim must trace to a specific evidence record. Claims that don't trace get
dropped, not softened.

**Anti-fabrication, learned the hard way.** Early versions invented skills wholesale — 10-20
per application, plausible-sounding and entirely unattested. The skills section is now computed
deterministically from real evidence tags and re-asserted after every revision loop, because a
prompt instruction alone did not hold. That pattern repeats throughout: where a model can
fabricate, something deterministic checks it.

**Explicit capability boundaries.** The absence of evidence turns out not to be a signal a
generator reads. Asked for something the inventory doesn't cover, it reaches for adjacent work
and relabels it — graduate coursework became a "Causal Inference" headline; a database
workload-management change became "end-to-end experimentation infrastructure." The fix was to
state the boundaries *positively* in the data, so the model can see what isn't there.

**Guardrails that assume the model is wrong.** A CAPTCHA is never attempted. Demographic
self-identification is never auto-filled. Compensation and work-authorization questions are
left blank rather than guessed. The submit button is only clicked when a CAPTCHA is absent,
required fields are non-empty, and the résumé actually attached — verified by reading back the
DOM, not by assuming the click worked.

**A kill switch that actually stops things.** One flag in S3, read fresh on every invocation
(deliberately uncached — the one moment it matters is the moment you flip it), halting
ingestion, scoring, email, replies, and the local worker within seconds and with no deploy.

## Engineering notes

A few problems that were more interesting than expected:

**Detecting a job application form is not a URL problem.** Employers proxy Greenhouse through
their own domains (`careers.company.com/job?gh_jid=...`), run regional variants
(`job-boards.eu.greenhouse.io`), and hide the form behind their own tab UI where all 30 fields
exist in the DOM with `is_visible() == False` until something is clicked. Three separate bugs,
one lesson: check what's actually rendered, not what the URL says.

**A hung process is worse than a crashed one.** The local watcher polls Gmail over IMAP. A
laptop sleeping mid-request killed the TCP connection without raising, and `imaplib` blocked on
the socket read for eight hours — process alive, `launchctl` reporting it healthy, log frozen
mid-line. `KeepAlive` can't restart something that never exits. Fixed with a socket timeout and
a watchdog thread that force-exits on a stalled tick.

**Ranking has to fight recency bias in the right direction.** Postings are ranked by fit score
blended with freshness (`fit − 6 × age_days`). Without the decay a one-time backlog dominates
the queue for days; with it, a genuinely new posting outranks the pile within about 48 hours.

**Cost is a design constraint, not an afterthought.** Scoring runs on a cheaper model and
generation on a stronger one. The evidence inventory is ~78% of every scoring prompt and
identical across calls, so it's sent as a cached prefix — verified by reading
`cache_read_input_tokens` back, because a silent cache invalidation looks exactly like success
while costing full price.

## What it actually did

Over one week of real operation:

| | |
|---|---|
| Postings ingested and scored | 1,083 |
| Applications generated, QA'd, rendered | 97 |
| Approval emails sent | 47 |
| Applications actually submitted | 4 |
| Steady-state cost | ~$4/day |

The gap between 97 generated and 4 submitted is the point, not a failure: a weekly cap bounds
how much review lands on a human, and most postings correctly never make it past QA.

## Architecture

| Stage | Runs on | What it does |
|---|---|---|
| Ingestion | 2 Lambdas, every 4h | Polls Greenhouse/Lever/Ashby APIs directly, plus aggregators. Grows its own company list by normalizing employer names into board slugs and probing for a live board. |
| Scoring | Lambda + Sonnet | Evidence audit against the inventory. Hard filters for remote-only and a compensation floor. |
| Gate | Same Lambda | Ranks by fit blended with freshness, enforces a weekly cap, and verifies the posting is actually applyable before spending anything on it. |
| Generation | Lambda + Opus, SQS-triggered | Structured content — headline, summary, sourced bullets, cover letter. Never a laid-out document. |
| QA | Lambda + Opus, stream-triggered | Two adversarial passes, capped at 2 revision loops. Unresolved gaps park the application rather than shipping or dropping it. |
| Render | Lambda | Deterministic PDF from a fixed template. |
| Approval | Lambda + SES | One email per posting, PDFs attached, with the reasons *against* included. |
| Reply listener | Lambda + IMAP | Approves only on a short, unambiguous affirmative. |
| Submission | Local Playwright | Fills the form, drafts grounded answers to open questions, hands over for anything sensitive. |

## Running it

```bash
cp .env.example .env.local          # AWS account, region, notification email
cp examples/accomplishment-inventory.example.json accomplishment-inventory.json
cp examples/applicant-profile.example.json applicant-profile.json

cd cdk && cdk deploy --all          # 7 stacks
cd submission_worker && ./install-watcher.sh

../cdk/scripts/pause.sh on          # stop everything, near-zero cost
../cdk/scripts/pause.sh off         # start again
```

The inventory and profile are yours to write — they're the evidence base everything else reasons
over, and they're deliberately not in this repo. The examples show the shape.

## A note on the code comments

Comments here explain *why*, and often cite the specific failure that motivated a rule —
including dates and the exact wrong output. That's deliberate. Most of these behaviors are
non-obvious and were only discovered by running the thing against real job postings; a future
reader (including me) will otherwise "simplify" a guardrail straight back into the bug it exists
to prevent.

## Built with

Python · AWS CDK · Lambda · DynamoDB · SQS · S3 · SES · EventBridge · Bedrock (Claude) ·
Playwright
