# Job Applier — Architecture Plan

Goal: a fully automated pipeline that discovers senior DS / Applied ML / Applied AI
job postings, scores fit against a master résumé, generates a tailored résumé +
cover letter/short answers per posting (via AWS Bedrock), and emails Matt one
message per candidate application. The **only** human step is replying "ok" to
that email — that reply triggers form-fill, and Matt personally completes
anything a script shouldn't (CAPTCHA, final submit) on his own machine.
Everything else runs unattended on AWS.

Status: **design settled, credentials in place, no infrastructure built
yet.** All decisions in §3 are closed, both setup blockers in §7 are
done. What's left is the concrete build, starting with the CDK
foundation (§6 phase 1). Full history of
what was tried, tested, and rejected along the way lives in git history,
not in this document — this file describes the current design only.
Revised 2026-09-02 after a real architecture review caught two errors in
the original design (submission mechanism, company-discovery mechanism) —
see §3 for what changed and why.

---

## 1. Pipeline overview

```
EventBridge (schedule)
   │
   ▼
[Ingest Lambdas] → Two source families, both free and official, fanning
   │                into the same Dedup step below:
   │                (1) PRIMARY — direct per-company polling of
   │                Greenhouse/Lever/Ashby's free public APIs. Highest
   │                density of any source tested. Company list grows from
   │                company names surfaced by every source — normalize
   │                the name to a likely board-token slug, probe the
   │                three ATS's free APIs directly (a 200 response
   │                confirms a real board), add hits to the
   │                `known_companies` table. `target-employer-list.md`
   │                seeds it with 19 already-verified companies.
   │                (2) SECONDARY — Himalayas, Jobicy (both used as-is),
   │                and RemoteOK (specific tags only — verify each tag
   │                empirically, its "data-science" tag silently ignores
   │                the filter). Ashby exposes a clean `isRemote` boolean,
   │                more reliable than Greenhouse/Lever's location string.
   │                Total ingestion cost: $0/month, $50/mo ceiling in
   │                reserve for a paid aggregator if this proves
   │                insufficient once running — not before.
   ▼
[Dedup + Filter] → DynamoDB "seen postings" table; title regex; **remote
   │                only — hard filter, not a scoring factor**, three
   │                layers so phrasing variety doesn't leak through
   │                (flagged by Matt 2026-09-02 — JDs describe remote
   │                arrangements in more ways than the literal word
   │                "remote"): (1) structured field where the ATS exposes
   │                one — Ashby's `isRemote` boolean is authoritative;
   │                (2) an expanded string-match against Greenhouse/
   │                Lever's free-text location field — "remote,"
   │                "distributed," "work from home," "WFH," "remote-
   │                first," "anywhere," "remote (US)" and similar,
   │                explicitly excluding "hybrid" and "remote flexible/
   │                remote days" patterns that aren't actually remote
   │                despite containing the word; (3) anything that field
   │                doesn't cleanly resolve falls through to fit-scoring,
   │                which reads the full JD text with an LLM rather than
   │                a fixed pattern — this is the actual correctness
   │                backstop, not the regex, since natural-language
   │                phrasing will always outrun a pattern list. Stores
   │                `source_published_at`,
   │                `source_updated_at`, `first_seen_at` — no hard
   │                freshness cutoff; freshness is a ranking input at the
   │                threshold-gate step, not a gate itself, since a
   │                strong 4-day-old posting should beat a mediocre
   │                4-hour-old one.
   ▼
[Fit-Scoring Lambda] → Bedrock (cheap model, e.g. Claude Haiku) does an
   │                    "evidence audit" against the accomplishment
   │                    inventory: fit score, reasons to interview/reject,
   │                    lane pick (Senior DS / Applied MLE / Applied AI);
   │                    resolves ambiguous remote/hybrid postings from JD
   │                    text where the structured filter couldn't. Comp
   │                    floor: **$130k base or total comp** — reject below
   │                    floor where a posting states a range; where none
   │                    is stated, comp is one factor in the rationale,
   │                    not a silent auto-reject. No non-compete/
   │                    competitor restriction.
   ▼
[Threshold gate + weekly cap] → ranks by fit score blended with freshness;
   │                             cap ramps per the schedule in §5, not a
   │                             flat 100/week from day one
   ▼
[Résumé/Letter Generation Lambda] → Bedrock (higher-quality model, e.g.
   │                                 Claude Opus) runs the evidence audit
   │                                 against the accomplishment inventory
   │                                 for the chosen lane and outputs
   │                                 structured content (headline, summary,
   │                                 ordered bullets) — not a laid-out
   │                                 document
   ▼
[Authenticity + Grounding + Specificity QA Lambda] → a separately-framed
   │                                    adversarial pass (see §4) on every
   │                                    generated string before it reaches
   │                                    Matt or an employer: rewrites
   │                                    against an AI-writing-tell
   │                                    checklist, verifies every factual
   │                                    claim traces to a specific record
   │                                    in the accomplishment inventory,
   │                                    and rejects true-but-abstracted
   │                                    bullets that never name the actual
   │                                    system/technique/domain
   ▼
[Recruiter/ATS Adversarial QA Lambda] → a fourth pass (see §4), on the
   │                                     whole assembled document against
   │                                     the actual JD: roleplays a
   │                                     skeptical recruiter/ATS screen —
   │                                     JD-requirement coverage gaps,
   │                                     narrative/seniority-signal
   │                                     clarity, reasons to reject. A
   │                                     gap fixable by re-selecting
   │                                     evidence loops back to
   │                                     Generation once; anything left
   │                                     unresolved goes to Matt as a
   │                                     NEEDS_REVIEW note, not a silent
   │                                     drop
   ▼
[Render] → deterministic renderer drops the QA'd structured content into
   │        the one fixed, visually appealing and machine-readable
   │        single-column template; PDF stored in S3
   ▼
[Approval Email Lambda] → sends ONE email per posting to Matt's Gmail:
   │                        company, role, fit rationale, link to the
   │                        generated résumé/cover letter, "reply ok to
   │                        submit"; writes a PENDING row to DynamoDB
   │                        with a TTL (job postings go stale — expire
   │                        pending approvals after ~5 days)
   ▼
[Reply Listener] → detects the "ok" reply via Gmail IMAP (§3), flips
   │                DynamoDB status → APPROVED, enqueues to SQS
   ▼
[Submission Worker] → runs LOCALLY on Matt's machine (Matt's call,
   │                   "whichever's easier" — local avoids remote-desktop
   │                   friction entirely, since it's a real visible
   │                   Chrome window on his own screen). Fills
   │                   SAFE_AUTOFILL fields from `applicant-profile.json`,
   │                   drafts GENERATE_GROUNDED answers for open-ended
   │                   questions (grounding-checked against the
   │                   accomplishment inventory same as résumé bullets),
   │                   and pauses for Matt to personally complete anything
   │                   NEEDS_REVIEW — a CAPTCHA, an unfamiliar legal
   │                   attestation, an ambiguous question — never attempts
   │                   to defeat a CAPTCHA or invent an answer. This is the
   │                   submission mechanism from day one, not a phase-2
   │                   fallback — see §3 for why the originally-planned
   │                   "API-first" approach doesn't actually work.
   │                   Postings sourced only from Himalayas with no
   │                   resolvable direct ATS board go straight to
   │                   NEEDS_REVIEW rather than building a fourth apply
   │                   pathway for MVP (see §3).
   ▼
[Funnel tracker] → DynamoDB row per application: source, posting age,
                    lane, résumé version, ATS, dates, and later manually
                    or semi-automatically updated stage (screen /
                    interview / offer / reject) for the "instrument the
                    search like an experiment" review every 4-6 weeks;
                    weekly digest email of funnel stats
```

## 2. AWS services by role

| Concern | Service |
|---|---|
| Scheduling | EventBridge Scheduler |
| Compute | Lambda for ingestion/scoring/generation/QA/email; the Submission Worker runs locally on Matt's machine (Playwright against a real browser), not in AWS — see §3 |
| Queueing | SQS between scoring→generation and approval→submission (the local worker polls this queue using the same scoped credential from `setup-runbook.md`), so nothing is lost on a Lambda failure/retry |
| Storage — structured | DynamoDB: `postings` (dedup), `known_companies` (ingestion, grown by name-probing), `applications` (funnel state machine), `pending_approvals` (TTL) |
| Storage — documents | S3: master résumé/accomplishment inventory (source of truth), generated résumé/cover-letter PDFs per application, submission confirmation screenshots |
| LLM | Bedrock — tiered: a cheap/fast model for the first-pass fit score on every posting (high volume), a stronger model for the résumé rewrite + evidence audit + grounded application-answer generation on postings that clear the bar (low volume, quality matters), and a separately-framed model for the QA critique passes (§4) so the critic isn't just the drafter rubber-stamping itself |
| Secrets | Secrets Manager — Gmail/Google OAuth token, any ATS credentials |
| Email | SES (send) + Gmail IMAP with an App Password (read replies) — see §3 |
| Observability | CloudWatch alarms on Lambda errors / DLQ depth / weekly spend; a kill switch (SSM parameter or EventBridge rule disable) to pause the whole pipeline instantly |
| IaC | CDK (Python, to match your stack) — everything above defined as code, not clicked in the console, so it's reproducible and reviewable |

## 3. Decisions (settled)

**Reply detection: Gmail IMAP + App Password — corrected 2026-09-02.**
The original plan (Gmail API via OAuth) hit a real wall: `gmail.send`/
`gmail.readonly` are both Google "sensitive" scopes, and moving an
External app to production with sensitive scopes requires a security
assessment (2-4 weeks) plus domain-ownership verification via Search
Console — wildly disproportionate for a single-user personal tool, not
just a permissions checkbox. Confirmed live: Google's Publish App flow
actually asked for a domain.

Replacement needs no OAuth at all: Gmail App Passwords still work for
regular (non-Workspace) accounts in 2026 — a 16-character credential
generated once at myaccount.google.com/apppasswords (requires 2-Step
Verification already enabled), used with plain IMAP to poll for the "ok"
reply. No consent screen, no scope review, no domain, no 7-day expiry, no
Google Cloud project at all. Sending stays on SES (unchanged) — this only
replaces how replies get detected. Trade-off worth naming: an app
password is a static credential until revoked, not a short-lived OAuth
token — store it in Secrets Manager the same way, and revoking/rotating
it just means generating a new one at the same Google Account page.

**Submission mechanism: Playwright, local execution, from day one —
corrected 2026-09-02.** The original design planned "API-first" auto-
submission via each ATS's application-submission endpoint. That doesn't
work for arbitrary employers: Greenhouse's, Lever's, and Ashby's
application-POST endpoints all require the *employer's own* API key
(Lever's docs are explicit that it must be generated by a Super Admin of
that employer's account) — not something an outside applicant can obtain.
The public GET endpoints used for discovery are anonymous; submission
never is. So there is no real "API-first" tier to fall back from —
browser automation against the employer's actual hosted application form
is the only real submission path, for every employer, starting now, not
as a phase-2 expansion.

Runs locally rather than in AWS Fargate, specifically to solve the
CAPTCHA/final-review handoff: a real, visible browser on Matt's own
screen needs no remote-desktop session at all, versus a cloud-hosted
headless browser which would. Matt is fine completing CAPTCHAs and final
submit clicks personally, as long as it's not RDP-into-a-Windows-box
levels of friction — local execution satisfies that directly. AWS-hosted
execution remains possible later if preferred; not the default because it
solves a problem that doesn't otherwise exist.

**Company discovery: name-probing, not a search API — corrected
2026-09-02.** The original design planned a scheduled Lambda running
`site:` queries against a search API to find new company board tokens.
Confirmed 2026-09-02: Google's Custom Search JSON API — the practical way
to run programmatic `site:` searches — is closed to new signups and shuts
down entirely 2027-01-01. Dead end regardless of when this gets built.
Replacement: every ingested posting, from any source, carries a company
name (Himalayas' `companySlug`, Jobicy's `companyName`, etc.) even when it
doesn't carry a usable original-ATS URL — normalize the name to a likely
board-token slug and probe Greenhouse/Lever/Ashby's free APIs directly. A
200 response confirms a real board at no cost beyond a wasted API call on
a miss. Needs no search API, no per-query cost, no Jan-2027 expiration.

**Caveat found while testing this, not fully resolved:** aggregator
listings don't reliably expose a parseable path to the origin ATS either.
Checked both live 2026-09-02: Jobicy's job pages do link out to "the
employer website" (present, not cleanly extractable via a simple fetch);
Himalayas does not — every Himalayas posting funnels through Himalayas'
own signup/apply flow, with no origin-ATS link exposed at all. Name-
probing still works for a Himalayas-sourced posting if that company
*also* happens to have a direct Greenhouse/Lever/Ashby board — many do —
but for one that doesn't, there's currently no automated submission path,
only Himalayas' own (unbuilt) apply flow. MVP choice: route those to
NEEDS_REVIEW rather than building a fourth submission pathway now. If a
real search API is needed later, the $50/mo reserve covers a Bing/Brave/
SerpApi-style option — not Google's, which is the one confirmed dead.

**Ingestion sourcing: direct ATS polling (self-growing company list) +
three free remote-job-board APIs, $0/month.** See §1. Evaluated and
rejected: Remotive (free tier is a capped marketing sample; real feed is a
paid $5k/mo product), Arbeitnow (Germany/EU-focused, no remote DS/ML
overlap found), HiringCafe (no official API; site search is client-side
JS with no accessible query interface), and paid cross-ATS aggregators
(not needed — direct polling outperformed them in live testing).

**Comp floor: $130k** base or total comp. **No non-compete/competitor
restriction** — direct fraud-detection/insurance-data competitors are in
scope normally.

**Application-question handling: `applicant-profile.json` + a three-way
classification — new 2026-09-02.** Every question on an application form
gets classified before the Submission Worker touches it:
- **SAFE_AUTOFILL** — an exact, known answer from `applicant-profile.json`
  (name, contact, work authorization, years of experience, GitHub/site,
  etc.). Filled automatically, no LLM involved.
- **GENERATE_GROUNDED** — open-ended questions ("Why are you interested
  in this role?") answered by Bedrock, grounding-checked against the
  accomplishment inventory the same way résumé bullets are (§4 Pass 2) —
  never invented from nothing.
- **NEEDS_REVIEW** — anything ambiguous, legally meaningful, or
  unsupported by the profile or inventory: CAPTCHAs, unfamiliar legal
  attestations, restrictive-covenant questions, salary negotiation
  beyond what the profile states, demographic/veteran/disability
  self-identification (these default to "decline to answer" unless Matt
  explicitly fills in real answers in the profile — never LLM-generated).
  Matt completes these personally in the local browser session, same
  motion as a CAPTCHA.

See `applicant-profile.json` for the template — needs Matt's real answers
before the Submission Worker can run.

## 4. Generated-content QA (authenticity + grounding + specificity + recruiter passes)

Every string Bedrock generates — résumé bullets, summary, cover-letter/
short-answer text, and now grounded application-question answers — goes
through a second, separately-framed Bedrock call before it's ever shown
to Matt or an employer. Separately-framed matters: having the same call
that drafted the text also approve it tends to just rubber-stamp its own
style; a distinct critic pass (ideally a different model — e.g. Haiku
critiquing Opus's draft) catches more.

**Pass 1 — authenticity.** Rewrites against a concrete checklist of
LLM writing tells, rather than a vague "make this sound human" instruction
(models do much better with specifics than with a mood):
- Rhetorical em-dash overuse
- Rule-of-three / triadic listing ("fast, reliable, and scalable")
- Stock transitions and hedges ("It's worth noting," "Moreover,"
  "Furthermore," "In today's fast-paced environment")
- Buzzword-as-filler ("leverage," "robust," "seamless," "cutting-edge,"
  "results-driven," "passionate about") unless backed by a specific number
- Uniform sentence rhythm — every bullet the same [verb][object][result]
  shape with no variation
- Excessive hedging ("can potentially help to")
- Perfectly balanced "not only X but also Y" constructions
- Generic opening lines ("As a highly motivated professional…")
- Mechanical keyword-stuffing that doesn't read naturally
- Title-Case Headers Everywhere as filler structure

**Pass 2 — grounding.** Every factual/quantified claim in the generated
text must trace to a specific `accomplishment-inventory.json` record id
(the inventory's `source` field chains back to `nicb-resume-info.md` or
`career-history.md` for deeper traceability). Catches drift (an inflated
number, a tool that was never actually used, a claim the JD's language
nudged the model toward) — including drift induced by adversarial content
embedded in a job posting itself, since the JD text is untrusted input to
these prompts. Anything that doesn't trace cleanly gets dropped, not
guessed into plausibility.

**Pass 3 — specificity.** Catches true, on-topic bullets that say nothing
— abstracted nouns ("a validation threshold," "a deprecated managed
explainability service") standing in for the actual system/technique/
domain. Different failure than authenticity (doesn't sound like AI) or
grounding (isn't false) — clean and true and empty. Check: does the
bullet name the real system/technique, or a generic stand-in ("a system,"
"a service," "a tool")? Would someone with zero context know what it's
about? Root cause can be upstream in the inventory itself, not just the
rewrite — a record's `text` field can be vague even with the concrete
detail sitting unused in its own `metrics` field.

**Pass 4 — recruiter/ATS adversarial review.** Different in kind from
passes 1-3: those operate per-bullet and don't need the target JD; this
one operates on the whole assembled document *against the actual
posting*, roleplaying a skeptical recruiter or ATS keyword screen. The
"act as a skeptical hiring manager" evidence-audit idea from the original
job-search strategy, formalized as a pipeline stage.

**Hard rule: every finding must trace to a specific line in the posting.**
Not a generic resume-best-practices audit — JD-relevance filtering comes
first, and required lines get checked individually rather than bundled
with adjacent nice-to-haves. (A looseness here once let a real gap slide:
"mentoring" and "stakeholder communication" got bundled as one finding
and only the nice-to-have half got fixed, while the actually-required
line stayed uncovered. Precision in the JD-mapping matters as much as
recall — sloppy mapping produces both false-positive findings and false
negatives hiding behind a bundled one.)

It produces:
- The 5 strongest reasons to interview, grounded in what's actually on the
  page (sanity-checks that the strongest evidence actually made the cut)
- The 3 most likely reasons to reject — each naming the specific JD line
  it fails, required lines checked separately from nice-to-haves
- Every required (not nice-to-have) JD line cross-checked individually,
  even ones that feel adjacent to something already covered
- Basic ATS-parseability sanity checks (consistent date formats, no
  tables/columns/graphics, standard section headers) — largely guaranteed
  by the fixed template rather than something this pass needs to catch

On a finding fixable by re-selecting existing inventory evidence, loops
back to Generation once. On a genuine evidence gap — not fixable by
rewriting — it's a NEEDS_REVIEW note for Matt, never papered over.

**Guardrails on the QA passes:** capped at 2 revision loops (cost/latency
control); anything still unresolved holds the application in
NEEDS_REVIEW for Matt rather than silently shipping or discarding it.

## 5. Guardrails (building these in regardless of the above)

- **Weekly application cap, ramped rather than flat from day one:**
  week 1: 5-10, inspect everything manually. Week 2: 15-25, inspect
  recruiter score, résumé, and form answers. Weeks 3-4: 30-50, only if
  the false-positive rate and QA-failure rate are actually low. Beyond
  that: climb toward 100/week only as far as real data says there are
  that many genuinely good matches — not because 100 is itself a
  problem, but because the fit classifier needs calibrating against real
  outcomes before it's trusted at firehose volume. Always highest
  fit-score (blended with freshness) first.
- **Cost ceiling**: CloudWatch billing alarm at a threshold you set; Bedrock
  calls tiered cheap-model-first so scoring 100s of postings/day doesn't
  burn budget on the expensive model.
- **Kill switch**: one flag that halts ingestion and submission instantly.
- **Audit trail**: every auto-submitted application keeps its generated
  documents, the fit rationale, and a submission timestamp/confirmation —
  so nothing is submitted from a black box.
- **Data hygiene**: master résumé and generated documents contain PII;
  S3 buckets are private + encrypted, IAM scoped per-Lambda, no public
  endpoints.
- **Least-privilege AWS creds for me**: a dedicated IAM user/role scoped
  to exactly the services in §2 (Lambda, DynamoDB, S3, SQS, EventBridge,
  Bedrock, SES, Secrets Manager, IAM-limited, CloudWatch), not root/admin
  keys.
- **Never defeat a CAPTCHA, never invent an answer to an ambiguous or
  legally-meaningful question** — NEEDS_REVIEW exists precisely so the
  pipeline doesn't have to.

## 6. Build phases

1. **Decisions + foundation** — CDK skeleton, IAM, Secrets Manager,
   S3/DynamoDB tables (§2).
2. **Ingestion** — connectors for direct Greenhouse/Lever/Ashby polling,
   the name-probing discovery logic that grows `known_companies`, and
   Himalayas/Jobicy/RemoteOK; dedup/filter logic; freshness stored, not
   gated.
3. **Scoring** — Bedrock evidence-audit prompt, fit threshold, ramped
   weekly cap (§5).
4. **Generation** — lane-specific structured-content rewrite against the
   accomplishment inventory.
5. **QA pass** — all four passes (§4), NEEDS_REVIEW path; fixed-template
   rendering to single-column/clean-parse-validated PDF.
6. **Approval loop** — send + reply detection, TTL on pending approvals.
7. **Submission** — local Playwright worker, `applicant-profile.json`
   autofill + grounded answer generation + NEEDS_REVIEW handoff (§3);
   confirmation capture.
8. **Reporting** — weekly funnel digest email, CloudWatch dashboard.
9. **Guardrails hardening** — ramp schedule, budget alarm, kill switch,
   audit trail.

## 7. What's needed to start

Everything content- and design-related is done: master résumé +
accomplishment inventory (§ résumé files), ingestion sources (§1, $0/mo),
comp/competitor criteria (§3), `applicant-profile.json` filled in.

- ~~**Scoped AWS credentials**~~ — **done 2026-09-02.** `job-applier-agent`
  IAM user, two least-privilege policies (`iam-policy-job-applier-core.json`,
  `iam-policy-job-applier-ops.json` — split from one because the original
  exceeded AWS's 6144-char managed-policy limit), local `job-applier`
  CLI profile configured and verified, temporary root bootstrap key
  deleted. Details: `setup-runbook.md` §1. This same profile is what the
  local Submission Worker uses too, not just deployment.
- ~~A Google Cloud project for Gmail API OAuth~~ — **no longer needed,
  corrected 2026-09-02** (see §3 — switched to IMAP + App Password).
- ~~**A Gmail App Password**~~ — **done 2026-09-02.** Generated, stored
  in Secrets Manager as `job-applier-gmail-app-password`, verified via
  `describe-secret`. Details: `setup-runbook.md` §2.

**Both blockers are now clear — nothing left needed from Matt to start
building.** Next: stand up the CDK foundation (§6, phase 1).

One caveat worth repeating, not a blocker but a real one: the QA passes in
§4 have only been run by me manually simulating Bedrock, with Matt
catching every miss along the way — not by actual unsupervised Bedrock
calls yet. The ramp schedule in §5 covers both this and the volume
estimate below — good reasons independently to prove the pipeline out
slowly rather than trusting it at volume from day one.

**Estimated raw volume, unverified (2026-09-02):** stock-to-flow estimate
from the 19-company live test in §1/§3 (189 open remote DS/ML/AI
postings ÷ an assumed 25-40 day posting dwell time) gives roughly 5-8 new
relevant remote postings/day from just those 19 companies. Once the
company list matures via name-probing discovery, a genuine order-of-
magnitude guess is 15-50/day system-wide — wide range because company-
list growth rate and per-company density for the long-tail companies
discovery actually finds are both unknown. Replace this estimate with
real measured data once ingestion runs for an actual week.
