# Job Applier — Architecture Plan

Goal: a fully automated pipeline that discovers senior DS / Applied ML / Applied AI
job postings, scores fit against a master résumé, generates a tailored résumé +
cover letter/short answers per posting (via AWS Bedrock), and emails Matt one
message per candidate application. The **only** human step is replying "ok" to
that email — that reply triggers the actual submission. Everything else runs
unattended on AWS.

Status: **design settled, no infrastructure built yet.** All decisions in
§3 are closed. What's left is the concrete build — see §7. Full history of
what was tried, tested, and rejected along the way (job-aggregator
vendors, HiringCafe, etc.) lives in git history, not in this document —
this file describes the current design only.

---

## 1. Pipeline overview

```
EventBridge (schedule)
   │
   ▼
[Ingest Lambdas] → Two source families, both free and official, fanning
   │                into the same Dedup step below (so adding/dropping a
   │                source is a config change, not a redesign):
   │                (1) PRIMARY — direct per-company polling of
   │                Greenhouse/Lever/Ashby's free public APIs. Highest
   │                density of any source tested. The company list is not
   │                hand-maintained: a scheduled search-discovery Lambda
   │                runs `site:` queries (per ATS platform × target job
   │                title) on a rotation, extracts new company board
   │                tokens from the result URLs, and writes them to a
   │                DynamoDB "known companies" table that this polling
   │                connector reads from — the list grows itself.
   │                `target-employer-list.md` seeds that table with 19
   │                already-verified companies.
   │                (2) SECONDARY — three remote-job-board APIs: Himalayas
   │                and Jobicy (both used as-is, no caveats), and RemoteOK
   │                (specific tags only — verify each tag empirically
   │                before relying on it; its "data-science" tag silently
   │                ignores the filter and returns the unfiltered firehose).
   │                Ashby also exposes a clean `isRemote` boolean, more
   │                reliable than the string-matching Greenhouse/Lever
   │                require.
   │                Total ingestion cost: $0/month. The $50/mo ceiling is
   │                unspent, held in reserve for a paid aggregator
   │                (candidates: theirstack.com, fantastic.jobs) if this
   │                combination's real coverage proves insufficient once
   │                it's actually running — not before.
   ▼
[Dedup + Filter] → DynamoDB "seen postings" table; title regex; **remote
   │                only — hard filter, not a scoring factor** (checked
   │                against the posting's own location/workplace-type
   │                field where the ATS exposes one; ambiguous postings
   │                fall through to fit-scoring to make the call from the
   │                JD text, not silently pass); posting age < 48h
   │                preferred, hard cutoff otherwise
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
   │                    competitor restriction — direct fraud-detection/
   │                    insurance-data competitors are in scope normally.
   ▼
[Threshold gate + weekly cap] → only postings above the fit-score bar
   │                             proceed; starts at ~100/week, scaling up
   │                             as long as the bar is genuinely being
   │                             cleared (not lowered to hit a number),
   │                             highest score first
   ▼
[Résumé/Letter Generation Lambda] → Bedrock (higher-quality model, e.g.
   │                                 Claude Sonnet) runs the evidence audit
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
   │                                     unresolved (usually a genuine
   │                                     evidence gap, not a rewrite
   │                                     problem) goes to Matt as a
   │                                     NEEDS_REVIEW note attached to
   │                                     the application, not a silent drop
   ▼
[Render] → deterministic renderer drops the QA'd structured content into
   │        the one fixed single-column template; PDF stored in S3
   ▼
[Approval Email Lambda] → sends ONE email per posting to Matt's Gmail:
   │                        company, role, fit rationale, link to the
   │                        generated résumé/cover letter, "reply ok to
   │                        submit"; writes a PENDING row to DynamoDB
   │                        with a TTL (job postings go stale — expire
   │                        pending approvals after ~5 days)
   ▼
[Reply Listener] → detects the "ok" reply via Gmail API (§3), flips
   │                DynamoDB status → APPROVED, enqueues to SQS
   ▼
[Submission Worker] → Lever's documented apply API where available;
   │                   otherwise headless-browser (Playwright, in a
   │                   Fargate task or Lambda container) fills the real
   │                   ATS form with the generated documents; captures a
   │                   confirmation screenshot; writes SUBMITTED
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
| Compute | Lambda for everything except browser automation; Fargate (or Lambda container image) for Playwright submission jobs |
| Queueing | SQS between scoring→generation and approval→submission, so nothing is lost on a Lambda failure/retry |
| Storage — structured | DynamoDB: `postings` (dedup), `known_companies` (ingestion, grown by search-discovery), `applications` (funnel state machine), `pending_approvals` (TTL) |
| Storage — documents | S3: master résumé/accomplishment inventory (source of truth), generated résumé/cover-letter PDFs per application, submission confirmation screenshots |
| LLM | Bedrock — tiered: a cheap/fast model for the first-pass fit score on every posting (high volume), a stronger model for the résumé rewrite + evidence audit on postings that clear the bar (low volume, quality matters), and a separately-framed model for the QA critique passes (§4) so the critic isn't just the drafter rubber-stamping itself |
| Secrets | Secrets Manager — Gmail/Google OAuth token, any ATS credentials |
| Email | SES (send) + Gmail API (read replies) — see §3 |
| Observability | CloudWatch alarms on Lambda errors / DLQ depth / weekly spend; a kill switch (SSM parameter or EventBridge rule disable) to pause the whole pipeline instantly |
| IaC | CDK (Python, to match your stack) — everything above defined as code, not clicked in the console, so it's reproducible and reviewable |

## 3. Decisions (settled)

**Reply detection: Gmail API.** Send and poll via OAuth against Matt's
actual Gmail account. Everything stays inside the existing inbox, no domain
needed. One-time setup: a Google Cloud project + OAuth consent + refresh
token stored in Secrets Manager.

**Submission scope: API-first + one-click fallback, to start.** True
zero-click auto-submit only on ATS with a documented apply endpoint
(confirmed for Lever; Greenhouse/Ashby need re-checking closer to
implementation). Everywhere else, the "ok" reply pre-fills the form and
opens a review-and-submit link rather than a true zero-click submission.
Playwright-based full automation for the remaining ATS is a phase-2
expansion once the rest of the pipeline is proven, not part of the initial
build.

**Ingestion sourcing: direct ATS polling (self-growing company list) +
three free remote-job-board APIs, $0/month.** See §1. Evaluated and
rejected: Remotive (free tier is a capped marketing sample; real feed is a
paid $5k/mo product), Arbeitnow (Germany/EU-focused, no remote DS/ML
overlap found), HiringCafe (no official API; site search is client-side
JS with no accessible query interface), and paid cross-ATS aggregators
(not needed — direct polling outperformed them in live testing). $50/mo
ceiling held in reserve, not spent.

**Comp floor: $130k** base or total comp. **No non-compete/competitor
restriction** — direct fraud-detection/insurance-data competitors are in
scope normally.

## 4. Generated-content QA (authenticity + grounding + specificity + recruiter passes)

Every string Bedrock generates — résumé bullets, summary, cover-letter/
short-answer text — goes through a second, separately-framed Bedrock call
before it's ever shown to Matt or an employer. Separately-framed matters:
having the same call that drafted the text also approve it tends to just
rubber-stamp its own style; a distinct critic pass (ideally a different
model — e.g. Haiku critiquing Sonnet's draft) catches more.

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

- **Weekly application cap**, highest fit-score first — starts at ~100/week
  (Matt's call), scaling up as long as the fit-score bar keeps being
  cleared by genuinely relevant postings rather than the bar dropping to
  fill a quota. Exists to keep a freak high-volume day from flooding the
  inbox, not to hold volume down deliberately.
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

## 6. Build phases

1. **Decisions + foundation** — CDK skeleton, IAM, Secrets Manager,
   S3/DynamoDB tables (§2).
2. **Ingestion** — connectors for direct Greenhouse/Lever/Ashby polling
   plus the search-discovery Lambda that grows the company list, and
   Himalayas/Jobicy/RemoteOK; dedup/filter logic.
3. **Scoring** — Bedrock evidence-audit prompt, fit threshold, weekly cap.
4. **Generation** — lane-specific structured-content rewrite against the
   accomplishment inventory.
5. **QA pass** — all four passes (§4), NEEDS_REVIEW path; fixed-template
   rendering to single-column/clean-parse-validated PDF.
6. **Approval loop** — send + reply detection, TTL on pending approvals.
7. **Submission** — API-first submitters, Playwright fallback, confirmation
   capture.
8. **Reporting** — weekly funnel digest email, CloudWatch dashboard.
9. **Guardrails hardening** — cap, budget alarm, kill switch, audit trail.

## 7. What's needed to start

Everything content- and design-related is done: master résumé +
accomplishment inventory (§ résumé files), ingestion sources (§1, $0/mo),
comp/competitor criteria (§3). What's left is infrastructure, and none of
it is something I can do without you:

- **Scoped AWS credentials** — a dedicated IAM user/role, not root (see
  least-privilege note in §5). I can hand you the exact policy JSON to
  create it with.
- **A Google Cloud project for Gmail API OAuth** — free, ~10 minutes,
  one-time setup for send + reply-detection.
- Once both exist: stand up the CDK foundation (§6, phase 1) that
  everything else attaches to.

One caveat worth repeating, not a blocker but a real one: the QA passes in
§4 have only been run by me manually simulating Bedrock, with Matt
catching every miss along the way — not by actual unsupervised Bedrock
calls yet. Worth proving that out, and probably starting well under the
100/week target while it does, before trusting this at volume.
