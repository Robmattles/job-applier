# Job Applier — Architecture Plan

Goal: a fully automated pipeline that discovers senior DS / Applied ML / Applied AI
job postings, scores fit against a master résumé, generates a tailored résumé +
cover letter/short answers per posting (via AWS Bedrock), and emails Matt one
message per candidate application. The **only** human step is replying "ok" to
that email — that reply triggers the actual submission. Everything else runs
unattended on AWS.

Status: **planning — no infrastructure built yet.** This doc is the reference
we'll build against once the two open decisions below are settled and Matt
has supplied (a) the master résumé/accomplishment inventory and (b) scoped
AWS credentials.

---

## 1. Pipeline overview

```
EventBridge (schedule)
   │
   ▼
[Ingest Lambdas]  → Greenhouse / Lever / Ashby job-board APIs for a curated
   │                 employer list (~100-200 cos); optional HiringCafe feed
   │                 (needs verification — no confirmed public API yet)
   ▼
[Dedup + Filter]  → DynamoDB "seen postings" table; title regex; **remote
   │                 only — hard filter, not a scoring factor** (checked
   │                 against the posting's own location/workplace-type
   │                 field where the ATS exposes one; ambiguous postings
   │                 fall through to the fit-scoring step to make the call
   │                 from the JD text, not silently pass); posting age
   │                 < 48h preferred, hard cutoff otherwise
   ▼
[Fit-Scoring Lambda] → Bedrock (cheap model, e.g. Claude Haiku) does an
   │                    "evidence audit" against the master résumé: fit
   │                    score, reasons to interview/reject, lane pick
   │                    (Senior DS / Applied MLE / Applied AI); also makes
   │                    the final remote/no call from the JD text on
   │                    postings the structured-field filter couldn't
   │                    resolve (e.g. "remote" in the title but the body
   │                    says hybrid-3-days) — a reject here counts as the
   │                    remote filter catching it, not a fit-score miss
   ▼
[Threshold gate + weekly cap] → only postings above the fit-score bar
   │                             proceed; starts at ~100/week (scale up from
   │                             there as long as the bar is genuinely
   │                             being cleared, not lowered to hit a
   │                             number), highest score first, so a busy
   │                             day doesn't flood the inbox
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
[Reply Listener] → detects the "ok" reply (mechanism = open decision #1
   │                below), flips DynamoDB status → APPROVED, enqueues
   │                to SQS
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
| Storage — structured | DynamoDB: `postings` (dedup), `applications` (funnel state machine), `pending_approvals` (TTL) |
| Storage — documents | S3: master résumé/accomplishment inventory (source of truth), generated résumé/cover-letter PDFs per application, submission confirmation screenshots |
| LLM | Bedrock — tiered: a cheap/fast model for the first-pass fit score on every posting (high volume), a stronger model for the résumé rewrite + evidence audit on postings that clear the bar (low volume, quality matters), and a separately-framed model for the authenticity/grounding QA critique pass (§4) so the critic isn't just the drafter rubber-stamping itself |
| Secrets | Secrets Manager — Gmail/Google OAuth token if that's the chosen mechanism, any ATS credentials |
| Email | SES (send) + either SES inbound (receive) or Gmail API (read replies) — see open decision #1 |
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

**Pass 3 — specificity.** Found empirically, not designed in advance: the
first hand-assembled sample résumé (`sample-resume-senior-ds.md`) produced
two bullets that were true, on-topic, and completely unreadable — "a
validation threshold," "a deprecated managed explainability service" —
because concrete nouns (entity resolution, SHAP/TreeSHAP, which system,
which domain) got abstracted away while tightening the prose. This is a
different failure than authenticity (doesn't sound like AI) or grounding
(isn't a false claim) — it's true and clean and says nothing. Checks: does
every bullet name the actual system/technique/domain rather than a generic
stand-in ("a system," "an implementation," "a service," "a tool")? Would
someone with zero context on this person's work know what the bullet is
about? The root cause turned out to be upstream too — several
`accomplishment-inventory.json` records had this same vagueness baked into
their `text` field even with the concrete detail sitting unused in
`metrics` (e.g. `nicb-er-calibration` never said "entity resolution" while
its own `metrics` field had "doctor NPI cohesion" right there) — fixed
2026-09-01, but worth this pass catching it again if it recurs, since nothing
stops a future edit from reintroducing it.

**Pass 4 — recruiter/ATS adversarial review.** Different in kind from
passes 1-3: those operate per-bullet and don't need the target JD; this one
operates on the whole assembled document *against the actual posting*,
roleplaying a skeptical recruiter or ATS keyword screen rather than an
editor. This is the "act as a skeptical hiring manager" evidence-audit
idea from the original job-search strategy, formalized as a pipeline stage
instead of a one-off prompt.

**Hard rule: every finding must trace to a specific line in the posting.**
Not a generic resume-best-practices audit — a JD-relevance filter comes
first. A weakness that's true in the abstract ("no dollar-impact figure
anywhere") doesn't count unless it maps to something the posting actually
asks for, and the finding has to name which line. Caught in the first dry
run doing this loosely: "no dollar-value business-impact figure" got
flagged as a generic recruiter concern rather than tied to the JD's actual
"comfortable communicating findings and trade-offs to non-technical
stakeholders and leadership" line — a real connection, but it should have
been stated as that connection, not asserted as a universal truth. Worse,
that looseness let a real miss slide through: mentoring (a *nice-to-have*
line) got fixed, but stakeholder/leadership communication (a *required*
line, and a different ask than mentoring) did not — because the two got
bundled into one finding instead of checked as the two separate
requirements they are. Precision here matters as much as recall: sloppy
JD-mapping produces both false-positive findings (flagged but not actually
what the posting cares about) and false negatives (a real required-line
gap hiding behind a bundled, imprecise one).

It produces:
- The 5 strongest reasons to interview, grounded in what's actually on the
  page (sanity-checks that the strongest evidence actually made the cut)
- The 3 most likely reasons to reject — each one naming the specific JD
  line it fails to satisfy, required lines checked separately from
  nice-to-haves rather than lumped together
- Every required (not nice-to-have) JD line cross-checked individually
  against the résumé, even ones that feel adjacent to something already
  covered — adjacent isn't the same as covered
- Basic ATS-parseability sanity checks (consistent date formats, no
  tables/columns/graphics, standard section headers) — a real but
  usually-already-satisfied check given the fixed single-column template

On a finding that's fixable by re-selecting or re-surfacing existing
inventory evidence (wrong bullet got cut, a requirement's evidence exists
but wasn't chosen), loops back to the Generation step once. On a finding
that isn't fixable that way — a genuine gap in the evidence itself, not a
selection problem — no amount of rewriting closes it; that's a NEEDS_REVIEW
note for Matt, not something the pipeline should paper over by fabricating
or straining existing evidence to fit.

**Guardrails on the QA pass itself:** capped at 2 revision loops (cost/
latency control); anything still unresolved after that holds the
application in a NEEDS_REVIEW state for Matt to look at manually rather
than either silently shipping it or silently discarding it.

## 5. Guardrails (building these in regardless of the above)

- **Weekly application cap**, highest fit-score first — starts at ~100/week
  (Matt's call, overriding the more conservative "8-15 strong matches"
  pacing from the earlier search-strategy discussion), scaling up from
  there as long as the fit-score bar keeps being cleared by genuinely
  relevant postings rather than the bar dropping to fill a quota. The cap
  exists to keep a freak high-volume day from flooding the inbox with
  approval emails, not to hold volume down deliberately.
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
- **Least-privilege AWS creds for me**: when we get to the credentials
  step, best is a dedicated IAM user/role scoped to exactly the services
  above (Lambda, DynamoDB, S3, SQS, EventBridge, Bedrock, SES, Secrets
  Manager, IAM-limited, CloudWatch), not root/admin keys.

## 6. Build phases

1. **Decisions + foundation** — settle §3, stand up CDK skeleton, IAM,
   Secrets Manager, S3/DynamoDB tables.
2. **Ingestion** — Greenhouse/Lever/Ashby connectors, employer list,
   dedup/filter logic; verify HiringCafe feasibility.
3. **Scoring** — Bedrock evidence-audit prompt, fit threshold, weekly cap.
4. **Generation** — lane-specific structured-content rewrite (§1) against
   the accomplishment inventory.
5. **QA pass** — authenticity + grounding checks (§4), NEEDS_REVIEW path;
   fixed-template rendering to single-column/clean-parse-validated PDF.
6. **Approval loop** — send + reply detection per decision #1, TTL on
   pending approvals.
7. **Submission** — API-first submitters, Playwright fallback per
   decision #2, confirmation capture.
8. **Reporting** — weekly funnel digest email, CloudWatch dashboard.
9. **Guardrails hardening** — cap, budget alarm, kill switch, audit trail.

## 7. What's needed to start

- Master résumé + accomplishment inventory (raw material for the three
  lane variants: Senior DS / Applied MLE / Applied AI).
- Curated target-employer list (~100-200 cos), biased toward companies with
  genuine remote hiring for DS/ML/AI roles — see `target-employer-list.md`.
- Scoped AWS credentials (see least-privilege note in §4).
- A Google Cloud project for Gmail API OAuth (send + poll for the "ok"
  reply) — free tier, just needs the OAuth consent screen set up once and
  a refresh token generated.
