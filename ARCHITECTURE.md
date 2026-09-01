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
[Dedup + Filter]  → DynamoDB "seen postings" table; title regex; posting
   │                 age < 48h preferred, hard cutoff otherwise
   ▼
[Fit-Scoring Lambda] → Bedrock (cheap model, e.g. Claude Haiku) does an
   │                    "evidence audit" against the master résumé: fit
   │                    score, reasons to interview/reject, lane pick
   │                    (Senior DS / Applied MLE / Applied AI)
   ▼
[Threshold gate + weekly cap] → only postings above the fit-score bar
   │                             proceed; starts at ~100/week (scale up from
   │                             there as long as the bar is genuinely
   │                             being cleared, not lowered to hit a
   │                             number), highest score first, so a busy
   │                             day doesn't flood the inbox
   ▼
[Résumé/Letter Generation Lambda] → Bedrock (higher-quality model, e.g.
   │                                 Claude Sonnet) rewrites the chosen
   │                                 lane template using the evidence
   │                                 audit; renders single-column PDF;
   │                                 stores in S3
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
| LLM | Bedrock — tiered: a cheap/fast model for the first-pass fit score on every posting (high volume), a stronger model for the résumé rewrite + evidence audit on postings that clear the bar (low volume, quality matters) |
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

## 4. Guardrails (building these in regardless of the above)

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

## 5. Build phases

1. **Decisions + foundation** — settle §3, stand up CDK skeleton, IAM,
   Secrets Manager, S3/DynamoDB tables.
2. **Ingestion** — Greenhouse/Lever/Ashby connectors, employer list,
   dedup/filter logic; verify HiringCafe feasibility.
3. **Scoring** — Bedrock evidence-audit prompt, fit threshold, weekly cap.
4. **Generation** — lane-specific résumé rewrite + PDF rendering,
   single-column/clean-parse validated.
5. **Approval loop** — send + reply detection per decision #1, TTL on
   pending approvals.
6. **Submission** — API-first submitters, Playwright fallback per
   decision #2, confirmation capture.
7. **Reporting** — weekly funnel digest email, CloudWatch dashboard.
8. **Guardrails hardening** — cap, budget alarm, kill switch, audit trail.

## 6. What's needed to start

- Master résumé + accomplishment inventory (raw material for the three
  lane variants: Senior DS / Applied MLE / Applied AI).
- Curated target-employer list (~100-200 cos) — can draft this together.
- Scoped AWS credentials (see least-privilege note in §4).
- A Google Cloud project for Gmail API OAuth (send + poll for the "ok"
  reply) — free tier, just needs the OAuth consent screen set up once and
  a refresh token generated.
