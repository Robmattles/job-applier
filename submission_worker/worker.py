"""job-applier submission worker — ARCHITECTURE.md §1 Submission Worker,
§6 phase 7.

Runs LOCALLY on Matt's machine, not in AWS (§3). The whole reason is the
handoff: a real visible Chrome window on his own screen needs no
remote-desktop session, where a cloud-hosted headless browser would.

    python3 submission_worker/worker.py            # process one, then stop
    python3 submission_worker/worker.py --loop     # keep draining the queue
    python3 submission_worker/worker.py --dry-run  # fill nothing, just show

It fills the form and submits it. The approval email is the human gate —
Matt already replied "ok" to this exact application, matching his
original ask ("if I reply saying ok, the application gets sent"). An
earlier version stopped short of clicking submit and made him drive the
browser too, which gated one decision twice; he asked for it simplified.

It still refuses to submit in three cases, where clicking would either
fail outright or file something wrong under his name (see
_submit_blockers): a CAPTCHA is present, a required field is still
empty, or the résumé didn't attach. Those hand back to him with the
browser open and the reason printed.

What it never does, regardless (§5):

  * It never touches voluntary self-identification (EEO) questions —
    applicant-profile.json marks those decline-by-default and explicitly
    "never auto-filled with a guess, never LLM-generated."
  * It never attempts a CAPTCHA.
  * It never invents an answer to a question it doesn't have grounded
    data for. Compensation, sponsorship, and LinkedIn are left blank.

`--review` restores the old fill-but-don't-submit behavior.

Aggregator-sourced postings (himalayas / remoteok / jobicy) have no
resolvable ATS form — the stored URL is a listing page, not an
application. Those open in the browser for Matt to drive manually rather
than guessing at a form structure, per §3's "straight to NEEDS_REVIEW
rather than building a fourth apply pathway."
"""
import argparse
import json
import os
import sys
import tempfile
import time

import boto3

PROFILE = os.environ.get("AWS_PROFILE", "job-applier")
REGION = "us-east-1"
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/ACCOUNT_ID/job-applier-submission-queue"
APPLICATIONS_TABLE = "job-applier-applications"
DOCUMENTS_BUCKET = "job-applier-documents-ACCOUNT_ID-us-east-1"
PROFILE_KEY = "source/applicant-profile.json"

# Sources whose stored URL is a listing page, not an application form.
AGGREGATOR_SOURCES = {"himalayas", "remoteok", "jobicy"}


def _session():
    return boto3.Session(profile_name=PROFILE, region_name=REGION)


def _apply_url(app: dict) -> str:
    """The employer's actual application form, reconstructed from the
    canonical board URL where we know how — the stored `url` is often the
    employer's own careers page with the form embedded in an iframe,
    which Playwright can't fill as directly as the board's own page."""
    # Prefer the URL the gate already verified reachable (§5
    # submittability check) — for an aggregator-sourced posting that's
    # the resolved employer form, not the listing article we ingested.
    verified = (app.get("apply_url") or "").strip()
    if verified:
        return verified

    application_id = app["application_id"]
    source, company, external_id = (application_id.split("#", 2) + ["", ""])[:3]
    if source == "greenhouse":
        return f"https://job-boards.greenhouse.io/{company}/jobs/{external_id}"
    if source == "ashby":
        # The job page itself has no form — confirmed live 2026-09-03,
        # it renders one stray search box. The application is a separate
        # route, where the real 36-field form lives.
        return f"https://jobs.ashbyhq.com/{company}/{external_id}/application"
    return app.get("url", "")


def _download_documents(s3, app: dict, workdir: str) -> dict:
    paths = {}
    for field, key in [
        ("resume", app.get("resume_pdf_key")),
        ("cover_letter", app.get("cover_letter_pdf_key")),
    ]:
        if not key:
            continue
        local = os.path.join(workdir, f"{field}.pdf")
        s3.download_file(DOCUMENTS_BUCKET, key, local)
        paths[field] = local
    return paths


def _load_profile(s3) -> dict:
    obj = s3.get_object(Bucket=DOCUMENTS_BUCKET, Key=PROFILE_KEY)
    return json.loads(obj["Body"].read().decode("utf-8"))


# --------------------------------------------------------------------------
# Form filling
# --------------------------------------------------------------------------

def _safe_autofill_values(profile: dict) -> dict:
    """Only fields applicant-profile.json marks SAFE_AUTOFILL. Note what
    is deliberately absent: voluntary_self_identification, work
    authorization phrasing that varies by form, and anything the profile
    marks NEEDS_REVIEW."""
    identity = profile.get("identity", {})
    full_name = identity.get("full_name", "")
    first, _, last = full_name.partition(" ")
    return {
        "first_name": first,
        "last_name": last,
        "full_name": full_name,
        "email": identity.get("email", ""),
        "phone": identity.get("phone", ""),
        "city": identity.get("city", ""),
        "state": identity.get("state", ""),
        "zip": identity.get("zip_code", "") or "",
        "location": f"{identity.get('city','')}, {identity.get('state','')}".strip(", "),
    }


# Label/name fragments → the profile key to fill from. Matched
# case-insensitively against a field's name, id, label, and placeholder.
FIELD_PATTERNS = [
    (("first name", "first_name", "firstname", "given name"), "first_name"),
    (("last name", "last_name", "lastname", "family name", "surname"), "last_name"),
    (("full name", "your name", "legal name", "_systemfield_name"), "full_name"),
    (("email",), "email"),
    (("phone", "mobile", "telephone"), "phone"),
    (("zip code", "postal code", "zip"), "zip"),
    (("city",), "city"),
    (("state", "province", "region"), "state"),
    (("location", "where are you based", "current location"), "location"),
]

# Fields that must never be auto-filled, matched the same way. Anything
# here is left for Matt even if a pattern above would otherwise match.
NEVER_AUTOFILL = (
    "gender", "race", "ethnicity", "veteran", "disability", "sexual orientation",
    "hispanic", "lgbt", "self-identif", "self identif", "eeo",
    "salary", "compensation", "desired pay", "expected pay",
    "sponsor", "visa", "work authorization", "authorized to work",
    "criminal", "felony", "background check", "security clearance",
    "reason for leaving", "why are you leaving",
    # applicant-profile.json: deliberately no LinkedIn, and a form that
    # requires one is NEEDS_REVIEW rather than a fabricated profile URL.
    "linkedin",
)


def _question_text(handle, descriptor: str) -> str:
    """The human-readable question for a field, preferred over its
    machine name — Greenhouse names custom questions `question_67590188`,
    which tells the model nothing, while the associated label carries the
    actual text."""
    for getter in (
        lambda: handle.get_attribute("aria-label"),
        lambda: handle.evaluate(
            "el => { const l = el.labels && el.labels[0]; return l ? l.innerText : null; }"
        ),
        lambda: handle.evaluate(
            "el => { const w = el.closest('div'); return w ? w.innerText : null; }"
        ),
    ):
        try:
            text = (getter() or "").strip()
        except Exception:  # noqa: BLE001
            continue
        if text and len(text) > 8:
            return " ".join(text.split())[:300]
    return descriptor


def _match_field(descriptor: str):
    d = descriptor.lower()
    if any(bad in d for bad in NEVER_AUTOFILL):
        return None
    for fragments, key in FIELD_PATTERNS:
        if any(f in d for f in fragments):
            return key
    return None


def fill_form(page, values: dict, documents: dict, dry_run: bool = False) -> dict:
    """Returns a report of what was filled, skipped, and left for Matt.

    Scans every frame, not just the top document. Confirmed live
    2026-09-03: `job-boards.greenhouse.io/{co}/jobs/{id}` 302s to the
    employer's own careers page, which embeds the real Greenhouse form in
    an `/embed/job_app` iframe — a top-document-only scan found exactly
    zero fields on a page with nineteen of them, and would have silently
    "filled" nothing while reporting success."""
    report = {"filled": [], "skipped_sensitive": [], "unmapped": [], "uploads": [],
              "skipped_autofill": [], "_open_questions": []}

    handles = []
    for frame in page.frames:
        # reCAPTCHA's own frames are never touched — §5, and there's
        # nothing fillable in them anyway.
        if "recaptcha" in (frame.url or "").lower():
            continue
        try:
            handles.extend(frame.query_selector_all("input, textarea, select"))
        except Exception:  # noqa: BLE001 — a detached frame mid-scan isn't fatal
            continue

    for handle in handles:
        try:
            if not handle.is_visible():
                continue
            input_type = (handle.get_attribute("type") or "").lower()
            if input_type in ("hidden", "submit", "button", "checkbox", "radio"):
                continue

            descriptor = " ".join(
                filter(None, [
                    handle.get_attribute("name") or "",
                    handle.get_attribute("id") or "",
                    handle.get_attribute("aria-label") or "",
                    handle.get_attribute("placeholder") or "",
                ])
            )
            # The rendered label has to be part of what we match on, not
            # just the machine name. Confirmed live 2026-09-03 against
            # Ashby, which names every custom field with a bare UUID
            # (`4f00b6bc-689c-420a-...`): matching on the name alone
            # missed the phone field entirely, and — the part that
            # matters — the NEVER_AUTOFILL guard never fired on "What
            # are your compensation expectations?" because there is no
            # word in the field name to catch. A safety list that only
            # reads machine names is no safety list on a form like this.
            descriptor = f"{descriptor} {_question_text(handle, '')}".strip()
            if input_type == "file":
                d = descriptor.lower()
                # Never feed the ATS's own résumé-parsing autofill widget.
                # Confirmed live 2026-09-03 on Ashby: it has a separate
                # "Autofill from resume" input alongside the real
                # Resume/CV slot, and dropping the PDF there fires their
                # parser, which then overwrites the fields we just filled
                # with whatever it scraped — Matt watched it clobber his
                # form mid-run. The real attachment slot is the only one
                # worth touching; our values are already the canonical
                # ones and don't need round-tripping through a parser.
                if "autofill" in d or "auto-fill" in d or "parse" in d:
                    report["skipped_autofill"].append(descriptor.strip()[:60])
                    continue
                # Resume/cover-letter uploads are the one place a wrong
                # guess is harmless — worst case Matt re-attaches.
                target = "cover_letter" if "cover" in d else "resume"
                if target in documents and not dry_run:
                    handle.set_input_files(documents[target])
                report["uploads"].append(f"{descriptor.strip()[:60]} <- {target}")
                continue

            key = _match_field(descriptor)
            if key is None:
                if any(bad in descriptor.lower() for bad in NEVER_AUTOFILL):
                    report["skipped_sensitive"].append(descriptor.strip()[:60])
                elif descriptor.strip():
                    report["unmapped"].append(descriptor.strip()[:60])
                    # Keep the handle so the caller can offer a grounded
                    # draft for it — the label is usually the real
                    # question text ("Do you have consulting experience
                    # working with external clients?").
                    report["_open_questions"].append((_question_text(handle, descriptor), handle))
                continue

            value = values.get(key, "")
            if not value:
                continue
            if not dry_run:
                handle.fill(value)
            report["filled"].append(f"{descriptor.strip()[:40]} = {value}")
        except Exception as e:  # noqa: BLE001 — one odd field shouldn't abort the form
            report["unmapped"].append(f"(error reading field: {e})")

    return report


# --------------------------------------------------------------------------
# Main flow
# --------------------------------------------------------------------------

def _submit_blockers(page, documents: dict) -> list:
    """Reasons this form must not be auto-submitted. Empty list = safe.

    The approval email is the human gate — Matt already said "ok" to this
    exact application, so a second manual gate at the browser is gating
    him twice for one decision. But "he approved the application" is not
    "anything the page throws up is fine," and these three are the cases
    where clicking submit would either fail or be wrong:
    """
    blockers = []

    # 1. A CAPTCHA. §5 is absolute: never attempt one. Submitting into a
    #    live challenge fails anyway, usually silently.
    for frame in page.frames:
        if "recaptcha" in (frame.url or "").lower() or "hcaptcha" in (frame.url or "").lower():
            if frame.query_selector("iframe, .g-recaptcha, #captcha"):
                blockers.append("CAPTCHA present — yours to complete")
                break

    # 2. Required fields still empty. Submitting incomplete either bounces
    #    off validation or files a half-blank application under his name.
    for frame in page.frames:
        try:
            handles = frame.query_selector_all("input[required], textarea[required], select[required]")
        except Exception:  # noqa: BLE001
            continue
        for h in handles:
            try:
                if not h.is_visible():
                    continue
                t = (h.get_attribute("type") or "").lower()
                if t in ("hidden", "submit", "button"):
                    continue
                if t == "file":
                    continue  # uploads are verified separately below
                if not (h.input_value() or "").strip():
                    label = _question_text(h, h.get_attribute("name") or "a required field")
                    blockers.append(f"required and empty: {label[:70]}")
            except Exception:  # noqa: BLE001
                continue

    # 3. The résumé never attached. An application without it is worse
    #    than no application.
    if "resume" in documents:
        attached = False
        for frame in page.frames:
            try:
                for h in frame.query_selector_all("input[type=file]"):
                    d = (h.get_attribute("name") or "") + (h.get_attribute("id") or "")
                    if "autofill" in d.lower():
                        continue
                    if h.evaluate("el => el.files && el.files.length > 0"):
                        attached = True
            except Exception:  # noqa: BLE001
                continue
        if not attached:
            blockers.append("résumé did not attach")

    return blockers


def _click_submit(page) -> bool:
    for sel in (
        "button[type=submit]",
        "input[type=submit]",
        "button:has-text('Submit application')",
        "button:has-text('Submit Application')",
        "button:has-text('Submit')",
        "button:has-text('Apply')",
    ):
        for frame in page.frames:
            try:
                el = frame.query_selector(sel)
            except Exception:  # noqa: BLE001
                continue
            if el and el.is_visible() and el.is_enabled():
                el.click()
                page.wait_for_timeout(5000)
                return True
    return False


def _offer_drafts(session, open_questions: list, app: dict, profile: dict, auto: bool = False):
    """Draft an answer per open question and let Matt accept, edit, or
    skip each one. Nothing here is ever filled without him seeing it —
    these answers go into a real application, and unlike a résumé bullet
    nothing downstream reviews them again."""
    import answers as answers_module

    bedrock = session.client("bedrock-runtime")
    s3 = session.client("s3")
    inventory = json.loads(
        s3.get_object(Bucket=DOCUMENTS_BUCKET, Key="source/accomplishment-inventory.json")["Body"].read()
    )
    # The FULL inventory, deliberately not lane-filtered. Lane filtering
    # is right for choosing résumé bullets (tailoring) and wrong here:
    # confirmed live 2026-09-03, asking "6+ years deploying ML to
    # production?" against the applied_ai slice alone drew a flat "No,
    # approximately 2-3 years" — because that lane holds only the recent
    # GenAI work, with the FBI ML systems and the NICB entity-resolution
    # and fraud-detection production work all filtered out. An
    # application question is about the whole career, and understating
    # here disqualifies him from a role he plausibly meets, which is
    # just as damaging as overstating.
    records = [
        {k: r.get(k) for k in ("id", "role", "text", "metrics", "skills", "priority")}
        for r in inventory.get("records", [])
    ]
    exp = profile.get("experience_summary", {})
    career_facts = {
        "total_years_professional_experience": exp.get("total_years_professional_experience"),
        "years_data_science_ml": exp.get("years_data_science_ml"),
        # The caveat travels with the number on purpose — the profile's
        # own note says the 7 could reasonably be counted as ~5, and a
        # draft that states it flatly is overstating a contested figure.
        "years_counting_caveat": exp.get("_inferred"),
        "education": exp.get("education"),
        "notable_certifications": exp.get("notable_certifications"),
    }
    posting = {"title": app.get("title", ""), "company_name": app.get("company_name", "")}

    print("\n" + "=" * 70)
    print(f"  {len(open_questions)} question(s) this form asks that aren't autofillable")

    for question, handle in open_questions:
        kind = answers_module.classify_question(question)
        print("\n  " + "-" * 66)
        print(f"  Q: {question[:220]}")
        if kind != "GENERATE_GROUNDED":
            print(f"  -> {kind}: yours to answer, leaving it blank.")
            continue

        try:
            draft = answers_module.draft_answer(bedrock, question, records, career_facts, posting)
        except Exception as e:  # noqa: BLE001 — a failed draft is a blank field, not a crash
            print(f"  -> drafting failed ({e}); leaving blank.")
            continue

        flag = "  [concedes a gap]" if draft.get("is_honest_negative") else ""
        print(f"  -> draft (confidence: {draft.get('confidence')}){flag}")
        print(f"     grounded in: {', '.join(draft.get('grounded_in') or []) or '(career facts)'}")
        for line in (draft.get("answer") or "").splitlines():
            print(f"     {line}")

        if auto:
            handle.fill(draft["answer"])
            print("     filled (auto).")
            continue

        choice = input("\n     [a]ccept / [e]dit / [s]kip? ").strip().lower()
        if choice.startswith("a"):
            handle.fill(draft["answer"])
            print("     filled.")
        elif choice.startswith("e"):
            edited = input("     your answer: ").strip()
            if edited:
                handle.fill(edited)
                print("     filled with your text.")
        else:
            print("     left blank.")


def process_application(app: dict, dry_run: bool = False, auto: bool = True) -> str:
    from playwright.sync_api import sync_playwright

    session = _session()
    s3 = session.client("s3")
    applications = session.resource("dynamodb").Table(APPLICATIONS_TABLE)

    application_id = app["application_id"]
    source = application_id.split("#", 1)[0]
    url = _apply_url(app)

    print("=" * 70)
    print(f"{app.get('company_name','')} — {app.get('title','')}")
    print(f"  application_id: {application_id}")
    print(f"  form: {url}")

    workdir = tempfile.mkdtemp(prefix="job-applier-")
    documents = _download_documents(s3, app, workdir)
    profile = _load_profile(s3)
    values = _safe_autofill_values(profile)
    print(f"  documents: {', '.join(documents) or 'none'} (in {workdir})")

    if source in AGGREGATOR_SOURCES:
        print(f"\n  NOTE: {source} is an aggregator listing, not an application form.")
        print("  Opening it for you to navigate to the employer's real form yourself.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=45000)

        report = {"filled": [], "skipped_sensitive": [], "unmapped": [], "uploads": [],
              "skipped_autofill": [], "_open_questions": []}
        if source not in AGGREGATOR_SOURCES:
            # The Greenhouse embed iframe finishes loading well after the
            # host page does; scanning too early finds an empty form.
            page.wait_for_timeout(3000)
            report = fill_form(page, values, documents, dry_run=dry_run)

        print("\n  filled:")
        for line in report["filled"] or ["    (none)"]:
            print(f"    {line}")
        if report["uploads"]:
            print("  uploaded:")
            for line in report["uploads"]:
                print(f"    {line}")
        if report["skipped_autofill"]:
            print("  skipped the ATS resume-parser widget (it overwrites our fields):")
            for line in report["skipped_autofill"]:
                print(f"    {line}")
        if report["skipped_sensitive"]:
            print("  left blank deliberately (sensitive — yours to answer):")
            for line in report["skipped_sensitive"]:
                print(f"    {line}")
        if report["unmapped"]:
            print("  not recognized (check these):")
            for line in report["unmapped"]:
                print(f"    {line}")

        if report["_open_questions"] and not dry_run:
            _offer_drafts(session, report["_open_questions"], app, profile, auto=auto)

        print("\n" + "-" * 70)
        if dry_run:
            print("  (dry run — nothing filled, nothing submitted)")
            answer = "s"
        elif source in AGGREGATOR_SOURCES:
            print("  No employer form here — the listing is all this source exposes.")
            answer = input("\n  Did you submit it yourself? [y]es / [n]o / [s]kip: ").strip().lower()
        elif not auto:
            print("  --review: filled but not submitted. Over to you.")
            answer = input("\n  Did you submit it? [y]es / [n]o / [s]kip: ").strip().lower()
        else:
            blockers = _submit_blockers(page, documents)
            if blockers:
                print("  NOT submitting — these need you:")
                for bl in blockers:
                    print(f"    - {bl}")
                answer = input("\n  Did you submit it? [y]es / [n]o / [s]kip: ").strip().lower()
            elif _click_submit(page):
                print(f"  SUBMITTED -> {page.url[:88]}")
                answer = "y"
            else:
                print("  no submit button found — over to you.")
                answer = input("\n  Did you submit it? [y]es / [n]o / [s]kip: ").strip().lower()
        browser.close()

    now = int(time.time())
    if answer.startswith("y"):
        applications.update_item(
            Key={"application_id": application_id},
            UpdateExpression="SET #s = :s, submitted_at = :ts, submitted_via = :v",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "SUBMITTED", ":ts": now, ":v": url},
        )
        print(f"  recorded as SUBMITTED\n")
        return "submitted"
    if answer.startswith("n"):
        applications.update_item(
            Key={"application_id": application_id},
            UpdateExpression="SET #s = :s, abandoned_at = :ts",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "NOT_SUBMITTED", ":ts": now},
        )
        print("  recorded as NOT_SUBMITTED\n")
        return "not_submitted"
    print("  left APPROVED — it stays in the queue for next time\n")
    return "skipped"


def main() -> int:
    parser = argparse.ArgumentParser(description="job-applier local submission worker")
    parser.add_argument("--loop", action="store_true", help="keep draining the queue")
    parser.add_argument("--dry-run", action="store_true", help="open and inspect, fill nothing")
    parser.add_argument("--review", action="store_true",
                        help="fill but never submit — you review and click submit yourself")
    args = parser.parse_args()

    session = _session()
    sqs = session.client("sqs")
    applications = session.resource("dynamodb").Table(APPLICATIONS_TABLE)

    while True:
        resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        messages = resp.get("Messages", [])
        if not messages:
            print("nothing waiting in the submission queue")
            return 0

        message = messages[0]
        application_id = json.loads(message["Body"])["application_id"]
        app = applications.get_item(Key={"application_id": application_id}).get("Item")

        if app is None:
            print(f"{application_id}: no application row, dropping message")
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            continue
        if app.get("status") != "APPROVED":
            print(f"{application_id}: status is {app.get('status')}, not APPROVED — dropping")
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            continue

        outcome = process_application(app, dry_run=args.dry_run, auto=not args.review)
        if outcome in ("submitted", "not_submitted"):
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])

        if not args.loop:
            return 0


if __name__ == "__main__":
    sys.exit(main())
