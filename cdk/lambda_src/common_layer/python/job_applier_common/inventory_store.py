"""Loads accomplishment-inventory.json from S3, cached per warm Lambda
container — it's ~47KB and doesn't change between invocations, no reason
to re-fetch it from S3 on every posting scored."""
import json
import os
import re

import boto3

_cache = None


def load_inventory() -> dict:
    global _cache
    if _cache is None:
        s3 = boto3.client("s3")
        bucket = os.environ["DOCUMENTS_BUCKET"]
        key = os.environ.get("INVENTORY_KEY", "source/accomplishment-inventory.json")
        obj = s3.get_object(Bucket=bucket, Key=key)
        _cache = json.loads(obj["Body"].read().decode("utf-8"))
    return _cache


_ramp_cache = None


def load_weekly_cap(default: int) -> int:
    """The §5 weekly cap, read from `config/ramp.json` in the documents
    bucket rather than a Lambda env var.

    The ramp is deliberately manual — §5 raises the cap "only if the
    false-positive rate and QA-failure rate are actually low," which is a
    judgment about real results, not something a calendar should
    escalate. But manual shouldn't mean *a code edit and a CDK deploy*:
    that's enough friction to discourage lowering it in a hurry, which is
    exactly when it matters most. An S3 object is editable in one
    command, and scoring already reads this bucket, so it needs no new
    permissions.

    Falls back to the env-var default if the object is missing or
    malformed — a broken config file should not silently uncap
    submissions."""
    global _ramp_cache
    if _ramp_cache is None:
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(
                Bucket=os.environ["DOCUMENTS_BUCKET"],
                Key=os.environ.get("RAMP_CONFIG_KEY", "config/ramp.json"),
            )
            _ramp_cache = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            print(f"ramp config unreadable ({e}); falling back to WEEKLY_CAP={default}")
            _ramp_cache = {}
    value = _ramp_cache.get("weekly_cap", default)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        print(f"ramp config weekly_cap={value!r} isn't an int; using {default}")
        return default


def load_cap_reset_at() -> int:
    """`cap_reset_at` in config/ramp.json: approval emails sent at or
    before this epoch don't count toward the weekly cap.

    The cap bounds *how much review lands on Matt*. A day spent proving
    the pipeline works spends that budget without ever costing him
    review — 2026-09-03 sent 25 emails, most of which Gmail spam-filtered
    so he never saw them, and the handful he did engage with were
    debugging exercises rather than real hiring decisions. Zeroing the
    counter by deleting or backdating those rows would corrupt the audit
    trail (and marking them WITHDRAWN would silently break replies to
    the ones still open), so instead this records an explicit "start
    counting here" line, kept alongside the cap's own change history.

    Read uncached, like the kill switch and for the same reason: it's
    flipped in the moment you want it to take effect."""
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(
            Bucket=os.environ["DOCUMENTS_BUCKET"],
            Key=os.environ.get("RAMP_CONFIG_KEY", "config/ramp.json"),
        )
        return int(json.loads(obj["Body"].read().decode("utf-8")).get("cap_reset_at", 0) or 0)
    except Exception as e:  # noqa: BLE001
        print(f"cap_reset_at unreadable ({e}); counting the full window")
        return 0


def is_paused() -> bool:
    """The §5 kill switch: `"paused": true` in `config/ramp.json` halts
    ingestion and everything outbound, instantly and with no deploy.

    Deliberately NOT cached, unlike load_weekly_cap's `_ramp_cache`. A
    warm container holding a stale `paused: false` for its whole lifetime
    would defeat the entire point — the one moment this flag matters is
    the moment you flip it, and "instantly" in §5 is the requirement, not
    a nice-to-have. One extra S3 GET per invocation against a 300-byte
    object is not a cost worth optimizing here.

    Fails OPEN (returns False) if the object is unreadable, matching
    load_weekly_cap's fallback: a transient S3 error is not a stop
    signal, and a pipeline that halts itself on a blip would be its own
    outage. The tradeoff is explicit — this halts a *running* system on
    request, it is not a safety interlock.

    What it does NOT stop, by design: generation, QA, and render. Those
    are stream/SQS-triggered, so returning early consumes the trigger and
    silently loses the work, and none of them send anything to an
    employer or ingest anything new — they draft. §5 asks for ingestion
    and submission to halt; drafting in flight finishing harmlessly is
    the correct behavior, and the sweeper picks up anything the pause
    stranded once it's lifted."""
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(
            Bucket=os.environ["DOCUMENTS_BUCKET"],
            Key=os.environ.get("RAMP_CONFIG_KEY", "config/ramp.json"),
        )
        config = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"kill switch unreadable ({e}); continuing unpaused")
        return False
    return bool(config.get("paused", False))


def halt_if_paused(what: str) -> bool:
    """`if inventory_store.halt_if_paused("ingestion"): return {...}` —
    the one line every haltable Lambda opens with."""
    if is_paused():
        print(f"KILL SWITCH ON (config/ramp.json paused=true) — {what} halted, no work done")
        return True
    return False


def capability_boundaries(inventory: dict) -> str:
    """Renders meta.capability_boundaries for a prompt, or "" if unset.

    The absence of evidence is not a signal a generator reads reliably —
    given a posting asking for something the inventory doesn't cover, it
    reaches for the nearest adjacent record and relabels it rather than
    concluding the candidate doesn't match. Confirmed live 2026-09-04
    twice over: graduate coursework became a "Causal Inference" headline,
    and a Redshift workload-management change became "end-to-end
    experimentation infrastructure... exactly the experimentation backbone
    the posting describes." Stating the boundaries positively is what
    makes them visible; a missing tag never was."""
    b = (inventory.get("meta", {}) or {}).get("capability_boundaries") or {}
    lines = [v for k, v in b.items() if not k.startswith("_") and isinstance(v, str)]
    if not lines:
        return ""
    return "THINGS THIS CANDIDATE HAS NOT DONE (never claim, never relabel adjacent work as):\n" + \
           "\n".join(f"- {t}" for t in lines)


def compact_records(inventory: dict, lane: str = None, include_role: bool = False) -> list:
    """Strip inventory records to what a prompt needs, dropping `source`
    (the grounding-QA pass's traceability field, not needed by any
    Bedrock call) and `role` unless asked for. Shared by fit-scoring
    (lane=None — it's the one choosing the lane — role never needed, an
    evidence audit doesn't care which job a record happened under) and
    generation (lane= the one fit-scoring already picked; include_role=
    True — the renderer (§6 phase 5) groups résumé bullets under a
    "Company — Title (Dates)" header per role, so generation has to know
    which role each bullet it writes belongs to) so the record shape
    sent to Bedrock lives in one place."""
    records = inventory.get("records", [])
    if lane:
        records = [r for r in records if lane in r.get("lanes", [])]
    compact = [
        {
            "id": r["id"],
            "text": r["text"],
            "metrics": r.get("metrics", []),
            "skills": r.get("skills", []),
            "lanes": r.get("lanes", []),
            "capability_tags": r.get("capability_tags", []),
            "priority": r.get("priority"),
        }
        for r in records
    ]
    if include_role:
        for c, r in zip(compact, records):
            c["role"] = r.get("role", "")
    return compact


def compute_skills_section(inventory: dict, lane: str, jd_text: str = "", max_skills: int = 20) -> list:
    """Confirmed live 2026-09-02: asking Bedrock to author the résumé
    skills list itself failed on effectively every generation run —
    plausible-sounding but unattested terms (Prompt Engineering, Multi-
    Agent Orchestration, Token Budget Management, RAG Architecture, and
    further afield ones like PyTorch/TensorFlow/Kubernetes with no
    grounding anywhere) despite an explicit instruction to only use real
    `skills` tags. Computed here instead of asked for.

    Ranking, also confirmed live 2026-09-02 as needing a real fix, not
    just grounding: an unranked, uncapped union surfaced things like a
    stray "Python zipfile/SpreadsheetML" or "BM25" — single-record,
    narrow implementation trivia — with equal footing to Python or AWS
    Bedrock, and near-duplicate tags (Titan embeddings / Titan v2
    embeddings from two different records) sitting side by side. Matt's
    own framing: "start with broadly applicable, in demand skills,
    especially relevant to jd, work down from there. cut off at a
    reasonable length." So: (1) JD-relevant first — a skill whose text
    appears in the posting as a whole word/phrase (word-boundary regex,
    not plain substring containment — confirmed live 2026-09-02 that
    "SES" as a bare substring check matches inside ordinary words like
    "proces-SES" or "as-SES-ses", meaning almost any real JD text would
    false-positive-match short tags and shove them to the top for no
    real reason); still no LLM call, so still no fabrication risk, just
    imperfect recall on paraphrases; (2) then not-administrative-plumbing
    (_ADMIN_PLUMBING below) — confirmed live 2026-09-02 that SES still
    ranked #5 with a correct, non-false-positive JD check: it's tagged on
    a priority-1 flagship record, so priority+frequency alone rated it
    "broadly applicable," but "used inside one flagship project" isn't
    the same claim as "a core, in-demand ML/DS skill" — SES is
    incidental email plumbing inside that project, not a differentiator,
    and no amount of tuning priority/frequency weights fixes a category
    error; (3) then by how many distinct records mention it, as a proxy
    for "broadly applicable to the whole practice" rather than a one-off
    detail from a single project; (4) best (lowest) source-record
    priority as a final tiebreak; (5) hard-capped at max_skills — a
    narrow single-record tag not called out by the JD now has to
    outrank on frequency to make the cut at all, rather than appearing
    just because it technically exists somewhere."""
    records = compact_records(inventory, lane=lane, include_role=False)
    jd_lower = (jd_text or "").lower()

    freq, best_priority, display = {}, {}, {}
    for r in records:
        for s in r.get("skills", []):
            key = s.lower()
            freq[key] = freq.get(key, 0) + 1
            best_priority[key] = min(best_priority.get(key, 99), r.get("priority") or 99)
            display.setdefault(key, s)

    def jd_relevant(key: str) -> bool:
        return bool(re.search(r"\b" + re.escape(key) + r"\b", jd_lower))

    # Operational/administrative AWS services that support a project but
    # aren't themselves an ML/DS/platform differentiator — messaging,
    # logging, migration tooling. Deliberately NOT here: S3, Lambda, EC2,
    # ECS/Fargate, Docker, DynamoDB, Aurora (Postgres) — those are
    # genuine platform/data-engineering skills, not administrative
    # plumbing, even though they're also "AWS services." AWS Neptune is
    # excluded too — it's the actual graph database behind real graph-ML
    # analysis work, not incidental infrastructure.
    _ADMIN_PLUMBING = {
        "ses", "cloudwatch", "cloudtrail", "eventbridge", "sns", "aws dms", "performance insights",
    }

    keys = sorted(
        freq,
        key=lambda k: (not jd_relevant(k), k in _ADMIN_PLUMBING, best_priority[k], -freq[k]),
    )
    return [display[k] for k in keys[:max_skills]]


def apply_role_history_fallback(content: dict, inventory: dict) -> dict:
    """Guarantees every role in the inventory's role_history roster
    appears in resume_bullets at least once — no exceptions, no lane
    filter. Confirmed live 2026-09-02, Matt's own words: "regardless of
    whether it's directly relevant to the jd, the roles need to be
    listed and at least briefly described" — a résumé that silently
    omits real employment history (which generation's own lane-filtered
    evidence selection would otherwise do for any role with zero
    lane-tagged records) reads as incomplete or evasive, not
    well-tailored. Falls back to the roster's own neutral, Matt-
    confirmed description (source_record_ids left empty — this is
    roster fact, not evidence tied to one accomplishment record; QA's
    grounding check treats an empty list as "already fact-checked
    outside this pipeline," not a citation to verify). Shared by
    generation (applies it once) and QA (re-applies after every
    revision loop, same reasoning as compute_skills_section above — a
    revise() call that hands the model the full content object and
    asks for "the same shape back" is exactly the kind of opening that
    let a fabrication back in before, so nothing here gets the benefit
    of the doubt from prompting alone)."""
    role_history = inventory.get("meta", {}).get("role_history", [])
    represented = {b.get("role", "") for b in content.get("resume_bullets", [])}
    for entry in role_history:
        role = entry["role"]
        if role in represented:
            continue
        content.setdefault("resume_bullets", []).append({
            "text": entry["neutral_description"],
            "source_record_ids": [],
            "role": role,
        })
    return content
