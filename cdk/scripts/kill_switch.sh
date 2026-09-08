#!/bin/bash
# ARCHITECTURE.md §5 kill switch: "one flag that halts ingestion and
# submission instantly."
#
#   ./kill_switch.sh on       halt
#   ./kill_switch.sh off      resume
#   ./kill_switch.sh status   what is it now
#
# It edits `paused` in config/ramp.json in the documents bucket. No
# deploy, no console, takes effect on the next invocation of anything —
# the Lambdas read it fresh every time rather than caching it, because
# the moment you flip this is the one moment staleness would matter.
#
# What it halts: both ingest Lambdas, fit-scoring, the approval email,
# the reply listener, the sweeper, and the local submission worker.
# What it deliberately does not: generation, QA, and render. Those are
# stream/SQS-triggered, so an early return eats the trigger and loses the
# work, and none of them send anything to an employer — they draft. Work
# already in flight finishes harmlessly; the sweeper picks up whatever
# the pause stranded once you turn it off again.
set -euo pipefail
PROFILE=${AWS_PROFILE:-job-applier}
source "$(dirname "$0")/../../.env.local" 2>/dev/null || true
BUCKET=job-applier-documents-${JOB_APPLIER_ACCOUNT_ID:?set it in .env.local}-${JOB_APPLIER_REGION:-us-east-1}
KEY=config/ramp.json
TMP=$(mktemp -t ramp)
trap 'rm -f "$TMP" "$TMP.new"' EXIT

aws s3 cp "s3://$BUCKET/$KEY" "$TMP" --profile "$PROFILE" --quiet

case "${1:-status}" in
  status)
    python3 - "$TMP" <<'PY'
import json, sys
c = json.load(open(sys.argv[1]))
paused = bool(c.get("paused", False))
print(f"kill switch: {'ON — pipeline halted' if paused else 'off — pipeline running'}")
print(f"weekly_cap:  {c.get('weekly_cap')}")
if paused and c.get("paused_at"):
    print(f"paused at:   {c['paused_at']}")
PY
    exit 0
    ;;
  on)  VALUE=true  ;;
  off) VALUE=false ;;
  *) echo "usage: $0 {on|off|status}" >&2; exit 2 ;;
esac

python3 - "$TMP" "$TMP.new" "$VALUE" <<'PY'
import json, sys, datetime
src, dst, value = sys.argv[1], sys.argv[2], sys.argv[3] == "true"
c = json.load(open(src))
c["paused"] = value
c["paused_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds") if value else None
json.dump(c, open(dst, "w"), indent=2)
PY

aws s3 cp "$TMP.new" "s3://$BUCKET/$KEY" --profile "$PROFILE" --quiet
if [ "$VALUE" = true ]; then
  echo "kill switch ON — ingestion, scoring, approval, replies, sweeper and the local worker are halted."
  echo "Anything mid-flight in generation/QA/render finishes; the sweeper will pick it up after 'off'."
else
  echo "kill switch off — pipeline running. The sweeper's next run (<=30 min) unsticks anything the pause stranded."
fi
