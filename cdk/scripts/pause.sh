#!/bin/bash
# Pause the whole system to near-zero cost, reversibly.
#
#   ./pause.sh on       stop everything
#   ./pause.sh off      start it again
#   ./pause.sh status   what state is it in
#
# What this is, versus kill_switch.sh: the kill switch is the §5 safety
# control — one flag, instant, halts work. This wraps it with the other
# things that have to happen for a *deliberate multi-day pause*: the local
# watcher, and log retention so CloudWatch doesn't grow forever while
# nobody's looking.
#
# What it costs while paused, measured 2026-09-08:
#   - Lambdas still fire on schedule but return immediately on the kill
#     switch (~100ms). ~10,600 invocations/month is about $0.01.
#   - DynamoDB is PAY_PER_REQUEST, so an idle table is storage only: ~9MB
#     across both tables, a fraction of a cent.
#   - S3 (documents + generated PDFs) and idle SQS queues: pennies at most.
#   Total: effectively free. Bedrock is the entire real cost of this
#   system, and it stops completely — nothing is scored, generated, or QA'd.
#
# Deliberately NOT done here: disabling the EventBridge rules or running
# `cdk destroy`. Disabling rules saves that last ~$0.01/month but needs
# events:DisableRule, which the scoped job-applier policy doesn't grant —
# so resuming would require re-attaching AdministratorAccess. Not worth
# trading a least-privilege resume for a penny. `cdk destroy` would drop
# the DynamoDB tables, which hold every posting scored and every
# application generated to date; that's the expensive thing to rebuild,
# not the infrastructure.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROFILE=${AWS_PROFILE:-job-applier}
LABEL=com.mattbarr.job-applier-watcher

case "${1:-status}" in
  status)
    "$HERE/kill_switch.sh" status
    if launchctl list 2>/dev/null | grep -q "$LABEL"; then
      echo "watcher:     running (local browser auto-launch is live)"
    else
      echo "watcher:     stopped"
    fi
    exit 0
    ;;

  on)
    "$HERE/kill_switch.sh" on
    # Uninstall, don't just bootout. Confirmed 2026-10-01: `bootout` only
    # unloads for the current session, and the plist sets RunAtLoad=true —
    # so every login resurrected the watcher. Three weeks later there were
    # three of them running at once. They were harmlessly no-opping on the
    # kill switch, but "paused" has to survive a reboot, and three watchers
    # racing to launch browsers is exactly the duplicate-launch bug the
    # per-application cooldown exists to prevent.
    "$HERE/../../submission_worker/install-watcher.sh" uninstall >/dev/null 2>&1 || true
    pkill -f 'submission_worker/watcher.py' 2>/dev/null || true
    sleep 1
    pkill -9 -f 'submission_worker/watcher.py' 2>/dev/null || true
    echo "watcher uninstalled (survives reboot)."
    # Bound CloudWatch growth. Every log group defaulted to never-expire;
    # tiny today (~1.3MB) but it only goes one direction, and a paused
    # system is exactly when nobody notices.
    # Needs logs:DescribeLogGroups, which the scoped policy doesn't grant —
    # this was a one-time fix applied under admin on 2026-09-08 and is a
    # no-op afterwards, so a failure here is expected and not worth noise.
    for lg in $(aws logs describe-log-groups --region us-east-1 --profile "$PROFILE" \
                  --log-group-name-prefix /aws/lambda/job-applier \
                  --query 'logGroups[?retentionInDays==`null`].logGroupName' --output text 2>/dev/null || true); do
      aws logs put-retention-policy --region us-east-1 --profile "$PROFILE" \
        --log-group-name "$lg" --retention-in-days 30 && echo "  log retention 30d: $lg"
    done
    echo
    echo "PAUSED. Nothing is scored, generated, QA'd, emailed, or submitted."
    echo "Cost while paused is effectively zero — Bedrock is the whole bill and it has stopped."
    echo "Resume with: $0 off"
    ;;

  off)
    "$HERE/kill_switch.sh" off
    "$HERE/../../submission_worker/install-watcher.sh" >/dev/null
    echo "watcher restarted."
    echo
    echo "RUNNING again. Ingestion resumes on its next 4-hour tick; scoring at :30 past every 4th hour."
    echo "Note the weekly cap still applies — check '$HERE/pause.sh status' and config/ramp.json if"
    echo "approval emails don't appear (a full cap looks exactly like a broken pipeline)."
    ;;

  *) echo "usage: $0 {on|off|status}" >&2; exit 2 ;;
esac
