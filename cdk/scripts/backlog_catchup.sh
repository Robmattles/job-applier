#!/bin/bash
# One-time catch-up loop: repeatedly invoke job-applier-fit-scoring until
# the NEW backlog stops shrinking (or a safety iteration cap is hit).
# The Lambda's own DynamoDB lock makes this safe to run alongside the
# regular EventBridge schedule -- overlapping fires just skip cleanly.
set -u
PROFILE=job-applier
REGION=us-east-1
TABLE=job-applier-postings
MAX_ITERS=15
LOGFILE=/tmp/backlog_catchup.log

new_count() {
  aws dynamodb scan --table-name "$TABLE" --profile "$PROFILE" --region "$REGION" \
    --filter-expression "#s = :n" --expression-attribute-names '{"#s":"status"}' \
    --expression-attribute-values '{":n":{"S":"NEW"}}' \
    --select COUNT --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Count'])"
}

echo "$(date -u) starting catch-up loop" | tee -a "$LOGFILE"
prev=-1
for i in $(seq 1 $MAX_ITERS); do
  cur=$(new_count)
  echo "$(date -u) iteration $i: NEW=$cur" | tee -a "$LOGFILE"
  if [ "$cur" -eq 0 ]; then
    echo "$(date -u) backlog cleared" | tee -a "$LOGFILE"
    break
  fi
  if [ "$cur" -eq "$prev" ]; then
    echo "$(date -u) NEW count unchanged since last iteration ($cur) -- likely all remaining are erroring or locked out, stopping" | tee -a "$LOGFILE"
    break
  fi
  prev=$cur
  aws lambda invoke --function-name job-applier-fit-scoring --profile "$PROFILE" --region "$REGION" \
    --cli-read-timeout 950 /tmp/catchup_result_$i.json >> "$LOGFILE" 2>&1
  echo "$(date -u) iteration $i result: $(cat /tmp/catchup_result_$i.json 2>/dev/null)" | tee -a "$LOGFILE"
done
echo "$(date -u) catch-up loop finished" | tee -a "$LOGFILE"
