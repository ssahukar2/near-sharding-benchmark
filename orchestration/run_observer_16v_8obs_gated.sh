#!/usr/bin/env bash
# 16 validators + 8 observers + 8 external submitters, with monitor gated
# on the harness 'Run: python3 scripts/monitor.py' line.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

python3 -u scripts/observer_scaling.py --skip-initial-nonce-query 2>&1 \
  | tee /tmp/run_observer_16v_8obs.log &
EXP_JOB=$!

WAIT_MAX_S="${WAIT_MAX_S:-3600}"
elapsed=0
while ! grep -q "Run: python3 scripts/monitor.py" /tmp/run_observer_16v_8obs.log 2>/dev/null; do
  if ! kill -0 "$EXP_JOB" 2>/dev/null; then
    echo "Experiment exited before monitor line" >&2
    wait "$EXP_JOB" || true
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
  if [ "$elapsed" -ge "$WAIT_MAX_S" ]; then
    echo "Timeout waiting for Run: python3 scripts/monitor.py" >&2
    exit 1
  fi
done

echo "$(date -Is) Gated: starting monitor" | tee -a /tmp/monitor_observer_16v_8obs_stdout.log
python3 scripts/monitor.py --shards 16 \
  --output results/single_node_scaling/hdd/monitor_observer_16v_8obs.json \
  --wait 2>&1 | tee -a /tmp/monitor_observer_16v_8obs_stdout.log &
MON_JOB=$!

wait "$EXP_JOB" || true
exp_rc=$?
wait "$MON_JOB" || true
mon_rc=$?
echo "=== Done: experiment exit=$exp_rc monitor exit=$mon_rc ==="
exit "$exp_rc"
