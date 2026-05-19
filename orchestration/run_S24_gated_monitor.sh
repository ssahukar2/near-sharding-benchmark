#!/usr/bin/env bash
# N=24 HDD: start benchmark, then monitor only after harness prints the
# Run: python3 scripts/monitor.py line.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

python3 -u scripts/run_benchmark.py --shards 24 --storage hdd 2>&1 | tee /tmp/run_S24.log &
BENCH_JOB=$!

WAIT_MAX_S="${WAIT_MAX_S:-3600}"
elapsed=0
while ! grep -q "Run: python3 scripts/monitor.py" /tmp/run_S24.log 2>/dev/null; do
  if ! kill -0 "$BENCH_JOB" 2>/dev/null; then
    echo "ERROR: benchmark exited before monitor instruction appeared" >&2
    tail -80 /tmp/run_S24.log >&2 || true
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
  if [ "$elapsed" -ge "$WAIT_MAX_S" ]; then
    echo "TIMEOUT (${WAIT_MAX_S}s) waiting for Run: scripts/monitor.py line" >&2
    exit 1
  fi
done

echo "$(date -Is) Gated: starting monitor" | tee -a /tmp/monitor_S24_stdout.log
python3 scripts/monitor.py --shards 24 \
  --output results/single_node_scaling/hdd/monitor_S24.json \
  --wait 2>&1 | tee -a /tmp/monitor_S24_stdout.log &
MON_JOB=$!

wait "$BENCH_JOB" || true
bench_rc=$?
wait "$MON_JOB" || true
mon_rc=$?
echo "$(date -Is) benchmark exit=$bench_rc monitor exit=$mon_rc"
exit "$bench_rc"
