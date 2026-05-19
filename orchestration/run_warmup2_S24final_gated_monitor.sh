#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "=== Step 1: N=8 warmup ==="
set +e
python3 -u scripts/run_benchmark.py --shards 8 --storage hdd 2>&1 | tee /tmp/run_S08_warmup2.log
WARM_RC=${PIPESTATUS[0]}
set -e
if [ "$WARM_RC" -ne 0 ]; then
  echo "Warmup failed: exit $WARM_RC" >&2
  exit "$WARM_RC"
fi

echo "=== Step 2: N=24 ==="
python3 -u scripts/run_benchmark.py --shards 24 --storage hdd 2>&1 | tee /tmp/run_S24_final.log &
BENCH_JOB=$!

WAIT_MAX_S="${WAIT_MAX_S:-3600}"
elapsed=0
while ! grep -q "Run: python3 scripts/monitor.py" /tmp/run_S24_final.log 2>/dev/null; do
  if ! kill -0 "$BENCH_JOB" 2>/dev/null; then
    echo "N=24 exited before monitor line" >&2
    wait "$BENCH_JOB" || true
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
  if [ "$elapsed" -ge "$WAIT_MAX_S" ]; then
    echo "Timeout waiting for Run: python3 scripts/monitor.py" >&2
    exit 1
  fi
done

echo "=== Step 3: monitor (gated) ==="
python3 scripts/monitor.py --shards 24 \
  --output results/single_node_scaling/hdd/monitor_S24.json \
  --wait 2>&1 | tee /tmp/monitor_S24_final_stdout.log &
MON_JOB=$!

wait "$BENCH_JOB" || true
bench_rc=$?
wait "$MON_JOB" || true
mon_rc=$?
echo "=== Done: benchmark exit=$bench_rc monitor exit=$mon_rc ==="
exit "$bench_rc"
