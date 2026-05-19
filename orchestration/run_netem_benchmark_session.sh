#!/usr/bin/env bash
# Apply lo netem, run 1ms sweep then 2ms partial sweep, cleanup. Logs tc state before benchmarks.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
CH="$(cd "$ROOT/.." && pwd)"
LOG="${CH}/logs/sweep_netem_run.log"
RESULT_CSV="${CH}/results/sweep_results_netem.csv"
SWEEP="${ROOT}/run_rps_sweep_netem.sh"

exec > >(tee -a "${LOG}") 2>&1

echo ""
echo "############################################"
echo "# $(date -Is) Netem benchmark session start"
echo "############################################"

echo ""
echo "=== tc qdisc show dev lo (initial check) ==="
tc qdisc show dev lo || true

echo ""
echo "=== Remove existing lo root qdisc (if any) ==="
sudo tc qdisc del dev lo root 2>/dev/null || true

echo ""
echo "=== Add 1ms delay to lo ==="
sudo tc qdisc add dev lo root netem delay 1ms

echo ""
echo "=== VERIFY1ms — tc qdisc show dev lo (before1ms benchmark runs) ==="
tc qdisc show dev lo

echo "netem_ms,RPS,approx_tps,sum_shard_tps,total_received,wall_seconds,errors" > "${RESULT_CSV}"

export RESULT_CSV
export NETEM_MS=1
export RPSS="3000 5000 7500 15000 25000"
"${SWEEP}"

echo ""
echo "=== Remove 1ms qdisc ==="
sudo tc qdisc del dev lo root

echo ""
echo "=== Add 2ms delay to lo ==="
sudo tc qdisc add dev lo root netem delay 2ms

echo ""
echo "=== VERIFY 2ms — tc qdisc show dev lo (before 2ms benchmark runs) ==="
tc qdisc show dev lo

export NETEM_MS=2
export RPSS="5000 7500"
"${SWEEP}"

echo ""
echo "=== Cleanup: remove netem from lo ==="
sudo tc qdisc del dev lo root

echo ""
echo "=== tc qdisc show dev lo (after cleanup) ==="
tc qdisc show dev lo || true

echo ""
echo "# $(date -Is) Netem benchmark session finished"
echo "Results: ${RESULT_CSV}"
echo "Log: ${LOG}"
