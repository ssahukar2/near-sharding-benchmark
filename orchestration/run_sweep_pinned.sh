#!/usr/bin/env bash
# CPU-pinned neard sweep (taskset). Writes ~/sweep_results_pinned.csv (fresh each run).
set -u

# Auto-detect RPC port (single-node 4040 vs multi-node 3030).
if curl -s http://localhost:3030/status &>/dev/null; then
    export RPC_ADDR="127.0.0.1:3030"
elif curl -s http://localhost:4040/status &>/dev/null; then
    export RPC_ADDR="127.0.0.1:4040"
else
    echo "ERROR: neard not responding on 3030 or 4040 — is neard running?"
    exit 1
fi
echo "Auto-detected RPC_ADDR=$RPC_ADDR"

wait_for_rpc() {
    local url=$1
    local max_wait=600
    local elapsed=0
    echo "Waiting for RPC at $url..."
    while true; do
        code="000"
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url/status" 2>/dev/null) || true
        if [[ "$code" == "200" ]]; then
            echo "RPC ready after ${elapsed}s"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        if [[ $elapsed -ge $max_wait ]]; then
            echo "ERROR: RPC not ready after ${max_wait}s"
            return 1
        fi
    done
}

CASE="cases/local/4_cp_1_rpc_4_shard"
BM="/home/ubuntu/nearcore/benchmarks/sharded-bm"
PARAMS="${BM}/${CASE}/params.json"
export SYNTH_BM_BIN="/home/ubuntu/nearcore/benchmarks/synth-bm/target/release/near-synth-bm"
export LOG_DIR="${BM}/logs"
export BENCHNET_DIR="${BENCHNET_DIR:-/home/ubuntu/bench}"
# node0..3 = shard validators; node4 = RPC — pin to disjoint core sets (48 CPUs total).
export NEARD_TASKSET_MASKS="0-11 12-23 24-35 36-43 44-47"

RESULT_CSV="${HOME}/sweep_results_pinned.csv"
PARSE_PY="/home/ubuntu/near-benchmark-toolkit/scripts/parse_bench_run.py"

RPSS=(1000 3000 5000 7500 15000 25000)
TOTAL=${#RPSS[@]}

cd "${BM}" || exit 1

echo "RPS,approx_tps,sum_shard_tps,total_received,wall_seconds,errors" > "${RESULT_CSV}"

for i in "${!RPSS[@]}"; do
  RPS=${RPSS[$i]}
  RUN=$((i + 1))
  if [ "${i}" -lt $((TOTAL - 1)) ]; then
    NEXT_RPS=${RPSS[$((i + 1))]}
  else
    NEXT_RPS=""
  fi

  echo ""
  echo "========== Pinned run ${RUN}/${TOTAL} | RPS (per shard) ${RPS} =========="

  jq --argjson r "${RPS}" '.requests_per_second = $r' "${PARAMS}" > /tmp/params.$$.json
  mv /tmp/params.$$.json "${PARAMS}"

  echo "[Pinned ${RUN}/${TOTAL} | RPS ${RPS}] → init..."
  ./bench.sh init "${CASE}" || { echo "init failed — aborting"; exit 1; }

  echo "[Pinned ${RUN}/${TOTAL} | RPS ${RPS}] → create-accounts..."
  ./bench.sh create-accounts "${CASE}" || { echo "create-accounts failed — aborting"; exit 1; }

  echo "[Pinned ${RUN}/${TOTAL} | RPS ${RPS}] → start-nodes (taskset via NEARD_TASKSET_MASKS)..."
  ./bench.sh start-nodes "${CASE}" || { echo "start-nodes failed — aborting"; exit 1; }

  wait_for_rpc "http://${RPC_ADDR}" || { echo "wait_for_rpc failed — aborting"; exit 1; }

  rm -f "${HOME}/iostat_pinned_${RPS}.txt"
  iostat -x 1 120 > "${HOME}/iostat_pinned_${RPS}.txt" &
  IOP=$!

  echo "[Pinned ${RUN}/${TOTAL} | RPS ${RPS}] → native-transfers..."
  ERRF=$(mktemp)
  t0=$(date +%s)
  set +e
  ./bench.sh native-transfers "${CASE}" 2>"${ERRF}"
  NT_RC=$?
  set -e
  t1=$(date +%s)
  WALL=$((t1 - t0))

  kill "${IOP}" 2>/dev/null || true
  wait "${IOP}" 2>/dev/null || true

  LOGERR=""
  if [ -s "${ERRF}" ]; then
    LOGERR=$(head -c 2000 "${ERRF}" | tr '\n' ' ' | tr ',' ';')
  fi
  rm -f "${ERRF}"

  SHERR=""
  if ls "${LOG_DIR}"/gen_shard* >/dev/null 2>&1; then
    SHERR=$( (grep -hEi 'error|timeout|panic|invalid|fail' "${LOG_DIR}"/gen_shard* 2>/dev/null | head -n 20) | tr '\n' ' ' | tr ',' ';' || true)
  fi

  ERRS=""
  if [ "${NT_RC}" -ne 0 ]; then
    ERRS="native-transfers_exit_${NT_RC}"
  fi
  if [ -n "${LOGERR}" ]; then
    ERRS="${ERRS};stderr:${LOGERR}"
  fi
  if [ -n "${SHERR}" ]; then
    ERRS="${ERRS};logs:${SHERR}"
  fi
  ERRS=$(echo "${ERRS}" | head -c 3500)

  ./bench.sh stop-nodes "${CASE}" || true
  pkill -9 near-synth-bm 2>/dev/null || true
  sleep 2

  METF=$(mktemp)
  python3 "${PARSE_PY}" "${LOG_DIR}" > "${METF}" 2>/dev/null || echo "{}" > "${METF}"

  export SWEEP_RPS="${RPS}"
  export SWEEP_WALL="${WALL}"
  export SWEEP_ERRS="${ERRS}"
  export SWEEP_RESULT_CSV="${RESULT_CSV}"
  export SWEEP_METF="${METF}"
  export SWEEP_RUN="${RUN}"
  export SWEEP_TOTAL="${TOTAL}"
  export SWEEP_NEXT_RPS="${NEXT_RPS}"
  python3 <<'PY'
import ast, csv, os

result_csv = os.environ["SWEEP_RESULT_CSV"]
metf = os.environ["SWEEP_METF"]
rps = int(os.environ["SWEEP_RPS"])
wall = int(os.environ["SWEEP_WALL"])
errs = os.environ.get("SWEEP_ERRS", "")
run = int(os.environ["SWEEP_RUN"])
total = int(os.environ["SWEEP_TOTAL"])
next_rps = os.environ.get("SWEEP_NEXT_RPS", "").strip()

with open(metf, encoding="utf-8") as f:
    s = f.read().strip()
try:
    d = ast.literal_eval(s) if s else {}
except Exception:
    d = {}
approx = d.get("approx_tps", "")
sum_s = d.get("sum_shard_tps", "")
tot = d.get("total_received", "")
with open(result_csv, "a", newline="") as f:
    csv.writer(f).writerow([rps, approx, sum_s, tot, wall, errs])

print(
    f"\u2713 Run [{run}/{total}] complete — RPS: {rps} | approx_tps: {approx} | sum_shard_tps: {sum_s} | time: {wall}s"
)
if next_rps:
    print(f"Next: starting RPS {next_rps}...")
PY
  rm -f "${METF}"

done

echo ""
echo "Pinned sweep finished. Results: ${RESULT_CSV}"
