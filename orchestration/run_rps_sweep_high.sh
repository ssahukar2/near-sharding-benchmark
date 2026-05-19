#!/usr/bin/env bash
# High-RPS extension sweep (per-shard RPS). Appends to ~/sweep_results.csv.
# Stops early if approx_tps < 15000 or init/create-accounts/start-nodes/wait_for_rpc fails.
set -u

# Auto-detect RPC port when not already set.
if [ -z "${RPC_ADDR:-}" ]; then
    if curl -s http://localhost:3030/status &>/dev/null; then
        export RPC_ADDR="127.0.0.1:3030"
    elif curl -s http://localhost:4040/status &>/dev/null; then
        export RPC_ADDR="127.0.0.1:4040"
    else
        echo "ERROR: neard not responding on 3030 or 4040 — is neard running?"
        exit 1
    fi
    echo "Auto-detected RPC_ADDR=$RPC_ADDR"
fi

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
RESULT_CSV="${HOME}/sweep_results.csv"
PARSE_PY="/home/ubuntu/near-benchmark-toolkit/scripts/parse_bench_run.py"

RPSS=(12000 15000 20000 25000 30000)
TOTAL=${#RPSS[@]}

cd "${BM}" || exit 1

if [[ ! -f "${RESULT_CSV}" ]]; then
  echo "RPS,approx_tps,sum_shard_tps,total_received,wall_seconds,errors" > "${RESULT_CSV}"
fi

for i in "${!RPSS[@]}"; do
  RPS=${RPSS[$i]}
  RUN=$((i + 1))
  if [ "${i}" -lt $((TOTAL - 1)) ]; then
    NEXT_RPS=${RPSS[$((i + 1))]}
  else
    NEXT_RPS=""
  fi

  echo ""
  echo "========== Extension run ${RUN}/${TOTAL} | RPS (per shard) ${RPS} =========="

  jq --argjson r "${RPS}" '.requests_per_second = $r' "${PARAMS}" > /tmp/params.$$.json
  mv /tmp/params.$$.json "${PARAMS}"

  echo "[Ext ${RUN}/${TOTAL} | RPS ${RPS}] → init..."
  ./bench.sh init "${CASE}" || { echo "init failed — aborting"; exit 1; }

  echo "[Ext ${RUN}/${TOTAL} | RPS ${RPS}] → create-accounts..."
  ./bench.sh create-accounts "${CASE}" || { echo "create-accounts failed — aborting"; exit 1; }

  echo "[Ext ${RUN}/${TOTAL} | RPS ${RPS}] → start-nodes..."
  ./bench.sh start-nodes "${CASE}" || { echo "start-nodes failed — aborting"; exit 1; }

  wait_for_rpc "http://${RPC_ADDR}" || { echo "wait_for_rpc failed — aborting"; exit 1; }

  rm -f "${HOME}/iostat_${RPS}.txt"
  iostat -x 1 120 > "${HOME}/iostat_${RPS}.txt" &
  IOP=$!

  echo "[Ext ${RUN}/${TOTAL} | RPS ${RPS}] → native-transfers..."
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
  STALL_FILE=$(mktemp)
  python3 "${PARSE_PY}" "${LOG_DIR}" > "${METF}" 2>/dev/null || echo "{}" > "${METF}"

  export SWEEP_RPS="${RPS}"
  export SWEEP_WALL="${WALL}"
  export SWEEP_ERRS="${ERRS}"
  export SWEEP_RESULT_CSV="${RESULT_CSV}"
  export SWEEP_METF="${METF}"
  export SWEEP_STALL_FILE="${STALL_FILE}"
  export SWEEP_RUN="${RUN}"
  export SWEEP_TOTAL="${TOTAL}"
  export SWEEP_NEXT_RPS="${NEXT_RPS}"
  python3 <<'PY'
import ast, csv, os

result_csv = os.environ["SWEEP_RESULT_CSV"]
metf = os.environ["SWEEP_METF"]
stall_file = os.environ["SWEEP_STALL_FILE"]
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
    f"✓ Run [{run}/{total}] complete — RPS: {rps} | approx_tps: {approx} | sum_shard_tps: {sum_s} | time: {wall}s"
)
if next_rps:
    print(f"Next: starting RPS {next_rps}...")

stall = False
try:
    if approx != "" and float(approx) < 15000:
        stall = True
except (TypeError, ValueError):
    pass
with open(stall_file, "w") as f:
    f.write("1" if stall else "0")
PY
  STALL=$(cat "${STALL_FILE}")
  rm -f "${METF}" "${STALL_FILE}"

  if [[ "${STALL}" == "1" ]]; then
    echo "Stopping early: approx_tps < 15000 (severe degradation threshold)."
    break
  fi

done

echo ""
echo "Extension sweep finished. Appended to ${RESULT_CSV}"
