#!/usr/bin/env bash
# One-shot verification: fixed RPC wait, metrics sample, user-data layout, 3000 RPS native-transfers.
set -euo pipefail

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
PARSE_PY="/home/ubuntu/near-benchmark-toolkit/scripts/parse_bench_run.py"
RPC_BASE="http://127.0.0.1:4040"

cd "${BM}"

echo "========== Clean run: init + create-accounts + start-nodes =========="
./bench.sh stop-nodes "${CASE}" 2>/dev/null || true
pkill -9 neard 2>/dev/null || true
sleep 1

./bench.sh init "${CASE}"
./bench.sh create-accounts "${CASE}"

jq --argjson r 3000 '.requests_per_second = $r' "${PARAMS}" > /tmp/p.$$.json
mv /tmp/p.$$.json "${PARAMS}"
echo "params requests_per_second (per shard): $(jq -r .requests_per_second "${PARAMS}")"

echo ""
echo "[start-nodes]"
./bench.sh start-nodes "${CASE}"

echo ""
echo "========== RPC readiness =========="
wait_for_rpc "${RPC_BASE}"

echo ""
echo "========== Metrics (transaction / chunk / tps) sample =========="
( curl -s "${RPC_BASE}/metrics" | grep -iE 'transaction|chunk|tps' | head -20 ) || true

echo ""
echo "========== user-data: first 3 account IDs per shard (from filenames) =========="
for s in 0 1; do
  echo "--- shard${s} ---"
  n=0
  for f in "${BM}/user-data/shard${s}/"*.json; do
    [[ -f "$f" ]] || continue
    b=$(basename "$f")
    echo "${b%.json}"
    n=$((n + 1))
    [[ "$n" -eq 3 ]] && break
  done
done

echo ""
echo "========== bench.sh: each shard i uses user-data/shard\${i} (see native_transfers_local) =========="
sed -n '646,657p' "${BM}/bench.sh"

echo ""
echo "========== native-transfers @ 3000 RPS (per shard), all 4 shards =========="
set +e
./bench.sh native-transfers "${CASE}"
NT=$?
set -e
echo "native-transfers exit code: ${NT} (bench.sh may be non-zero even on successful workload)"

./bench.sh stop-nodes "${CASE}" 2>/dev/null || true

echo ""
echo "========== Raw log: gen_shard0 (shard0 synth-bm stdout/stderr) =========="
cat "${LOG_DIR}/gen_shard0"

echo ""
echo "========== Parsed aggregate metrics =========="
python3 "${PARSE_PY}" "${LOG_DIR}"
