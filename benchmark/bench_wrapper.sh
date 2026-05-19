#!/usr/bin/env bash
set -euo pipefail
BM_DIR="${BM_DIR:-$HOME/nearcore/benchmarks/sharded-bm}"
CASE="${CASE:-cases/local/4_cp_1_rpc_4_shard}"
cd "$BM_DIR"
./bench.sh init "$CASE"
./bench.sh create-accounts "$CASE"
./bench.sh start-nodes "$CASE"
./bench.sh native-transfers "$CASE"
./bench.sh stop-nodes "$CASE"
