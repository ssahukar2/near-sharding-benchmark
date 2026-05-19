#!/usr/bin/env bash
set -euo pipefail

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

# Full baseline sweep = base 6-point + high extension 5-point appended.
ROOT="$(cd "$(dirname "$0")" && pwd)"
"$ROOT/run_rps_sweep.sh"
"$ROOT/run_rps_sweep_high.sh"
