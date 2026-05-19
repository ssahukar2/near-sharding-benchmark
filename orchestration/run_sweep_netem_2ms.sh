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

ROOT="$(cd "$(dirname "$0")" && pwd)"
sudo tc qdisc del dev lo root 2>/dev/null || true
sudo tc qdisc add dev lo root netem delay 2ms
tc qdisc show dev lo
export NETEM_MS=2
export RPSS="5000 7500"
"$ROOT/run_rps_sweep_netem.sh"
sudo tc qdisc del dev lo root
