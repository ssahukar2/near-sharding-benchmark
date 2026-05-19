#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
"$ROOT/run_sweep_baseline.sh"
"$ROOT/run_sweep_pinned.sh"
"$ROOT/run_sweep_netem_1ms.sh"
"$ROOT/run_sweep_netem_2ms.sh"
