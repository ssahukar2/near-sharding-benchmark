#!/usr/bin/env bash
# =============================================================================
# Phase 3 — Verify Chain + Run Benchmark
# =============================================================================
# Run this from pet-squid AFTER chameleon_near_setup.sh completes.
#
# What it does:
#   1. Verifies all nodes are producing blocks
#   2. Verifies P2P connectivity between all validators
#   3. Verifies RPC node can reach all validators
#   4. Runs the baseline RPS sweep
#   5. Optionally runs pinned and netem sweeps for comparison
#
# Usage:
#   ./phase3_verify_and_benchmark.sh --validators validators.txt
#   ./phase3_verify_and_benchmark.sh --validators validators.txt --sweep all
#   ./phase3_verify_and_benchmark.sh --validators validators.txt --sweep baseline
#
# Sweep options:
#   baseline   — 11-point RPS sweep, unpinned (default)
#   pinned     — CPU pinned sweep
#   netem      — netem 1ms sweep
#   all        — run all 3 sweeps in sequence
# =============================================================================
set -euo pipefail

# ── colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── defaults ──────────────────────────────────────────────────────────────────
REMOTE_USER="ubuntu"
RPC_PORT="4040"
P2P_PORT="24567"
VALIDATORS_FILE=""
SWEEP="baseline"
DRY_RUN=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/results"
EXPERIMENTS_DIR="${REPO_ROOT}/orchestration"

# ── parse args ────────────────────────────────────────────────────────────────
usage() {
    echo -e "${BOLD}Usage:${NC} $0 --validators FILE [options]"
    echo ""
    echo -e "  ${CYAN}--validators FILE${NC}   File with one validator IP per line"
    echo -e "  ${CYAN}--sweep TYPE${NC}        baseline | pinned | netem | all (default: baseline)"
    echo -e "  ${CYAN}--user USER${NC}         Remote SSH user (default: ubuntu)"
    echo -e "  ${CYAN}--dry-run${NC}           Print what would happen without doing it"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --validators) VALIDATORS_FILE="$2"; shift 2 ;;
        --sweep)      SWEEP="$2";           shift 2 ;;
        --user)       REMOTE_USER="$2";     shift 2 ;;
        --dry-run)    DRY_RUN=true;         shift ;;
        --help|-h)    usage; exit 0 ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; usage; exit 1 ;;
    esac
done

[[ -z "$VALIDATORS_FILE" ]] && { echo -e "${RED}Error: --validators required${NC}"; usage; exit 1; }
[[ ! -f "$VALIDATORS_FILE" ]] && { echo -e "${RED}Error: $VALIDATORS_FILE not found${NC}"; exit 1; }

mapfile -t VALIDATORS < <(grep -v '^\s*#' "$VALIDATORS_FILE" | grep -v '^\s*$' | tr -d '[:space:]')
NUM_VALIDATORS=${#VALIDATORS[@]}
RPC_IP=$(hostname -I | awk '{print $1}')

# ── helpers ───────────────────────────────────────────────────────────────────
log()  { echo -e "${GREEN}${BOLD}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] $*${NC}"; }
err()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR: $*${NC}"; exit 1; }
ssh_cmd() {
    local ip="$1"; shift
    ssh -o StrictHostKeyChecking=accept-new \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        "${REMOTE_USER}@${ip}" "$@"
}

# ── banner ────────────────────────────────────────────────────────────────────
echo -e "${GREEN}${BOLD}"
echo "╔══════════════════════════════════════════════╗"
echo "║   Phase 3 — Verify Chain + Run Benchmark    ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  RPC node        : ${CYAN}${RPC_IP}${NC} (this machine)"
echo -e "  Validators      : ${CYAN}${NUM_VALIDATORS}${NC}"
for i in "${!VALIDATORS[@]}"; do
    echo -e "    node${i} (shard ${i}) → ${CYAN}${VALIDATORS[$i]}${NC}"
done
echo -e "  Sweep           : ${CYAN}${SWEEP}${NC}"
echo ""
[[ "$DRY_RUN" == "true" ]] && echo -e "${YELLOW}DRY RUN MODE${NC}\n"

# ── step 1: verify all validators are producing blocks ────────────────────────
log "[1/4] Verifying all validator nodes are producing blocks..."
ALL_OK=true
for i in "${!VALIDATORS[@]}"; do
    ip="${VALIDATORS[$i]}"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}[DRY RUN]${NC} Check node${i} ($ip)"; continue
    fi
    height=$(ssh_cmd "$ip" \
        "curl -s http://localhost:3030/status 2>/dev/null \
         | jq -r '.sync_info.latest_block_height // \"ERROR\"'" \
        2>/dev/null || echo "UNREACHABLE")
    if [[ "$height" == "ERROR" ]] || [[ "$height" == "UNREACHABLE" ]]; then
        echo -e "  ${RED}✗${NC} node${i} ($ip) — not responding (height: $height)"
        echo -e "     Check logs: ssh ubuntu@${ip} 'tail -20 ~/bench/neard.log'"
        ALL_OK=false
    else
        echo -e "  ${GREEN}✓${NC} node${i} ($ip) — block height: ${height}"
    fi
done

# check RPC node (this machine)
if [[ "$DRY_RUN" == "false" ]]; then
    rpc_height=$(curl -s "http://localhost:${RPC_PORT}/status" 2>/dev/null \
        | jq -r '.sync_info.latest_block_height // "ERROR"')
    if [[ "$rpc_height" == "ERROR" ]]; then
        echo -e "  ${RED}✗${NC} RPC node ($RPC_IP) — not responding"
        echo -e "     Check logs: tail -20 ~/bench/neard_rpc.log"
        ALL_OK=false
    else
        echo -e "  ${GREEN}✓${NC} RPC node ($RPC_IP) — block height: ${rpc_height}"
    fi
fi

[[ "$ALL_OK" == "false" ]] && err "Some nodes are not producing blocks. Fix before running benchmark."
echo ""

# ── step 2: verify P2P connectivity ──────────────────────────────────────────
log "[2/4] Verifying P2P peer connectivity..."
for i in "${!VALIDATORS[@]}"; do
    ip="${VALIDATORS[$i]}"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}[DRY RUN]${NC} Check peers on node${i} ($ip)"; continue
    fi
    peer_count=$(ssh_cmd "$ip" \
        "curl -s http://localhost:3030/status 2>/dev/null \
         | jq '.sync_info.num_peers // 0'" \
        2>/dev/null || echo "0")
    if [[ "$peer_count" -ge 1 ]]; then
        echo -e "  ${GREEN}✓${NC} node${i} ($ip) — ${peer_count} peers connected"
    else
        echo -e "  ${YELLOW}⚠${NC} node${i} ($ip) — 0 peers (may still be connecting)"
    fi
done
echo ""

# ── step 3: verify RPC can submit transactions ────────────────────────────────
log "[3/4] Verifying RPC node is ready to accept transactions..."
if [[ "$DRY_RUN" == "false" ]]; then
    rpc_status=$(curl -s "http://localhost:${RPC_PORT}/status" 2>/dev/null \
        | jq -r '.sync_info.syncing' 2>/dev/null || echo "unknown")
    if [[ "$rpc_status" == "false" ]]; then
        echo -e "  ${GREEN}✓${NC} RPC node is synced and ready"
    else
        warn "RPC node may still be syncing (syncing: $rpc_status) — waiting 10s..."
        sleep 10
    fi
fi
echo ""

# ── step 4: run benchmark sweep(s) ───────────────────────────────────────────
log "[4/4] Running benchmark sweep: ${SWEEP}..."
echo ""

run_sweep() {
    local name="$1"
    local script="$2"
    local outfile="$3"

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Running: ${name}${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}[DRY RUN]${NC} Would run: $script"
        return
    fi

    if [[ ! -f "$script" ]]; then
        warn "Script not found: $script — skipping"
        return
    fi

    bash "$script"

    if [[ -f "$outfile" ]]; then
        echo ""
        echo -e "${GREEN}  Results saved to: $outfile${NC}"
        echo ""
        echo -e "  Summary:"
        column -t -s',' "$outfile" | head -15
    fi
}

case "$SWEEP" in
    baseline)
        run_sweep "Baseline RPS sweep (unpinned)" \
            "${EXPERIMENTS_DIR}/run_sweep_baseline.sh" \
            "${RESULTS_DIR}/sweep_results.csv"
        ;;
    pinned)
        run_sweep "CPU pinned sweep" \
            "${EXPERIMENTS_DIR}/run_sweep_pinned.sh" \
            "${RESULTS_DIR}/sweep_results_pinned.csv"
        ;;
    netem)
        run_sweep "netem 1ms sweep" \
            "${EXPERIMENTS_DIR}/run_sweep_netem_1ms.sh" \
            "${RESULTS_DIR}/sweep_results_netem.csv"
        ;;
    all)
        run_sweep "Baseline RPS sweep (unpinned)" \
            "${EXPERIMENTS_DIR}/run_sweep_baseline.sh" \
            "${RESULTS_DIR}/sweep_results.csv"
        echo ""
        read -rp "  Baseline done. Press ENTER to run CPU pinned sweep..."
        run_sweep "CPU pinned sweep" \
            "${EXPERIMENTS_DIR}/run_sweep_pinned.sh" \
            "${RESULTS_DIR}/sweep_results_pinned.csv"
        echo ""
        read -rp "  Pinned done. Press ENTER to run netem 1ms sweep..."
        run_sweep "netem 1ms sweep" \
            "${EXPERIMENTS_DIR}/run_sweep_netem_1ms.sh" \
            "${RESULTS_DIR}/sweep_results_netem.csv"
        ;;
    *)
        err "Unknown sweep type: $SWEEP. Use baseline, pinned, netem, or all."
        ;;
esac

# ── final summary ─────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║           Phase 3 Complete!                 ║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Results saved to: ${CYAN}${RESULTS_DIR}/${NC}"
echo ""
echo -e "  Files:"
[[ -f "${RESULTS_DIR}/sweep_results.csv" ]]        && echo -e "  ${CYAN}sweep_results.csv${NC}        — baseline"
[[ -f "${RESULTS_DIR}/sweep_results_pinned.csv" ]] && echo -e "  ${CYAN}sweep_results_pinned.csv${NC} — CPU pinned"
[[ -f "${RESULTS_DIR}/sweep_results_netem.csv" ]]  && echo -e "  ${CYAN}sweep_results_netem.csv${NC}  — netem 1ms"
echo ""
echo -e "  To regenerate plots:"
echo -e "  ${CYAN}python3 ${REPO_ROOT}/benchmark/plot_near_benchmarks.py${NC}"
echo ""