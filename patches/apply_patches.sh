#!/usr/bin/env bash
set -euo pipefail
# Apply Chameleon patches to the two git submodules checked out in this repo.
#
# Usage: ./apply_patches.sh [NEARCORE_DIR] [NEAR_BENCHMARK_DIR]
#   Defaults: <repo-root>/nearcore   and   <repo-root>/near-benchmark

PATCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PATCH_DIR/.." && pwd)"
NEARCORE_DIR="${1:-${NEARCORE_DIR:-$REPO_ROOT/nearcore}}"
HARNESS_DIR="${2:-${NEAR_BENCHMARK_DIR:-$REPO_ROOT/near-benchmark}}"
GENESIS_SRC="${REPO_ROOT}/scripts/genesis_bake.py"
GENESIS_DST="${NEARCORE_DIR}/benchmarks/sharded-bm/genesis_create_accounts.py"
NC_PATCH="${PATCH_DIR}/nearcore"
HB_PATCH="${PATCH_DIR}/near-benchmark"

apply_patch_to_repo() {
  local repo="$1"
  local patch_file="$2"
  local name
  name="$(basename "$patch_file")"
  if [[ ! -s "$patch_file" ]]; then
    echo "OK: skip empty patch ${name}"
    return 0
  fi
  echo "Applying ${name} -> ${repo}..."
  git -C "$repo" apply "$patch_file"
  echo "OK: applied ${name}"
}

if [[ ! -d "${NEARCORE_DIR}/.git" ]]; then
  echo "ERROR: not a git repo: ${NEARCORE_DIR}"
  exit 1
fi
if [[ ! -d "${HARNESS_DIR}/.git" ]]; then
  echo "ERROR: not a git repo: ${HARNESS_DIR}"
  exit 1
fi

echo "=== nearcore: ${NEARCORE_DIR} ==="
for p in bench.sh.patch node_handle.py.patch sharded_bm.py.patch mirror.py.patch \
         node_config.py.patch local_test_node.py.patch; do
  apply_patch_to_repo "$NEARCORE_DIR" "${NC_PATCH}/$p"
done

if [[ ! -f "$GENESIS_SRC" ]]; then
  echo "ERROR: missing ${GENESIS_SRC}"
  exit 1
fi
echo "Copying genesis helper -> ${GENESIS_DST}"
cp -a "$GENESIS_SRC" "$GENESIS_DST"
echo "OK: genesis_create_accounts.py installed (from scripts/genesis_bake.py)"

echo "=== near-benchmark (one-million-tps): ${HARNESS_DIR} ==="
shopt -s nullglob
for p in "${HB_PATCH}"/0*.patch; do
  apply_patch_to_repo "$HARNESS_DIR" "$p"
done
shopt -u nullglob

echo "All steps completed."
echo "  NEARCORE_DIR=${NEARCORE_DIR}"
echo "  HARNESS_DIR=${HARNESS_DIR}"
