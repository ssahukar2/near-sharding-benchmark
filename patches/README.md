# Index: Chameleon patches

## `nearcore/` submodule (pinned: `d178e1830b062b407c270e8f8045753fd41cd081`)

| File | Applies to | Notes |
|------|------------|--------|
| `bench.sh.patch` | `benchmarks/sharded-bm/bench.sh` | **Non-empty** — `numactl` / `taskset` CPU pinning, netem hooks, measurement window env vars. |
| `node_handle.py.patch` | `pytest/tests/mocknet/node_handle.py` | Empty on reference tree — skipped by `apply_patches.sh`. |
| `sharded_bm.py.patch` | `pytest/tests/mocknet/sharded_bm.py` | Empty — skipped. |
| `mirror.py.patch` | `pytest/tests/mocknet/mirror.py` | Empty — skipped. |
| `node_config.py.patch` | `pytest/tests/mocknet/node_config.py` | Empty — skipped. |
| `local_test_node.py.patch` | `pytest/tests/mocknet/local_test_node.py` | Empty — skipped. |

After patching nearcore, copy `scripts/genesis_bake.py` (bundled copy of
`genesis_create_accounts.py`) into
`nearcore/benchmarks/sharded-bm/genesis_create_accounts.py` if your workflow
uses the sharded-bm genesis helper (see root `README.md`).

## `near-benchmark/` submodule (pinned: `7cb8029002e51b507acbb22a804ad61709fffeb7`)

These four unified diffs were generated **from the pet-squid working tree**
against the **clean** submodule tip (`7cb8029`).

| Patch | File | Maps to Chameleon themes (see `CHAMELEON_CHANGES.md`) |
|-------|------|------------------------------------------------------|
| `01_sharded_bm.patch` | `scripts/mocknet/sharded_bm.py` | Genesis baking, seat layout, ramp/window, GCP removal, LOCK cleanup, py3.10+ compat, timeouts — **largest change**. |
| `02_local_test_node.patch` | `scripts/mocknet/local_test_node.py` | Account/genesis path, RocksDB teardown, local node lifecycle. |
| `03_mirror.patch` | `scripts/mocknet/mirror.py` | GCS / GCP path stripping, local-only asset paths. |
| `04_node_handle.patch` | `scripts/mocknet/node_handle.py` | Small RPC / timeout tuning. |

Apply **in numeric order**:

```bash
cd near-benchmark
git checkout 7cb8029002e51b507acbb22a804ad61709fffeb7
for p in ../patches/near-benchmark/0*.patch; do git apply "$p"; done
```
