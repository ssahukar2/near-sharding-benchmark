# Nine Chameleon changes ↔ four patch files

The harness port is delivered as **four file-level unified diffs** (easier to
review than nine overlapping commits). Below is how the nine bullets from the
paper / report map onto those files.

| # | Theme | Primary patch | Notes |
|---|--------|---------------|-------|
| 1 | Genesis account baking (avoid RPC HDD timeouts) | `01_sharded_bm.patch`, `02_local_test_node.patch` | Accounts materialized in genesis; reduced hot-path RPC. |
| 2 | Python 3.10 / 3.11 compatibility | `01_sharded_bm.patch`, `02_local_test_node.patch` | Typing / `asyncio` / `utcnow` fixes. |
| 3 | GCP API / GCS removal | `01_sharded_bm.patch`, `03_mirror.patch` | No bucket uploads; local-only artifacts. |
| 4 | RPC timeout tuning | `01_sharded_bm.patch`, `04_node_handle.patch` | Longer waits for slow disks. |
| 5 | CPU pinning (`numactl`) | `patches/nearcore/bench.sh.patch` | Launcher wraps `neard` under `numactl`. |
| 6 | `tc netem` hooks | `patches/nearcore/bench.sh.patch` | Optional injected delay for WAN studies. |
| 7 | Measurement window parameterization | `01_sharded_bm.patch` | Ramp-up + steady window via env / CLI. |
| 8 | `num_block_producer_seats_per_shard = [1]*N` | `01_sharded_bm.patch` | **Critical** — default `[100]*N` breaks single-machine scaling semantics. |
| 9 | RocksDB `LOCK` cleanup before restart | `01_sharded_bm.patch`, `02_local_test_node.patch` | Prevents stale lock after crash / kill. |

To regenerate these patches from a machine that has both clean and modified trees:

```bash
cd near-benchmark
BASE=7cb8029002e51b507acbb22a804ad61709fffeb7
git diff "$BASE" -- scripts/mocknet/sharded_bm.py       > 01_sharded_bm.patch
git diff "$BASE" -- scripts/mocknet/local_test_node.py > 02_local_test_node.patch
git diff "$BASE" -- scripts/mocknet/mirror.py          > 03_mirror.patch
git diff "$BASE" -- scripts/mocknet/node_handle.py     > 04_node_handle.patch
```
