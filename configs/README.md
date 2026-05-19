# Configuration layout (`configs/`)

This directory documents how **per-shard-count** NEAR genesis and node
configuration should look for the single-machine Chameleon scaling
experiments. The JSON files themselves are generated on the experiment host
under `~/bench/` (they are large, run-specific, and not checked into Git).

## What belongs here

For each shard count **N** you run, you need a matching pair:

| File | Purpose |
|------|---------|
| `genesis.json` | Chain genesis: validators, shard layout, **producer seat counts**, gas limit |
| `config.json` | Per-node `neard` settings (RPC/P2P ports, boot nodes, RocksDB paths) |

On the machine, use a dedicated directory per **N** so runs do not overwrite
each other:

```text
~/bench/N4/genesis.json
~/bench/N4/config.json
~/bench/N8/genesis.json
~/bench/N8/config.json
   …
```

(`N4` means four shards, `N8` means eight, and so on.)

The harness (`scripts/run_benchmark.py`) can also use a shared template under
`~/bench/genesis.json` and `~/bench/config.json` when you copy or symlink
from `~/bench/N{N}/` before launching.

---

## Critical parameter: `num_block_producer_seats_per_shard`

For an **N-shard** experiment with **exactly N validator processes** on one
host (one validator per shard), set:

```text
num_block_producer_seats_per_shard = [1, 1, …, 1]   # length N
num_chunk_producer_seats_per_shard = [1, 1, …, 1]   # length N
num_block_producer_seats = N
```

**Do not** leave the cloud-cluster default **`[100, 100, …, 100]`** (length N).

With only **N** validators but **100 seats per shard**, the epoch manager
treats most seats as unoccupied or assigns producers in ways that do not match
your single-machine layout. Symptomatically you see **near-zero TPS**, stuck
heights, or validators that never produce the shard you are load-testing.

---

## Sample `genesis.json` fragments

### N = 4

```json
"num_block_producer_seats": 4,
"num_block_producer_seats_per_shard": [1, 1, 1, 1],
"num_chunk_producer_seats_per_shard": [1, 1, 1, 1],
```

### N = 8

```json
"num_block_producer_seats": 8,
"num_block_producer_seats_per_shard": [1, 1, 1, 1, 1, 1, 1, 1],
"num_chunk_producer_seats_per_shard": [1, 1, 1, 1, 1, 1, 1, 1],
```

### Gas limit (all N)

Per-shard gas limit used in our campaign:

```json
"gas_limit": 30000000000000
```

That is **30 Tgas** per shard (30 × 10¹²).

---

## Account baking with `scripts/genesis_bake.py`

The helper bakes sub-accounts into genesis and synth-bm user-data trees
(offline, no RPC account creation on slow HDD).

From the repository root:

```bash
python3 scripts/genesis_bake.py --num-shards N --num-accounts 5000 \
  --bench-dir ~/bench/N{N} \
  --users-data-dir ~/bench/N{N}/user-data
```

Example for **N = 4**:

```bash
python3 scripts/genesis_bake.py --num-shards 4 --num-accounts 5000 \
  --bench-dir ~/bench/N4 --users-data-dir ~/bench/N4/user-data
```

It updates genesis under the bench tree (typically
`~/bench/N4/node0/genesis.json` after node dirs exist). **Merge or verify** the
seat fields above into that genesis file before starting `neard`.

---

## Other runtime hygiene

- **CPU governor:** `echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`
- **RocksDB `LOCK`:** delete stale `LOCK` files under each node's `data/` directory before every `neard` restart after a crash.
- **N ≥ 8:** re-query RPC after the first epoch so chunk-producer ↔ shard assignment matches monitoring (see root `README.md`).

See the root [README.md](../README.md) for full methodology and results.
