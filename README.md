# NEAR Sharding Benchmark (Chameleon / single-node)

Empirical evaluation of **NEAR Protocol Nightshade** sharding using the official
**one-million-TPS** harness adapted for **Chameleon Cloud** bare metal — CS
work with **Prof. Ioan Raicu**, **DataSys Lab**, **Illinois Institute of
Technology**.

This repository contains:

- **Git submodules:** pinned `nearcore` (for `neard`) and the upstream **1M
  TPS harness** (`near/one-million-tps`, checked out under directory
  `near-benchmark/`).
- **Patches** for Chameleon (no GCP, genesis baking, CPU pinning, netem,
  correct producer seats, RocksDB lock cleanup).
- **Scripts:** single-node shard scaling runner, monitor, analyzers, genesis
  helper.
- **Canonical results:** per-shard Prometheus JSON from HDD and tmpfs sweeps.

> **Note on submodule naming:** The harness upstream is
> [`near/one-million-tps`](https://github.com/near/one-million-tps). It is
> imported as `near-benchmark/` for clarity. If your course materials refer to
> `nearone/near-benchmark`, substitute the appropriate remote in `.gitmodules`
> after cloning — the **pin commit** for this project is
> `7cb8029002e51b507acbb22a804ad61709fffeb7`.

---

## Hardware (reference: `pet-squid`, Chameleon)

| Resource | Specification |
|----------|----------------|
| CPU | Intel Xeon, **48 HT cores** |
| RAM | **128 GB** |
| Disk | HDD ~**80–100 MB/s** sequential |
| Network | **10 GbE** |
| `neard` revision | **`d178e1830b062b407c270e8f8045753fd41cd081`** (`nearcore` submodule) |

**Protocol / gas:** Follow `nearcore` release metadata at the pin above (bench
configs use **30 Tgas** gas limit in our campaign).

---

## Clone

```bash
git clone --recurse-submodules https://github.com/ssahukar2/near-sharding-benchmark.git
cd near-sharding-benchmark
```

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

### Submodule pins (verify)

```bash
git submodule status
# near-benchmark → 7cb8029002e51b507acbb22a804ad61709fffeb7
# nearcore       → d178e1830b062b407c270e8f8045753fd41cd081
```

---

## Build `neard`

```bash
cd nearcore
rustup show   # rustc from rustup toolchain file
cargo build -p neard --release
# binary: nearcore/target/release/neard
```

The Python harness expects the binary at:

`./nearcore/target/release/neard` (relative to this repo — **no symlink to
`$HOME/nearcore` required**).

---

## Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Apply Chameleon patches

From the repo root — this invokes `patches/apply_patches.sh`:

```bash
chmod +x patches/apply_patches.sh
./patches/apply_patches.sh
```

Or manually:

**Nearcore** (only `bench.sh.patch` is non-empty on the pinned tree; others
no-op if empty):

```bash
cd nearcore
git checkout d178e1830b062b407c270e8f8045753fd41cd081
git apply ../patches/nearcore/bench.sh.patch
```

**Harness** (`near-benchmark/`), **in order**:

```bash
cd near-benchmark
git checkout 7cb8029002e51b507acbb22a804ad61709fffeb7
for p in ../patches/near-benchmark/0*.patch; do git apply "$p"; done
```

See `patches/CHAMELEON_CHANGES.md` for how the **nine** documented Chameleon
themes map onto the **four** patch files, and `patches/README.md` for a file
index.

---

## Critical configuration notes

1. **`num_block_producer_seats_per_shard`** must be **`[1] * N`** for an
   **N-shard** single-machine experiment — **not** the default `[100] * N`**
   layout (that targets cloud clusters with many logical producers per shard).
2. **CPU governor:** `echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`
   — `schedutil` caused large regressions in our sweeps.
3. **RocksDB `LOCK`:** delete stale `LOCK` files under each node's DB path
   before every `neard` restart after an unclean shutdown.
4. **Shard assignment at N ≥ 8:** the epoch manager may reshuffle
   chunk-producer ↔ shard mapping after the first epoch — the harness
   **re-queries RPC** to realign monitoring / tx submission with the post-epoch
   assignment (see `scripts/run_benchmark.py`).

Hosting `config.json` / `genesis.json`: typically `$HOME/bench/` on the
experiment machine (`configs/README.md`).

---

## Run benchmarks (single node)

From repo root, after `neard` is built and bench configs exist under
`~/bench/`:

```bash
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Example: N=4, HDD
python3 scripts/run_benchmark.py --shards 4 --storage hdd --keep-logs --duration 180

# Example: N=4, tmpfs (/dev/shm)
python3 scripts/run_benchmark.py --shards 4 --storage tmpfs --keep-logs --duration 180
```

**Monitoring** (second terminal — wait until the harness prints the `Run:
python3 scripts/monitor.py …` line):

```bash
python3 scripts/monitor.py --shards 4 \
  --output results/single_node_scaling/hdd/monitor_S04.json --wait
```

**Offline analysis** (per monitor file):

```bash
python3 scripts/analyze_block_pipeline.py \
  --input results/single_node_scaling/hdd/monitor_S04.json
```

**Genesis / account baking** (offline helper, also copied into `nearcore` by
`apply_patches.sh`):

```bash
python3 scripts/genesis_bake.py --help
```

**Observer scaling** (16 validators + 8 observers + external submitters):

```bash
python3 scripts/observer_scaling.py --help
bash orchestration/run_observer_16v_8obs_gated.sh
```

**Multi-node Phase 3** (validator grid + RPS sweeps — paths are repo-relative):

```bash
bash orchestration/phase3_verify_and_benchmark.sh --validators orchestration/validators.txt
```

---

## Results (canonical)

Committed JSON under `results/single_node_scaling/` (all **< 50 MB** in this
bundle — largest file ~5.8 MB).

### HDD

| N | Aggregate TPS | TPS/shard | Block time | BFT finality |
|--:|--------------:|----------:|-----------:|-------------:|
| 1 | 1,070 | 1,070 | 1,019 ms | 6.8 s |
| 2 | 1,238 | 619 | 1,368 ms | 22.9 s |
| 4 | 1,272 | 318 | 1,500 ms | 31.5 s |
| 8 | **1,498** | 187 | 2,120 ms | 77.5 s |
| 16 | 1,204 | 75 | 2,284 ms | 64.0 s |
| 24 | 1,110 | 46 | 2,605 ms | 50.2 s |

### tmpfs (RAM-backed RocksDB path)

| N | Aggregate TPS | vs HDD |
|--:|--------------:|:------:|
| 1 | 925 | −14 % |
| 2 | 650 | −47 % |
| 4 | 1,440 | +12 % |
| 8 | 1,656 | +25 % |
| 16 | **0 (stall)** | — |

At **N=16 tmpfs** the chain halted with **~29×** orphan-witness rate vs
N=8 tmpfs; CPU **~47 %** — not a resource exhaustion failure but a **timing /
coherence** failure. **HDD throughput unintentionally paced** chunk
production to the witness gossip pipeline.

---

## Five headline findings

1. **Sub-linear scaling:** peak aggregate TPS only ~**40 %** above single-shard;
   per-shard TPS falls **~23×** from N=1 → N=24.
2. **Bottleneck migration:** at low **N**, **L3 / trie working-set** pressure
   dominates apply; at mid **N**, **witness gossip + partial-chunk pipeline**
   dominates; at high **N** on fast storage, **protocol coherence** breaks.
3. **Storage is not the primary ceiling** on HDD: **tmpfs** does not recover
   most of the gap; at some **N** it **destabilizes** the run.
4. **BFT / endorsement traffic** grows super-linearly and stretches **block
   interval** and **finality latency** even when raw apply is no longer the
   long pole.
5. **Chameleon / academic hardware** is viable for **reproducible baselines**
   that cloud-only numbers don't provide — at the cost of instrumenting CPU
   governors, seats, and DB hygiene carefully.

---

## Layout

```
near-sharding-benchmark/
├── nearcore/                 # submodule @ d178e1830…
├── near-benchmark/           # submodule @ 7cb8029… (one-million-tps)
├── patches/                  # nearcore + harness unified diffs + apply_patches.sh
├── scripts/                  # run_benchmark.py, monitor.py, analyzers, …
├── orchestration/            # Phase 3, gated monitor helpers, RPS sweep shells
├── benchmark/                # 4-shard sweep plotting + genesis JSON snippets
├── configs/README.md
├── results/                  # CSV + single_node_scaling/{hdd,tmpfs} monitors
├── poster/                   # Fig. sources (SVG / Python generators)
└── plots/                    # RPS sweep figures
```

---

## License / attribution

Benchmark harness and `nearcore` retain their upstream licenses. Patches and
scripts in this repository are provided for **academic reproducibility**
(IIT / DataSys).
