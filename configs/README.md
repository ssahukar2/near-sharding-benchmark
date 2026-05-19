# Config templates

Runtime NEAR configs are **not** committed here: they live next to your data
directory (e.g. `$HOME/bench/config.json`, `$HOME/bench/genesis.json`) and are
generated or copied during the harness run.

After building genesis with the bundled helper:

```bash
python3 scripts/genesis_bake.py --help   # see nearcore benchmark utility args
```

**Critical**

- `num_block_producer_seats_per_shard` must be **`[1] * N`** for an N-shard
  single-machine experiment (not `[100, 100, …]`).
- Set CPU governor: `echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`
- Delete stale `LOCK` files under each node's RocksDB dir before restarting `neard`.

See root `README.md` for full methodology.
