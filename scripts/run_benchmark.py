#!/usr/bin/env python3
"""Single-machine NEAR shard scaling experiment runner.

This script is intentionally self-contained and follows a conservative setup strategy:
- It only depends on Python stdlib + requests + system tools + neard binary.
- It treats unknown/ambiguous environment differences as warnings or hard failures,
  rather than guessing hidden defaults.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import requests

# Repository root (parent of scripts/) — results and monitors are written here so
# clones work regardless of where the repo lives under $HOME.
REPO_ROOT = Path(__file__).resolve().parent.parent

ALLOWED_SHARDS = {1, 2, 4, 8, 16, 24}
MAX_SHARDS = 24
HTTP_TIMEOUT_S = 10
READINESS_TIMEOUT_S = 900  # Default; readiness_gate uses a shard-scaled timeout (see readiness_gate).
READINESS_COND6_COMPARE_WINDOW_S = 60
READINESS_POLL_INTERVAL_S = 10
READINESS_SLOW_WARNING_S = 120
STABILIZATION_S = 30
# After neard restart (accounts_path correction), allow full readiness window.
READINESS_RESTART_TIMEOUT_S = 1800
# Measurement retries for transient post-readiness stalls.
MAX_MEASUREMENT_RETRIES = 2
MEASUREMENT_RETRY_WAIT_S = 30
# Poll validators RPC until each node has a chunk-producer shard or we time out.
ASSIGNMENT_VERIFY_DEADLINE_S = 30
ASSIGNMENT_POLL_INTERVAL_S = 2
# Finality lag samples during measure_run (instance 0 / port 3030).
FINALITY_SAMPLE_INTERVAL_S = 30


@dataclass(frozen=True)
class Paths:
    """Holds all filesystem locations used by the experiment."""

    home: Path
    neard_bin: Path
    config_template: Path
    genesis_template: Path
    accounts_base: Path
    run_base: Path
    results_dir: Path
    monitor_out: Path


@dataclass
class ProcessState:
    """Tracks spawned processes to guarantee cleanup on success/failure/interrupt."""

    neard_processes: list[subprocess.Popen]


class ExperimentError(RuntimeError):
    """Raised for deterministic experiment failures."""


def now_hms() -> str:
    """Returns local wall-clock for human-friendly phase logging."""
    return datetime.now().strftime("%H:%M:%S")


def utc_iso() -> str:
    """Returns canonical UTC timestamp for result records."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def phase(message: str) -> None:
    """Prints a timestamped phase header to make long runs readable."""
    print(f"[{now_hms()}] PHASE: {message}", flush=True)


def info(message: str) -> None:
    """Prints a timestamped informational line."""
    print(f"[{now_hms()}] {message}", flush=True)


def run_cmd(cmd: list[str], *, cwd: Path | None = None, dry_run: bool = False) -> subprocess.CompletedProcess:
    """Runs a command with strict error checks and debuggable stderr.

    We avoid shell=True to reduce accidental quoting bugs.
    """
    rendered = " ".join(cmd)
    if dry_run:
        info(f"DRY-RUN cmd: {rendered} (cwd={cwd or Path.cwd()})")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    cp = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)
    if cp.returncode != 0:
        raise ExperimentError(
            f"Command failed ({cp.returncode}): {rendered}\n"
            f"stdout:\n{cp.stdout}\n"
            f"stderr:\n{cp.stderr}"
        )
    return cp


def safe_pkill_neard(*, dry_run: bool = False) -> None:
    """Kills all neard processes before each run.

    pkill returns 1 when no matches exist; that is not a failure for our workflow.
    """
    cmd = ["pkill", "-9", "neard"]
    if dry_run:
        info("DRY-RUN cmd: pkill -9 neard")
        return
    cp = subprocess.run(cmd, text=True, capture_output=True)
    if cp.returncode not in (0, 1):
        raise ExperimentError(f"pkill failed ({cp.returncode}): {cp.stderr}")


def read_json(path: Path) -> dict[str, Any]:
    """Reads JSON object from disk with explicit UTF-8."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    """Writes pretty JSON deterministically for easier debugging."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=False)


def preflight_checks(paths: Paths, dry_run: bool) -> None:
    """Validates required external dependencies before any long-running phases.

    Runs in both normal and dry-run mode so dry-run still catches missing tools.
    """
    phase("Preflight checks")
    failures: list[str] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        status = "PASS" if ok else "FAIL"
        suffix = f" - {detail}" if detail else ""
        info(f"[{status}] {name}{suffix}")
        if not ok:
            failures.append(f"{name}: {detail}".strip(": "))

    record("neard binary exists", paths.neard_bin.exists(), str(paths.neard_bin))
    record("neard binary executable", os.access(paths.neard_bin, os.X_OK), str(paths.neard_bin))

    try:
        cp = subprocess.run([str(paths.neard_bin), "--version"], text=True, capture_output=True)
        record("neard --version", cp.returncode == 0, cp.stdout.strip() or cp.stderr.strip())
    except Exception as e:  # pragma: no cover - defensive
        record("neard --version", False, str(e))

    for tool in ("numactl", "pkill"):
        record(f"tool available: {tool}", shutil.which(tool) is not None, shutil.which(tool) or "not found")

    try:
        import requests as _requests  # noqa: F401
        record("python requests import", True)
    except Exception as e:  # pragma: no cover - defensive
        record("python requests import", False, str(e))

    for name, path in (("config.json", paths.config_template), ("genesis.json", paths.genesis_template)):
        exists = path.exists()
        record(f"{name} exists", exists, str(path))
        if exists:
            try:
                _ = read_json(path)
                record(f"{name} valid JSON", True)
            except Exception as e:
                record(f"{name} valid JSON", False, str(e))

    if failures:
        raise ExperimentError("Preflight checks failed:\n- " + "\n- ".join(failures))


def ensure_accounts(
    *,
    paths: Paths,
    n_shards: int,
    accounts_per_shard: int,
    dry_run: bool,
) -> None:
    """Verifies account key files exist for shards used in this run.

    Accounts are pre-generated once. This is a fast existence check only.
    If files are missing, fail fast with a clear error rather than
    silently generating partial data.
    """
    phase(f"Verifying account key files for shards 0..{n_shards - 1}")
    missing: list[str] = []
    for shard in range(n_shards):
        shard_dir = paths.accounts_base / f"shard{shard}"
        if dry_run:
            info(f"DRY-RUN check {shard_dir} ({accounts_per_shard} accounts)")
            continue
        if not shard_dir.exists():
            missing.append(str(shard_dir))
            continue
        # Spot check first and last account file only — fast, sufficient signal
        for idx in (0, accounts_per_shard - 1):
            account_id = f"a{shard:02d}_user_{idx}.node0"
            fp = shard_dir / f"{account_id}.json"
            if not fp.is_file():
                missing.append(str(fp))
    if missing:
        raise ExperimentError(
            "Missing account files — regenerate with ensure_accounts standalone script.\n"
            + "\n".join(f"  {p}" for p in missing)
        )
    if not dry_run:
        info(f"Account files verified for {n_shards} shard(s)")


def ensure_validator_keys(
    *,
    paths: Paths,
    n_shards: int,
    run_dir: Path,
    skip_setup: bool,
    dry_run: bool,
) -> dict[int, dict[str, str]]:
    """Ensures per-instance validator/node keys and returns public key map.

    WHY: Validator public keys are required to build genesis records and boot_nodes.
    """
    phase(f"Ensuring validator keys for N={n_shards}")
    out: dict[int, dict[str, str]] = {}

    for i in range(n_shards):
        node_home = run_dir / f"node{i}"
        if dry_run:
            info(f"DRY-RUN mkdir -p {node_home}")
        else:
            node_home.mkdir(parents=True, exist_ok=True)

        vkey_path = node_home / "validator_key.json"
        nkey_path = node_home / "node_key.json"

        if skip_setup and vkey_path.exists() and nkey_path.exists():
            vkey = read_json(vkey_path)
        else:
            run_cmd(
                [
                    str(paths.neard_bin),
                    "--home",
                    str(node_home),
                    "init",
                    "--account-id",
                    f"node{i}",
                    "--chain-id",
                    "localnet",
                ],
                dry_run=dry_run,
            )
            if dry_run:
                vkey = {"public_key": f"DRYRUN_PUBLIC_KEY_{i}"}
            else:
                vkey = read_json(vkey_path)

        out[i] = {"account_id": f"node{i}", "public_key": vkey["public_key"]}

    return out


def build_genesis(
    *,
    paths: Paths,
    n_shards: int,
    accounts_per_shard: int,
    validator_keys: dict[int, dict[str, str]],
    dry_run: bool = False,
) -> dict[str, Any]:
    """Builds full genesis object from scratch for this run.

    WHY: The spec requires deterministic shard layout and record construction,
    while preserving unspecified config fields from the reference template.
    """
    reference = read_json(paths.genesis_template)
    # The local genesis template is a flat dict (no nested `config` key),
    # so we deep-copy the full object and override only required fields.
    genesis = json.loads(json.dumps(reference))

    if n_shards == 1:
        shard_layout: dict[str, Any] = {"V0": {"num_shards": 1, "version": 0}}
    else:
        boundaries = [f"a{i:02d}" for i in range(1, n_shards)]
        shard_layout = {
            "V1": {
                "boundary_accounts": boundaries,
                "shards_split_map": None,
                "to_parent_shard_map": None,
                "version": 1,
            }
        }

    genesis["chain_id"] = "localnet"
    genesis["genesis_time"] = utc_iso()
    genesis["genesis_height"] = 0
    genesis["num_block_producer_seats"] = n_shards
    # One chunk-producer seat per shard so each shard has a single dedicated CP (no height
    # rotation within a large per-shard set). With N validators and N shards, assignment
    # gives one validator per shard; mempool txs on that shard are included when that
    # validator produces (required when disable_tx_routing=true).
    genesis["num_block_producer_seats_per_shard"] = [1] * n_shards
    genesis["num_chunk_producer_seats"] = n_shards
    genesis["minimum_validators_per_shard"] = 1
    genesis["shuffle_shard_assignment_for_chunk_producers"] = False
    genesis["transaction_validity_period"] = 10000
    genesis["epoch_length"] = 500
    genesis["gas_limit"] = 30_000_000_000_000
    genesis["shard_layout"] = shard_layout

    stake = 50_000_000 * 10**24
    user_balance = 10**24
    records: list[dict[str, Any]] = []
    validators: list[dict[str, Any]] = []

    for i in range(n_shards):
        node_account = f"node{i}"
        vpk = validator_keys[i]["public_key"]
        validators.append(
            {
                "account_id": node_account,
                "public_key": vpk,
                "amount": str(stake),
            }
        )
        records.append(
            {
                "Account": {
                    "account_id": node_account,
                    "account": {
                        "amount": str(stake + 10**24),
                        "locked": str(stake),
                        "code_hash": "11111111111111111111111111111111",
                        "storage_usage": 0,
                        "version": "V1",
                    },
                }
            }
        )
        records.append(
            {
                "AccessKey": {
                    "account_id": node_account,
                    "public_key": vpk,
                    "access_key": {"nonce": 0, "permission": "FullAccess"},
                }
            }
        )

    for s in range(n_shards):
        shard_dir = paths.accounts_base / f"shard{s}"
        for idx in range(accounts_per_shard):
            account_id = f"a{s:02d}_user_{idx}.node0"
            account_file = shard_dir / f"{account_id}.json"
            if account_file.exists():
                account_json = read_json(account_file)
                public_key = account_json["public_key"]
            elif dry_run:
                # In dry-run mode we may not have generated files on disk; use a placeholder
                # public key so the planning flow can continue without side effects.
                public_key = f"DRYRUN_ACCOUNT_PUBLIC_KEY_{s}_{idx}"
            else:
                raise ExperimentError(f"Missing account file required for genesis: {account_file}")
            records.append(
                {
                    "Account": {
                        "account_id": account_id,
                        "account": {
                            "amount": str(user_balance),
                            "locked": "0",
                            "code_hash": "11111111111111111111111111111111",
                            "storage_usage": 0,
                            "version": "V1",
                        },
                    }
                }
            )
            records.append(
                {
                    "AccessKey": {
                        "account_id": account_id,
                        "public_key": public_key,
                        "access_key": {"nonce": 0, "permission": "FullAccess"},
                    }
                }
            )

    # Genesis validation sums amount + locked for every Account record; a closed form
    # that only used "amount" missed validator stake locked in "locked".
    total_supply = 0
    for record in records:
        acct_wrapped = record.get("Account")
        if not acct_wrapped:
            continue
        bal = acct_wrapped["account"]
        total_supply += int(bal["amount"])
        total_supply += int(bal.get("locked", "0"))

    genesis["validators"] = validators
    genesis["records"] = records
    genesis["total_supply"] = str(total_supply)
    return genesis


def write_genesis_and_node_homes(
    *,
    run_dir: Path,
    genesis: dict[str, Any],
    n_shards: int,
    dry_run: bool,
) -> Path:
    """Writes shared genesis and copies it into each node home.

    WHY: Each node home is self-contained for easy inspection/debugging.
    """
    phase("Writing run genesis and distributing to node homes")
    shared_path = run_dir / "genesis.json"
    if dry_run:
        info(f"DRY-RUN write {shared_path}")
    else:
        write_json(shared_path, genesis)
        size_mb = shared_path.stat().st_size / (1024 * 1024)
        info(f"Genesis size: {size_mb:.2f} MB")

    for i in range(n_shards):
        node_home = run_dir / f"node{i}"
        node_home.mkdir(parents=True, exist_ok=True)
        dst = node_home / "genesis.json"
        if dry_run:
            info(f"DRY-RUN copy {shared_path} -> {dst}")
        else:
            shutil.copy2(shared_path, dst)

    return shared_path


def _set_if_exists_or_create(cfg: dict[str, Any], key: str, value: Any) -> None:
    """Writes a top-level config key.

    We update in place if present, otherwise create it because different neard
    builds sometimes ship slightly different config templates.
    """
    cfg[key] = value


def generate_node_configs(
    *,
    paths: Paths,
    run_dir: Path,
    n_shards: int,
    validator_keys: dict[int, dict[str, str]],
    dry_run: bool,
) -> None:
    """Generates per-instance config.json files with required overrides.

    WHY: Ports, boot-nodes, and tx_generator account paths are instance-specific.
    """
    phase("Generating per-node config.json files")
    base = read_json(paths.config_template)

    # Precompute the lexicographic CP shard assignment so accounts_path is set
    # correctly from the start, eliminating the post-launch config fix + restart.
    # NEAR assigns chunk producers by sorting validator account IDs lexicographically;
    # the validator at sorted index j becomes CP for shard j.
    # e.g. N=16: "node0"→0, "node1"→1, "node10"→2, "node11"→3, …, "node9"→15
    _sorted_validators = sorted(f"node{j}" for j in range(n_shards))
    _cp_shard_for_node = {name: idx for idx, name in enumerate(_sorted_validators)}

    for i in range(n_shards):
        cp_shard = _cp_shard_for_node[f"node{i}"]
        cfg = json.loads(json.dumps(base))
        node_home = run_dir / f"node{i}"

        cfg.setdefault("rpc", {})
        cfg["rpc"]["addr"] = f"0.0.0.0:{3030 + i}"

        cfg.setdefault("network", {})
        cfg["network"]["addr"] = f"0.0.0.0:{24567 + i}"
        cfg["network"]["skip_sync_wait"] = True

        boot_nodes = []
        for j in range(n_shards):
            if j == i:
                continue
            boot_nodes.append(f"{validator_keys[j]['public_key']}@127.0.0.1:{24567 + j}")
        cfg["network"]["boot_nodes"] = ",".join(boot_nodes)

        # Set to 2s (vs GCP benchmark's 200ms) because at 1 physical core on
        # Haswell, the chunk producer needs ~2s to select and validate enough
        # transactions to fill a chunk. At 200ms it would produce near-empty
        # chunks, artificially suppressing TPS below hardware capability.
        # This makes absolute TPS not directly comparable to GCP results,
        # but preserves relative comparisons across shard counts in this experiment.
        _set_if_exists_or_create(cfg, "produce_chunk_add_transactions_time_limit", {"secs": 2, "nanos": 0})
        _set_if_exists_or_create(cfg, "transaction_pool_size_limit", 100_000_000)
        _set_if_exists_or_create(cfg, "save_tx_outcomes", False)
        _set_if_exists_or_create(cfg, "save_state_changes", False)
        _set_if_exists_or_create(cfg, "disable_tx_routing", True)

        # Do not override min/max_block_production_delay: template values keep min <= max;
        # neard validate-config rejects min > max (e.g. 600ms min vs 500ms template max).
        if isinstance(cfg.get("consensus"), dict):
            cfg["consensus"]["doomslug_step_period"] = {"secs": 0, "nanos": 10_000_000}
        # Memory-tries toggle belongs under `store` in this config shape.
        if isinstance(cfg.get("store"), dict):
            cfg["store"]["load_mem_tries_for_tracked_shards"] = True

        # `~/bench/config.json` sets tx_generator to null; schedule must still match
        # near_transactions_generator::Config: each entry is Load { tps, duration_s }.
        # See nearcore/benchmarks/sharded-bm/cases/forknet/*/tx-generator-settings.json.
        cfg["tx_generator"] = {
            "accounts_path": str(paths.accounts_base / f"shard{cp_shard}"),
            "receiver_accounts_path": None,
            "receivers_from_senders_ratio": 0.0,
            "sender_accounts_zipf_skew": 0.0,
            "receiver_accounts_zipf_skew": 0.0,
            "schedule": [
                {"tps": 1, "duration_s": 120},
                {"tps": 10_000, "duration_s": 3480},
            ],
            "controller": {
                "target_block_production_time_s": 1.5,
                "bps_filter_window_length": 6,
                "gain_proportional": 300.0,
                "gain_integral": 0,
                "gain_derivative": 0.0,
                "block_pause_threshold_ms": 120000,
            },
        }

        node_home.mkdir(parents=True, exist_ok=True)
        user_data_link = node_home / "user-data"
        target_shard_dir = paths.accounts_base / f"shard{cp_shard}"
        if dry_run:
            info(
                f"DRY-RUN node{i} ports: rpc=0.0.0.0:{3030 + i} p2p=0.0.0.0:{24567 + i} "
                f"boot_nodes_count={len(boot_nodes)} boot_nodes={cfg['network']['boot_nodes']}"
            )
            info(f"DRY-RUN symlink {user_data_link} -> {target_shard_dir}")
            info(f"DRY-RUN write {node_home / 'config.json'}")
        else:
            if user_data_link.exists() or user_data_link.is_symlink():
                if user_data_link.is_dir() and not user_data_link.is_symlink():
                    shutil.rmtree(user_data_link)
                else:
                    user_data_link.unlink()
            os.symlink(target_shard_dir, user_data_link)
            write_json(node_home / "config.json", cfg)


def wipe_node_data_dirs(*, run_dir: Path, n_shards: int, dry_run: bool) -> None:
    """Wipes per-node RocksDB data directories for clean chain startup.

    Also removes stale node dirs from previous higher-N runs.
    """
    phase("Wiping node data directories")
    # Current run's data dirs
    current = {run_dir / f"node{i}" / "data" for i in range(n_shards)}
    # Stale dirs from previous runs with higher shard counts
    stale = {p for p in run_dir.glob("node*/data") if p not in current}
    if stale and not dry_run:
        info(f"Removing {len(stale)} stale node data dirs from previous runs")
    for data_dir in sorted(current | stale):
        if dry_run:
            info(f"DRY-RUN rm -rf {data_dir} && mkdir -p {data_dir}")
            continue
        if data_dir.exists():
            shutil.rmtree(data_dir)
        if data_dir in current:
            data_dir.mkdir(parents=True, exist_ok=True)


def metrics_chunk_tx_total(metrics_text: str) -> int:
    """Extracts near_chunk_transactions_total as specified.

    Rule: use lines starting with metric name and prefer non-zero series.
    """
    value = 0
    for line in metrics_text.splitlines():
        if not line.startswith("near_chunk_transactions_total{"):
            continue
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        try:
            candidate = int(float(parts[-1]))
        except ValueError:
            continue
        if candidate > 0:
            return candidate
        value = max(value, candidate)
    return value


def fetch_status(port: int) -> dict[str, Any] | None:
    """Fetches /status with strict timeout; returns None on transient failures."""
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/status", timeout=HTTP_TIMEOUT_S)
        if resp.status_code != 200:
            return None
        return resp.json()
    except requests.RequestException:
        return None


def fetch_metrics(port: int) -> str | None:
    """Fetches /metrics with strict timeout; returns None on transient failures."""
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/metrics", timeout=HTTP_TIMEOUT_S)
        if resp.status_code != 200:
            return None
        return resp.text
    except requests.RequestException:
        return None


def scrape_gauge(port: int, metric_name: str) -> float | None:
    """Scrape a single gauge value from /metrics endpoint.

    WHY: ``near_block_height_head`` and ``near_largest_final_height`` are
    simple gauge metrics (single float value per line) that we need
    to read alongside chunk transaction counts.
    """
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/metrics", timeout=HTTP_TIMEOUT_S)
        if resp.status_code != 200:
            info(f"WARNING: scrape_gauge {metric_name!r}: HTTP {resp.status_code}")
            return None
    except requests.RequestException as e:
        info(f"WARNING: scrape_gauge {metric_name!r}: request failed: {e}")
        return None
    prefix = f"{metric_name} "
    for line in resp.text.splitlines():
        if line.startswith(prefix):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return float(parts[-1])
                except ValueError:
                    return None
    return None


def scrape_processed_counters(port: int) -> tuple[int, int, int]:
    """Scrapes near_transaction_processed_{total,failed,successful} counters.

    Returns (total, failed, successful). Returns (0, 0, 0) on failure.
    """
    metrics = fetch_metrics(port)
    if not metrics:
        return (0, 0, 0)

    def extract(name: str) -> int:
        prefix = f"{name} "
        for line in metrics.splitlines():
            if line.startswith(prefix):
                try:
                    return int(float(line.split()[-1]))
                except (ValueError, IndexError):
                    return 0
        return 0

    total = extract("near_transaction_processed_total")
    failed = extract("near_transaction_processed_failed_total")
    successful = extract("near_transaction_processed_successfully_total")
    return (total, failed, successful)


def fetch_shard_snapshot(i: int, timeout: int = HTTP_TIMEOUT_S) -> dict[str, Any]:
    """Fetches status, metrics, and processed counters for one shard in parallel.

    Returns a dict with keys: shard, height, chunk_tx, processed.
    Uses a longer timeout when validators may be under heavy load.
    """
    port = 3030 + i
    # Use a separate session with custom timeout for this fetch
    height = 0
    chunk_tx = 0
    processed = (0, 0, 0)
    status_ok = False

    try:
        resp = requests.get(f"http://127.0.0.1:{port}/status", timeout=timeout)
        if resp.status_code == 200:
            st = resp.json()
            height = int(st["sync_info"].get("latest_block_height", 0))
            status_ok = True
    except requests.RequestException:
        pass

    try:
        resp = requests.get(f"http://127.0.0.1:{port}/metrics", timeout=timeout)
        if resp.status_code == 200:
            metrics_text = resp.text
            chunk_tx = metrics_chunk_tx_total(metrics_text)
            processed = _extract_processed_counters(metrics_text)
    except requests.RequestException:
        pass

    return {
        "shard": i,
        "height": height,
        "status_ok": status_ok,
        "chunk_tx": chunk_tx,
        "processed": processed,
    }


def _extract_processed_counters(metrics_text: str) -> tuple[int, int, int]:
    """Extracts near_transaction_processed counters from already-fetched metrics text."""

    def extract(name: str) -> int:
        prefix = f"{name} "
        for line in metrics_text.splitlines():
            if line.startswith(prefix):
                try:
                    return int(float(line.split()[-1]))
                except (ValueError, IndexError):
                    return 0
        return 0

    total = extract("near_transaction_processed_total")
    failed = extract("near_transaction_processed_failed_total")
    successful = extract("near_transaction_processed_successfully_total")
    return (total, failed, successful)


def _sample_finality_lag(port: int) -> float | None:
    """Returns ``near_block_height_head - near_largest_final_height`` or None."""
    head = scrape_gauge(port, "near_block_height_head")
    final_h = scrape_gauge(port, "near_largest_final_height")
    if head is None or final_h is None:
        return None
    return float(head - final_h)


def fetch_validators_epoch_info(port: int) -> dict[str, Any] | None:
    """Fetches JSON-RPC `validators` for EpochReference::Latest (chunk-producer shard sets)."""
    # JSON-RPC must use a params *object* for EpochReference::Latest, not `[{...}]`:
    # one-element arrays are parsed as Option<BlockId> (see nearcore chain/jsonrpc/src/api/validator.rs).
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": "validators",
        "method": "validators",
        "params": {"latest": None},
    }
    try:
        resp = requests.post(f"http://127.0.0.1:{port}/", json=payload, timeout=HTTP_TIMEOUT_S)
        if resp.status_code != 200:
            return None
        body = resp.json()
        if body.get("error"):
            return None
        result = body.get("result")
        return result if isinstance(result, dict) else None
    except (requests.RequestException, ValueError, TypeError):
        return None


def _coerce_shard_id_value(value: Any) -> int | None:
    """Parses shard id from JSON-RPC (int or decimal string)."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _cp_shard_for_account(epoch_info: dict[str, Any], account_id: str, n_shards: int) -> int | None:
    """Resolves chunk-producer shard id for `account_id` from validators RPC result.

    Prefers ``current_validators``; falls back to ``next_validators``. Shard ids are
    constrained to ``0..n_shards-1``.
    """

    def scan(validators_obj: Any) -> int | None:
        if not isinstance(validators_obj, list):
            return None
        entry = None
        for v in validators_obj:
            if isinstance(v, dict) and str(v.get("account_id")) == account_id:
                entry = v
                break
        if entry is None:
            return None
        shard_ids: list[int] = []
        for s in entry.get("shards") or []:
            sid = _coerce_shard_id_value(s)
            if sid is not None and 0 <= sid < n_shards:
                shard_ids.append(sid)
        if not shard_ids:
            return None
        if len(shard_ids) > 1:
            info(
                f"WARNING: {account_id}: multiple CP shard ids in RPC {sorted(shard_ids)}; "
                f"using {min(shard_ids)}"
            )
        return min(shard_ids)

    cur = scan(epoch_info.get("current_validators"))
    if cur is not None:
        return cur
    return scan(epoch_info.get("next_validators"))


def _try_build_chunk_producer_assignment(n_shards: int) -> dict[int, int]:
    """Single attempt: one validators RPC payload, map node index -> CP shard id."""
    epoch_info: dict[str, Any] | None = None
    for i in range(n_shards):
        ei = fetch_validators_epoch_info(3030 + i)
        if ei:
            epoch_info = ei
            break
    if not epoch_info:
        return {}
    out: dict[int, int] = {}
    for i in range(n_shards):
        sid = _cp_shard_for_account(epoch_info, f"node{i}", n_shards)
        if sid is not None:
            out[i] = sid
    return out


def _shard_ids_for_account_from_epoch(
    epoch_info: dict[str, Any], account_id: str, n_shards: int
) -> list[int]:
    """Shard id list for logging (current validators first, then next)."""
    for key in ("current_validators", "next_validators"):
        validators_obj = epoch_info.get(key)
        if not isinstance(validators_obj, list):
            continue
        for v in validators_obj:
            if isinstance(v, dict) and str(v.get("account_id")) == account_id:
                shard_ids: list[int] = []
                for s in v.get("shards") or []:
                    sid = _coerce_shard_id_value(s)
                    if sid is not None and 0 <= sid < n_shards:
                        shard_ids.append(sid)
                return sorted(set(shard_ids))
    return []


def verify_chunk_producer_shard_assignment(n_shards: int) -> dict[int, int]:
    """Maps node index -> chunk-producer shard id from ``validators`` JSON-RPC.

    Retries for up to ASSIGNMENT_VERIFY_DEADLINE_S. If some nodes stay unresolved,
    logs a warning and returns a partial map (callers may skip fixes for missing keys).

    /status only exposes ``validator_account_id``; CP shard sets come from ``validators`` RPC.
    """
    phase("Verifying validator id and chunk-producer shard assignment (informational)")
    deadline = time.time() + ASSIGNMENT_VERIFY_DEADLINE_S
    assignment: dict[int, int] = {}
    while time.time() < deadline:
        assignment = _try_build_chunk_producer_assignment(n_shards)
        if len(assignment) >= n_shards:
            break
        time.sleep(ASSIGNMENT_POLL_INTERVAL_S)

    if len(assignment) < n_shards:
        missing = [i for i in range(n_shards) if i not in assignment]
        info(
            f"WARNING: chunk-producer assignment incomplete after ~{ASSIGNMENT_VERIFY_DEADLINE_S}s; "
            f"missing node indices {missing}"
        )

    epoch_info: dict[str, Any] | None = None
    for i in range(n_shards):
        epoch_info = fetch_validators_epoch_info(3030 + i)
        if epoch_info:
            break

    for i in range(n_shards):
        port = 3030 + i
        expected = f"node{i}"
        st = fetch_status(port)
        if not st:
            info(f"WARNING: assignment check: no /status for instance {i} (port {port})")
            continue
        vid = st.get("validator_account_id")
        vid_s = str(vid) if vid is not None else None
        if vid_s != expected:
            info(
                f"WARNING: instance {i} validator_account_id={vid_s!r} "
                f"expected {expected!r}"
            )
        shard_ids: list[int] = []
        if epoch_info:
            shard_ids = _shard_ids_for_account_from_epoch(epoch_info, expected, n_shards)
        if i not in assignment:
            info(
                f"WARNING: node{i}: could not resolve CP shard from RPC "
                f"(shards={shard_ids if shard_ids else 'unknown'})"
            )
        elif i in shard_ids:
            info(f"node{i}: assigned shards={shard_ids} ✓")
        else:
            info(
                f"node{i}: assigned shards={shard_ids} "
                f"(CP shard {assignment[i]} per map; correcting accounts_path if needed)"
            )

    return assignment


def fix_accounts_path_for_assignment(
    *,
    paths: Paths,
    run_dir: Path,
    n_shards: int,
    assignment: dict[int, int],
    dry_run: bool,
) -> bool:
    """Sets ``tx_generator.accounts_path`` to ``shard{actual}`` when actual != node index.

    Returns True if any config was updated (or would be updated in dry-run).
    """
    any_changed = False
    for i in range(n_shards):
        actual = assignment.get(i)
        if actual is None or actual == i:
            continue
        cfg_path = run_dir / f"node{i}" / "config.json"
        correct_path = str(paths.accounts_base / f"shard{actual}")
        if dry_run:
            info(
                f"DRY-RUN would set node{i} tx_generator.accounts_path -> {correct_path} "
                f"(CP shard {actual} != node index {i})"
            )
            any_changed = True
            continue
        cfg = read_json(cfg_path)
        tg = cfg.get("tx_generator")
        if not isinstance(tg, dict):
            info(f"WARNING: node{i}: config has no tx_generator object; skipping accounts_path fix")
            continue
        prev = tg.get("accounts_path")
        if prev == correct_path:
            continue
        tg["accounts_path"] = correct_path
        write_json(cfg_path, cfg)
        info(
            f"Updated node{i} tx_generator.accounts_path: {prev!r} -> {correct_path!r} "
            f"(CP shard {actual})"
        )
        any_changed = True
    return any_changed


def wait_for_lock_release(run_dir: Path, n_shards: int,
                          *, timeout_s: int = 30) -> None:
    """Wait until RocksDB LOCK files are not held by any process.

    WHY: After SIGKILL, the kernel may not immediately release
    the flock on the RocksDB LOCK file. Launching a new neard
    before the LOCK is released causes an EAGAIN panic. This is
    especially common on HDD where file handle release is slower.
    """
    import fcntl
    import os as _os
    lock_paths = [
        run_dir / f"node{i}" / "data" / "LOCK"
        for i in range(n_shards)
    ]
    deadline = time.time() + timeout_s

    def lock_is_free(path: Path) -> bool:
        if not path.exists():
            return True
        try:
            fd = _os.open(str(path), _os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fd, fcntl.LOCK_UN)
                return True
            except BlockingIOError:
                return False
            finally:
                _os.close(fd)
        except OSError:
            return True

    while time.time() < deadline:
        if all(lock_is_free(p) for p in lock_paths):
            return
        time.sleep(1)

    info(f"WARNING: LOCK files not released within {timeout_s}s; "
         "deleting stale LOCK paths best-effort...")
    for lock_path in lock_paths:
        if lock_path.exists():
            try:
                lock_path.unlink()
                info(f"Deleted stale LOCK after timeout: {lock_path}")
            except OSError as e:
                info(f"WARNING: could not delete LOCK {lock_path}: {e}")


def wait_for_flat_storage_ready(n_shards: int, *, timeout_s: int = 300) -> None:
    """Waits until flat storage hops_to_head is at most 10 on all shards.

    WHY: After neard restart, flat_head_height starts at 0 while the chain
    is already at height N. Every state access requires N trie hops instead
    of flat storage reads, making block application extremely slow and causing
    Doomslug to deadlock at N=16. We must wait for flat storage to catch up
    before measuring.

    Metric: near_flat_storage_hops_to_head{shard_uid=...} — threshold 10
    allows normal lag while the chain produces blocks (~1-5 hops); 0 was
    too strict and never cleared during steady block production.
    """
    phase("Waiting for flat storage to catch up on all shards...")
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        all_ready = True
        max_hops = 0
        for i in range(n_shards):
            metrics = fetch_metrics(3030 + i)
            if not metrics:
                all_ready = False
                break
            # Sum hops across all shard_uid labels for this validator
            hops = 0
            for line in metrics.splitlines():
                if line.startswith("near_flat_storage_hops_to_head{"):
                    try:
                        val = float(line.split()[-1])
                        hops = max(hops, val)
                    except (ValueError, IndexError):
                        pass
            max_hops = max(max_hops, hops)
            if hops > 10:
                all_ready = False

        if all_ready:
            info("Flat storage ready on all shards (hops_to_head<=10).")
            return

        info(f"Flat storage still catching up (max hops_to_head={max_hops:.0f}), waiting 10s...")
        time.sleep(10)

    info(
        f"WARNING: flat storage did not reach hops_to_head<=10 within {timeout_s}s. "
        "Proceeding anyway."
    )


def launch_neard_processes(
    *,
    paths: Paths,
    run_dir: Path,
    n_shards: int,
    process_state: ProcessState,
    dry_run: bool,
) -> float:
    """Launches all neard instances pinned by shard index.

    WHY: CPU/NUMA pinning isolates shard workers and enforces experiment topology.
    """
    phase("Launching neard instances")

    for i in range(n_shards):
        node_home = run_dir / f"node{i}"
        log_path = node_home / "neard.log"
        cmd = [
            "numactl",
            f"--physcpubind={i},{i + 24}",
            f"--membind={i % 2}",
            str(paths.neard_bin),
            "--home",
            str(node_home),
            "run",
        ]

        if dry_run:
            info(f"DRY-RUN launch shard{i}: {' '.join(cmd)} > {log_path} 2>&1")
            continue

        # Delete LOCK immediately before launch to prevent EAGAIN
        lock_path = node_home / "data" / "LOCK"
        if lock_path.exists():
            try:
                lock_path.unlink()
                info(f"Deleted LOCK for node{i} before launch")
            except OSError as e:
                info(f"WARNING: could not delete LOCK for node{i}: {e}")

        log_f = log_path.open("w", encoding="utf-8")
        p = subprocess.Popen(
            cmd,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
            text=True,
        )
        process_state.neard_processes.append(p)

    launched_at = time.time()
    if not dry_run:
        time.sleep(5)
    return launched_at


def restart_neard_instances(
    *,
    paths: Paths,
    run_dir: Path,
    n_shards: int,
    process_state: ProcessState,
    dry_run: bool,
    storage: str,
) -> float:
    """SIGKILL neard validator processes only; keeps node data.

    Used after correcting ``tx_generator.accounts_path`` so generators match CP shards.
    """
    phase("Restarting neard instances (config reload)")
    if dry_run:
        info(
            "DRY-RUN: would SIGKILL neard PGPIDs, safe_pkill_neard, "
            "sleep (10s if n_shards>=16 and HDD else 3s), wait_for_lock_release, "
            "relaunch neard"
        )
        return time.time()
    for p in process_state.neard_processes:
        kill_process_group(p)
    process_state.neard_processes.clear()
    safe_pkill_neard(dry_run=False)
    post_kill_s = 10 if n_shards >= 16 and storage == "hdd" else 3
    time.sleep(post_kill_s)
    wait_for_lock_release(run_dir, n_shards, timeout_s=30)
    return launch_neard_processes(
        paths=paths,
        run_dir=run_dir,
        n_shards=n_shards,
        process_state=process_state,
        dry_run=False,
    )


def kill_process_group(proc: subprocess.Popen) -> None:
    """Kills a process group started with setsid, ignoring already-exited cases."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def cleanup_processes(process_state: ProcessState, *, dry_run: bool) -> None:
    """Best-effort cleanup of all spawned subprocesses and residual neard tasks."""
    phase("Cleaning up processes")
    for p in process_state.neard_processes:
        if dry_run:
            info(f"DRY-RUN killpg for pid={p.pid}")
        else:
            kill_process_group(p)

    safe_pkill_neard(dry_run=dry_run)
    if not dry_run:
        time.sleep(5)


def readiness_gate(
    *,
    n_shards: int,
    launched_at: float,
    readiness_timeout_override: int | None = None,
    skip_stabilization: bool = False,
) -> tuple[dict[int, int], float, list[str]]:
    """Waits until all readiness conditions pass for every instance.

    WHY: Starting measurement before all validators are live would skew TPS and
    block-time metrics, especially on a single HDD with slow startup.
    """
    phase("Entering readiness gate")
    gate_start = time.time()
    readiness_timeout_s = (
        readiness_timeout_override
        if readiness_timeout_override is not None
        else (900 if n_shards <= 8 else 1800)
    )
    readiness_poll_interval_s = 10 if n_shards <= 8 else 30
    chunk_tx_conditions_required = False
    info("NOTE: tx conditions 5 and 6 are advisory only. Gate passes on conditions 1-4.")
    tx_count_history: list[tuple[float, dict[int, int]]] = []
    warned_slow = set()
    warnings: list[str] = []

    while True:
        elapsed = time.time() - gate_start
        if elapsed > readiness_timeout_s:
            raise ExperimentError(f"Readiness timeout reached ({readiness_timeout_s}s)")

        status_rows: list[dict[str, Any]] = []
        heights: dict[int, int] = {}
        counts_a: dict[int, int] = {}

        for i in range(n_shards):
            port = 3030 + i
            st = fetch_status(port)
            metrics = fetch_metrics(port)

            cond1 = st is not None
            cond2 = bool(cond1 and st["sync_info"].get("syncing") is False)
            h = int(st["sync_info"].get("latest_block_height", 0)) if cond1 else 0
            cond3 = h >= 10
            heights[i] = h

            if cond3 and elapsed > READINESS_SLOW_WARNING_S and i not in warned_slow:
                msg = f"WARNING: shard{i} reached height>=10 after {elapsed:.1f}s"
                warnings.append(msg)
                info(msg)
                warned_slow.add(i)

            cnt = metrics_chunk_tx_total(metrics) if metrics is not None else 0
            counts_a[i] = cnt
            cond5 = cnt > 0

            status_rows.append(
                {
                    "shard": i,
                    "c1_http": cond1,
                    "c2_sync": cond2,
                    "c3_h>=10": cond3,
                    "c5_tx>0": cond5,
                    "height": h,
                    "count": cnt,
                }
            )

        spread_ok = (max(heights.values()) - min(heights.values()) <= 3) if heights else False

        time.sleep(readiness_poll_interval_s)

        counts_b: dict[int, int] = {}
        for i in range(n_shards):
            mt = fetch_metrics(3030 + i)
            c2 = metrics_chunk_tx_total(mt) if mt is not None else 0
            counts_b[i] = c2

        now = time.time()
        tx_count_history.append((now, dict(counts_b)))
        cutoff_ts = now - READINESS_COND6_COMPARE_WINDOW_S
        baseline_counts: dict[int, int] | None = None
        for hist_ts, hist_counts in reversed(tx_count_history):
            if hist_ts <= cutoff_ts:
                baseline_counts = hist_counts
                break

        if baseline_counts is None:
            cond6_map = {i: False for i in range(n_shards)}
        else:
            cond6_map = {
                i: i in baseline_counts and counts_b[i] > baseline_counts[i] for i in range(n_shards)
            }

        # Render condition table on every poll.
        info("Readiness condition table:")
        header = "shard c1_http c2_not_sync c3_height c4_agree c5_tx>0 c6_tx_increasing height tx_count"
        print(header)
        for row in status_rows:
            i = row["shard"]
            print(
                f"{i:>5} {str(row['c1_http']):>7} {str(row['c2_sync']):>11} {str(row['c3_h>=10']):>9} "
                f"{str(spread_ok):>8} {str(row['c5_tx>0']):>7} {str(cond6_map[i]):>16} {row['height']:>6} {counts_b[i]:>8}"
            )

        all_pass = True
        for row in status_rows:
            i = row["shard"]
            core_ok = (
                row["c1_http"]
                and row["c2_sync"]
                and row["c3_h>=10"]
                and spread_ok
            )
            shard_ok = core_ok
            if not shard_ok:
                all_pass = False
                break

        if all_pass:
            readiness_elapsed_s = time.time() - launched_at
            if skip_stabilization:
                info(f"All {n_shards} instances ready (skipping post-gate stabilization).")
            else:
                info(f"All {n_shards} instances ready. Stabilizing for {STABILIZATION_S}s...")
                time.sleep(STABILIZATION_S)
            return heights, readiness_elapsed_s, warnings

        # Extra delay between poll cycles only if inner scrape spacing were ever
        # smaller than READINESS_POLL_INTERVAL_S; currently they match per shard tier.
        time.sleep(max(0, READINESS_POLL_INTERVAL_S - readiness_poll_interval_s))


def verify_chunk_production_balance(
    n_shards: int,
    counts_start: dict[int, int],
    counts_end: dict[int, int],
    elapsed_s: float,
) -> tuple[list[str], dict[str, Any]]:
    """Verify all shards produced chunks during the measurement window.

    WHY: near_chunk_transactions_total only increments when THIS validator produces a chunk.
    If a validator produced zero chunks during measurement, its TPS is 0 regardless of
    tx_generator activity. This catches:
    1. Shard assignment mismatches (validator assigned wrong shard)
    2. Validators that stalled during measurement
    3. Unequal chunk production (some shards much slower than others)

    Returns (warnings, chunk_production_balance) for JSON artifacts.
    """
    deltas = {i: counts_end[i] - counts_start[i] for i in range(n_shards)}
    tps_values = {i: (deltas[i] / elapsed_s) if elapsed_s > 0 else 0.0 for i in range(n_shards)}

    warnings: list[str] = []

    zero_shards = [i for i in range(n_shards) if deltas[i] == 0]
    if zero_shards:
        warnings.append(
            f"BALANCE WARNING: shards {zero_shards} produced ZERO chunks "
            f"during measurement window. These validators may be misassigned "
            f"or stalled. Their TPS=0 will drag down averages."
        )

    active_tps_list = [tps_values[i] for i in range(n_shards) if deltas[i] > 0]
    cv: float | None = None
    if len(active_tps_list) >= 2:
        mean_tps = sum(active_tps_list) / len(active_tps_list)
        if mean_tps > 0:
            variance = sum((x - mean_tps) ** 2 for x in active_tps_list) / len(active_tps_list)
            cv = (variance**0.5) / mean_tps
            if cv > 0.3:
                warnings.append(
                    f"BALANCE WARNING: high TPS variance across shards "
                    f"(CV={cv:.2f} > 0.3). Per-shard TPS: "
                    f"{[round(tps_values[i], 1) for i in range(n_shards)]}. "
                    f"Chunk production is uneven."
                )

    active_count = n_shards - len(zero_shards)
    info(
        f"Chunk production balance: {active_count}/{n_shards} shards "
        f"produced chunks. Per-shard deltas: "
        f"{[deltas[i] for i in range(n_shards)]}"
    )

    chunk_production_balance: dict[str, Any] = {
        "active_shards": active_count,
        "zero_shards": zero_shards,
        "per_shard_deltas": {str(i): deltas[i] for i in range(n_shards)},
        "coefficient_of_variation": cv,
    }
    return warnings, chunk_production_balance


def measurement_is_valid(
    counts_start: dict[int, int],
    counts_end: dict[int, int],
    h1: dict[int, int],
    h2: dict[int, int],
    n_shards: int,
) -> bool:
    """Checks whether a measurement window captured live chain activity."""
    active = sum(1 for i in range(n_shards) if counts_end[i] - counts_start[i] > 0)
    height_advanced = any(h2.get(i, 0) > h1.get(i, 0) for i in range(n_shards))
    return active >= max(1, n_shards // 2) and height_advanced


def measure_run(
    *,
    paths: Paths,
    n_shards: int,
    accounts_per_shard: int,
    duration_s: int,
    warnings: list[str],
    launched_at: float,
    low_tps_phase_s: int = 120,
) -> dict[str, Any]:
    """Executes TPS, block-time, and execution-rate (tx processed metrics)."""
    phase(f"Starting measurement window ({duration_s}s)")

    # Window boundaries: H1/counts_start at t1; sleep(duration); H2/counts_end; t2 after end snapshots
    # so elapsed matches the interval over which chunk counters and heights advanced.
    #
    # Per-shard TPS from metrics: instance i is the sole chunk producer for shard i when
    # genesis sets num_block_producer_seats_per_shard = [1] * N (one CP seat per shard).
    # Then near_chunk_transactions_total on instance i counts only txs in chunks produced by
    # that node for shard i — aligned with shard i TPS. If seat counts per shard were > 1,
    # CP rotation would make this per-instance counter a poor proxy for shard-wide TPS.
    phase_end = launched_at + low_tps_phase_s + 10  # 10s buffer after phase transition
    wait_s = phase_end - time.time()
    if wait_s > 0:
        info(f"Waiting {wait_s:.0f}s for tx_generator phase transition to 10k TPS...")
        time.sleep(wait_s)
    else:
        info("tx_generator phase transition already passed, proceeding immediately.")

    elapsed = 0.0
    finality_samples: list[float] = []

    for attempt in range(MAX_MEASUREMENT_RETRIES + 1):
        t1 = time.time()
        h1 = {}
        counts_start = {}
        processed_start = {}
        with ThreadPoolExecutor(max_workers=n_shards) as ex:
            futures = {
                ex.submit(fetch_shard_snapshot, i, HTTP_TIMEOUT_S): i for i in range(n_shards)
            }
            for future in as_completed(futures):
                snap = future.result()
                i = snap["shard"]
                if snap["status_ok"]:
                    h1[i] = snap["height"]
                else:
                    h1[i] = 0
                    info(f"WARNING: /status fetch failed at measurement window start (shard{i})")
                counts_start[i] = snap["chunk_tx"]
                processed_start[i] = snap["processed"]

        for i in range(n_shards):
            info(f"counts_start shard{i}: {counts_start[i]} processed={processed_start[i]}")

        # Sample finality lag from instance 0 (representative for the network).
        finality_samples = []
        port0 = 3030
        s0 = _sample_finality_lag(port0)
        if s0 is not None:
            finality_samples.append(s0)
        remaining = duration_s
        while remaining > 0:
            sleep_chunk = min(FINALITY_SAMPLE_INTERVAL_S, remaining)
            time.sleep(sleep_chunk)
            remaining -= sleep_chunk
            s = _sample_finality_lag(port0)
            if s is not None:
                finality_samples.append(s)

        h2 = {}
        counts_end = {}
        processed_end = {}
        # Use longer timeout at window end — validators under heavy load
        # (e.g. N=16 HDD) may not respond within the default 10s.
        END_FETCH_TIMEOUT_S = 30
        with ThreadPoolExecutor(max_workers=n_shards) as ex:
            futures = {
                ex.submit(fetch_shard_snapshot, i, END_FETCH_TIMEOUT_S): i
                for i in range(n_shards)
            }
            for future in as_completed(futures):
                snap = future.result()
                i = snap["shard"]
                if snap["status_ok"]:
                    h2[i] = snap["height"]
                else:
                    h2[i] = h1[i]
                    info(
                        f"WARNING: /status fetch failed at measurement window end (shard{i}); "
                        "H2 defaulted to H1"
                    )
                counts_end[i] = snap["chunk_tx"]
                processed_end[i] = snap["processed"]

        t2 = time.time()
        elapsed = t2 - t1

        if measurement_is_valid(counts_start, counts_end, h1, h2, n_shards):
            break
        if attempt < MAX_MEASUREMENT_RETRIES:
            info(
                f"WARNING: measurement attempt {attempt + 1} produced zero results "
                f"(chain stall detected). Waiting {MEASUREMENT_RETRY_WAIT_S}s and retrying..."
            )
            time.sleep(MEASUREMENT_RETRY_WAIT_S)
        else:
            info("WARNING: all measurement attempts produced zero results. Reporting as-is.")

    balance_warnings, chunk_production_balance = verify_chunk_production_balance(
        n_shards=n_shards,
        counts_start=counts_start,
        counts_end=counts_end,
        elapsed_s=elapsed,
    )
    warnings.extend(balance_warnings)

    for i in range(n_shards):
        d = counts_end[i] - counts_start[i]
        tps_line = (d / elapsed) if elapsed > 0 else 0.0
        info(
            f"counts_end shard{i}: {counts_end[i]} "
            f"delta={d} "
            f"tps={tps_line:.2f}"
        )

    tps_per_shard: dict[int, float] = {}
    block_time_ms_per_shard: dict[int, float | None] = {}

    for i in range(n_shards):
        delta_count = counts_end[i] - counts_start[i]
        tps = delta_count / elapsed if elapsed > 0 else 0.0
        tps_per_shard[i] = tps

        delta_h = h2[i] - h1[i]
        info(
            f"block height snapshot: shard{i} H1={h1[i]} H2={h2[i]} "
            f"delta={delta_h} elapsed={elapsed:.1f}s"
        )
        if delta_h > 0:
            block_time_ms_per_shard[i] = (elapsed * 1000) / delta_h
        else:
            block_time_ms_per_shard[i] = None
            info(
                f"WARNING: block height did not increase during measurement window (shard{i} "
                f"H1={h1[i]} H2={h2[i]})"
            )

    execution_rate_per_shard: dict[int, float] = {}
    for i in range(n_shards):
        total_start, failed_start, successful_start = processed_start.get(i, (0, 0, 0))
        total_end, failed_end, successful_end = processed_end.get(i, (0, 0, 0))
        total_delta = total_end - total_start
        failed_delta = failed_end - failed_start
        successful_delta = successful_end - successful_start
        rate = (successful_delta / total_delta) if total_delta > 0 else 1.0
        execution_rate_per_shard[i] = rate
        if failed_delta > 0:
            warnings.append(
                f"WARNING: shard{i} had {failed_delta} failed transactions "
                f"(execution_rate={rate:.4f})"
            )
        info(
            f"shard{i} processed: total={total_delta} "
            f"failed={failed_delta} successful={successful_delta} "
            f"execution_rate={rate:.4f}"
        )
    execution_rate_avg = float(
        sum(execution_rate_per_shard.values()) / n_shards if n_shards else 1.0
    )

    aggregate_tps = float(sum(tps_per_shard.values()))
    avg_tps_per_shard = aggregate_tps / n_shards if n_shards else 0.0
    valid_block_times = [x for x in block_time_ms_per_shard.values() if x is not None]
    avg_block_time_ms = float(mean(valid_block_times)) if valid_block_times else 0.0

    finality_lag_blocks_avg: float | None = (
        float(mean(finality_samples)) if finality_samples else None
    )
    finality_lag_ms_avg: float | None = (
        finality_lag_blocks_avg * avg_block_time_ms
        if finality_lag_blocks_avg is not None and avg_block_time_ms
        else None
    )

    return {
        "tps_per_shard": tps_per_shard,
        "aggregate_tps": aggregate_tps,
        "avg_tps_per_shard": avg_tps_per_shard,
        "counts_start": counts_start,
        "counts_end": counts_end,
        "elapsed_s": elapsed,
        "chunk_production_balance": chunk_production_balance,
        "block_heights_start": h1,
        "block_heights_end": h2,
        "block_time_ms_per_shard": block_time_ms_per_shard,
        "avg_block_time_ms": avg_block_time_ms,
        "finality_lag_blocks_avg": finality_lag_blocks_avg,
        "finality_lag_ms_avg": finality_lag_ms_avg,
        "finality_lag_sample_count": len(finality_samples),
        "execution_rate_avg": execution_rate_avg,
        "execution_rate_per_shard": execution_rate_per_shard,
    }


def append_results(
    *,
    paths: Paths,
    n_shards: int,
    storage_mode: str,
    readiness_time_s: float,
    measurement: dict[str, Any],
    warnings: list[str],
    wall_clock_s: float,
) -> None:
    """Writes CSV + JSON detail + text summary and prints summary to stdout."""
    phase("Writing result artifacts")
    paths.results_dir.mkdir(parents=True, exist_ok=True)

    csv_path = paths.results_dir / "scaling_results.csv"
    csv_exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not csv_exists:
            w.writerow(
                [
                    "n_shards",
                    "storage_mode",
                    "aggregate_tps",
                    "avg_tps_per_shard",
                    "active_shards",
                    "avg_block_time_ms",
                    "finality_lag_blocks",
                    "finality_lag_ms",
                    "execution_rate_avg",
                ]
            )

        def _csv_opt_float(val: float | None, ndigits: int) -> str | float:
            if val is None:
                return ""
            return round(val, ndigits)

        w.writerow(
            [
                n_shards,
                storage_mode,
                round(measurement["aggregate_tps"], 1),
                round(measurement["avg_tps_per_shard"], 1),
                int(measurement["chunk_production_balance"]["active_shards"]),
                round(measurement["avg_block_time_ms"], 1),
                _csv_opt_float(measurement.get("finality_lag_blocks_avg"), 2),
                _csv_opt_float(measurement.get("finality_lag_ms_avg"), 1),
                round(measurement["execution_rate_avg"], 3),
            ]
        )

    detail_path = paths.results_dir / "scaling_results_detail.json"
    detail: list[Any] = []
    if detail_path.exists():
        raw = detail_path.read_text(encoding="utf-8").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    detail = parsed
            except json.JSONDecodeError:
                pass

    detail.append(
        {
            "n_shards": n_shards,
            "storage_mode": storage_mode,
            "timestamp": utc_iso(),
            "tps_per_shard": {str(k): v for k, v in measurement["tps_per_shard"].items()},
            "aggregate_tps": measurement["aggregate_tps"],
            "avg_tps_per_shard": measurement["avg_tps_per_shard"],
            "counts_start": {str(k): v for k, v in measurement["counts_start"].items()},
            "counts_end": {str(k): v for k, v in measurement["counts_end"].items()},
            "elapsed_s": measurement["elapsed_s"],
            "chunk_production_balance": measurement["chunk_production_balance"],
            "block_heights_start": {str(k): v for k, v in measurement["block_heights_start"].items()},
            "block_heights_end": {str(k): v for k, v in measurement["block_heights_end"].items()},
            "block_time_ms_per_shard": {
                str(k): v for k, v in measurement["block_time_ms_per_shard"].items()
            },
            "avg_block_time_ms": measurement["avg_block_time_ms"],
            "finality_lag_blocks_avg": measurement.get("finality_lag_blocks_avg"),
            "finality_lag_ms_avg": measurement.get("finality_lag_ms_avg"),
            "finality_lag_sample_count": measurement.get("finality_lag_sample_count"),
            "execution_rate_per_shard": {
                str(k): v for k, v in measurement["execution_rate_per_shard"].items()
            },
            "readiness_time_s": readiness_time_s,
            "warnings": warnings,
        }
    )
    detail_path.write_text(json.dumps(detail, indent=2), encoding="utf-8")

    summary_lines = [
        f"Single-node shard scaling summary (N={n_shards})",
        f"Timestamp: {utc_iso()}",
        f"storage_mode: {storage_mode}",
        "",
        "Run parameters:",
        f"  n_shards: {n_shards}",
        f"  storage_mode: {storage_mode}",
        f"  readiness_time_s: {readiness_time_s:.1f}",
        f"  measurement_elapsed_s: {measurement['elapsed_s']:.1f}",
        f"  wall_clock_s_total: {wall_clock_s:.1f}",
        "",
        "Per-shard TPS:",
    ]
    for i in range(n_shards):
        summary_lines.append(f"  shard{i}: {measurement['tps_per_shard'][i]:.2f}")

    summary_lines.extend(
        [
            "",
            f"Aggregate TPS: {measurement['aggregate_tps']:.2f}",
            f"Avg TPS per shard: {measurement['avg_tps_per_shard']:.2f}",
            f"Avg block time (ms): {measurement['avg_block_time_ms']:.2f}",
            "  note: transactions sit in mempool at most 1 block time",
            "   before inclusion and execution. Block time is therefore",
            "   the practical upper bound on time-to-execution.",
            "",
            "Finality latency (BFT finality):",
        ]
    )
    flb = measurement.get("finality_lag_blocks_avg")
    flm = measurement.get("finality_lag_ms_avg")
    if flb is not None:
        summary_lines.append(f"  avg lag blocks: {flb:.2f}")
    if flm is not None:
        summary_lines.append(f"  avg lag ms: {flm:.1f}ms")
    if flb is not None or flm is not None:
        summary_lines.extend(
            [
                "  note: near_largest_final_height advances in bursts",
                "   (not smoothly per block). This is full BFT finality —",
                "   mathematically irreversible. Different from time-to-execution",
                "   which equals approximately 1 block time.",
            ]
        )
    if flb is None and flm is None:
        summary_lines.append(
            "  (finality metrics unavailable — /metrics scrape missed or incomplete)"
        )
    summary_lines.extend(
        [
            "",
            f"Execution rate avg: {measurement['execution_rate_avg']:.3f}",
            "",
            "Execution rate note: successful_delta / total_delta from near_transaction_processed_* ",
            "counters on each validator's /metrics (chunk inclusion vs apply-time processing).",
            "",
            "Execution rate per shard:",
        ]
    )
    for i in range(n_shards):
        summary_lines.append(
            f"  shard{i}: {measurement['execution_rate_per_shard'][i]:.4f}"
        )

    if warnings:
        summary_lines.append("")
        summary_lines.append("Warnings:")
        summary_lines.extend([f"  - {w}" for w in warnings])

    summary = "\n".join(summary_lines)
    summary_path = paths.results_dir / f"run_S{n_shards:02d}_summary.txt"
    summary_path.write_text(summary + "\n", encoding="utf-8")
    print(summary)


def write_failure_artifacts(
    *,
    paths: Paths,
    n_shards: int,
    storage_mode: str,
    warnings: list[str],
    error_message: str,
    wall_clock_s: float,
) -> None:
    """Writes partial artifacts when a run fails midway.

    WHY: Preserve debugging context and satisfy the requirement that partial
    results should still be written on failures.
    """
    paths.results_dir.mkdir(parents=True, exist_ok=True)

    detail_path = paths.results_dir / "scaling_results_detail.json"
    detail: list[Any] = []
    if detail_path.exists():
        raw = detail_path.read_text(encoding="utf-8").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    detail = parsed
            except json.JSONDecodeError:
                pass

    detail.append(
        {
            "n_shards": n_shards,
            "storage_mode": storage_mode,
            "timestamp": utc_iso(),
            "status": "failed",
            "error": error_message,
            "warnings": warnings,
            "wall_clock_s_total": wall_clock_s,
        }
    )
    detail_path.write_text(json.dumps(detail, indent=2), encoding="utf-8")

    summary_lines = [
        f"Single-node shard scaling summary (N={n_shards})",
        f"Timestamp: {utc_iso()}",
        f"storage_mode: {storage_mode}",
        "STATUS: FAILED",
        f"Error: {error_message}",
        f"Wall clock s total: {wall_clock_s:.1f}",
    ]
    if warnings:
        summary_lines.append("Warnings:")
        summary_lines.extend([f"  - {w}" for w in warnings])
    summary = "\n".join(summary_lines)
    summary_path = paths.results_dir / f"run_S{n_shards:02d}_summary.txt"
    summary_path.write_text(summary + "\n", encoding="utf-8")
    print(summary)


def parse_args() -> argparse.Namespace:
    """Parses CLI args exactly as required by experiment contract."""
    p = argparse.ArgumentParser(description="Single-machine NEAR shard scaling experiment")
    p.add_argument("--shards", type=int, required=True, help="Shard count: one of 1,2,4,8,16,24")
    p.add_argument(
        "--storage",
        choices=["hdd", "tmpfs"],
        default="hdd",
        help="Storage backend: hdd uses ~/bench/singlenode/, tmpfs uses /dev/shm/near-bench/ (RAM-backed).",
    )
    p.add_argument("--skip-setup", action="store_true", help="Reuse existing setup artifacts when present")
    p.add_argument("--dry-run", action="store_true", help="Print planned actions without executing")
    p.add_argument(
        "--setup-only",
        action="store_true",
        help="Write genesis, configs, and keys then exit (no wipe/neard). For validate-config.",
    )
    p.add_argument("--accounts", type=int, default=5000, help="Accounts per shard (100..50000)")
    p.add_argument("--duration", type=int, default=180, help="Measurement duration in seconds")
    p.add_argument(
        "--keep-logs",
        action="store_true",
        help="Copy neard logs to results dir before wiping run directory",
    )
    args = p.parse_args()

    if args.shards not in ALLOWED_SHARDS:
        p.error("--shards must be one of: 1,2,4,8,16,24")
    if not (100 <= args.accounts <= 50000):
        p.error("--accounts must be between 100 and 50000")
    if args.duration <= 0:
        p.error("--duration must be > 0")
    if args.setup_only and args.dry_run:
        p.error("--setup-only cannot be combined with --dry-run (dry-run skips real key material)")
    return args


def install_interrupt_handler(process_state: ProcessState, dry_run: bool) -> None:
    """Installs SIGINT handler that performs deterministic cleanup."""

    def _handler(signum: int, frame: Any) -> None:  # noqa: ARG001
        print("\nInterrupted — cleaning up", flush=True)
        try:
            cleanup_processes(process_state, dry_run=dry_run)
        finally:
            raise SystemExit(1)

    signal.signal(signal.SIGINT, _handler)


def assert_node_keys_exist(run_dir: Path, n_shards: int) -> None:
    """Verify all node key files exist before launching neard.

    WHY: A partial or corrupt run directory from a failed previous
    run can leave some nodes without key files. neard fails
    immediately with "Failed reading node key file" producing
    zombies that never serve HTTP and cause readiness to fail
    permanently. Fail fast here with a clear error.
    """
    missing = []
    for i in range(n_shards):
        node_dir = run_dir / f"node{i}"
        for fname in ["node_key.json", "validator_key.json",
                      "config.json"]:
            fpath = node_dir / fname
            if not fpath.exists():
                missing.append(str(fpath))
    if missing:
        raise ExperimentError(
            f"Missing key/config files — run directory is corrupt. "
            f"Missing:\n" + "\n".join(f"  {p}" for p in missing) +
            f"\n\nFix: pkill -9 neard && "
            f"rm -rf {run_dir} and rerun."
        )
    info(f"Key file assertion passed: all {n_shards} nodes have "
         f"node_key.json, validator_key.json, config.json")


def run_experiment(args: argparse.Namespace) -> None:
    """Coordinates full lifecycle from setup to measurement to artifact writing."""
    start_wall = time.time()
    home = Path.home()
    paths = Paths(
        home=home,
        neard_bin=REPO_ROOT / "nearcore" / "target" / "release" / "neard",
        config_template=home / "bench" / "config.json",
        genesis_template=home / "bench" / "genesis.json",
        accounts_base=home / "bench" / "singlenode" / "accounts",
        run_base=(
            Path("/dev/shm/near-bench") / f"S{args.shards:02d}"
            if args.storage == "tmpfs"
            else home / "bench" / "singlenode" / f"S{args.shards:02d}"
        ),
        results_dir=REPO_ROOT / "results" / "single_node_scaling" / args.storage,
        monitor_out=(
            REPO_ROOT
            / "results"
            / "single_node_scaling"
            / args.storage
            / f"monitor_S{args.shards:02d}.json"
        ),
    )

    if args.storage == "tmpfs":
        shm = Path("/dev/shm")
        if not shm.exists():
            sys.exit("ERROR: /dev/shm does not exist on this host")
        stat = shutil.disk_usage(shm)
        required_gb = args.shards * 3.5
        available_gb = stat.free / (1024 ** 3)
        if available_gb < required_gb:
            sys.exit(
                f"ERROR: /dev/shm has {available_gb:.1f}GB free, "
                f"need ~{required_gb:.1f}GB for {args.shards} shards"
            )

    preflight_checks(paths, args.dry_run)

    for required in (paths.neard_bin, paths.config_template, paths.genesis_template):
        if not args.dry_run and not required.exists():
            raise ExperimentError(f"Required path missing: {required}")

    process_state = ProcessState(neard_processes=[])
    install_interrupt_handler(process_state, args.dry_run)

    warnings: list[str] = []
    measurement: dict[str, Any] | None = None
    readiness_time_s = 0.0

    try:
        ensure_accounts(
            paths=paths,
            n_shards=args.shards,
            accounts_per_shard=args.accounts,
            dry_run=args.dry_run,
        )

        validator_keys = ensure_validator_keys(
            paths=paths,
            n_shards=args.shards,
            run_dir=paths.run_base,
            skip_setup=args.skip_setup,
            dry_run=args.dry_run,
        )

        genesis_path = paths.run_base / "genesis.json"
        if args.skip_setup and genesis_path.exists() and not args.dry_run:
            phase("Skipping genesis generation (--skip-setup and file exists)")
        else:
            genesis = build_genesis(
                paths=paths,
                n_shards=args.shards,
                accounts_per_shard=args.accounts,
                validator_keys=validator_keys,
                dry_run=args.dry_run,
            )
            write_genesis_and_node_homes(
                run_dir=paths.run_base,
                genesis=genesis,
                n_shards=args.shards,
                dry_run=args.dry_run,
            )

        generate_node_configs(
            paths=paths,
            run_dir=paths.run_base,
            n_shards=args.shards,
            validator_keys=validator_keys,
            dry_run=args.dry_run,
        )

        if args.setup_only:
            phase("Setup-only: wrote genesis and configs; exiting before wipe and neard launch")
            return

        phase("Pre-launch cleanup")
        safe_pkill_neard(dry_run=args.dry_run)
        if not args.dry_run:
            time.sleep(3)

        wipe_node_data_dirs(run_dir=paths.run_base, n_shards=args.shards, dry_run=args.dry_run)

        if not args.dry_run:
            assert_node_keys_exist(paths.run_base, args.shards)

        info(f"Monitor output path: {paths.monitor_out}")
        info(f"Run: python3 scripts/monitor.py --shards {args.shards} --output {paths.monitor_out} --wait")
        launched_at = launch_neard_processes(
            paths=paths,
            run_dir=paths.run_base,
            n_shards=args.shards,
            process_state=process_state,
            dry_run=args.dry_run,
        )

        if args.dry_run:
            phase("Dry-run complete")
            return

        _, readiness_time_s, gate_warnings = readiness_gate(n_shards=args.shards, launched_at=launched_at)
        warnings.extend(gate_warnings)

        node_assignment = verify_chunk_producer_shard_assignment(args.shards)
        configs_changed = fix_accounts_path_for_assignment(
            paths=paths,
            run_dir=paths.run_base,
            n_shards=args.shards,
            assignment=node_assignment,
            dry_run=False,
        )
        if configs_changed:
            launched_at = restart_neard_instances(
                paths=paths,
                run_dir=paths.run_base,
                n_shards=args.shards,
                process_state=process_state,
                dry_run=False,
                storage=args.storage,
            )
            restart_readiness_timeout_s = 900 if args.shards <= 8 else READINESS_RESTART_TIMEOUT_S
            _, r2, w2 = readiness_gate(
                n_shards=args.shards,
                launched_at=launched_at,
                readiness_timeout_override=restart_readiness_timeout_s,
                skip_stabilization=True,
            )
            readiness_time_s += r2
            warnings.extend(w2)
            # Do NOT reset launched_at here. It is already set to the restart
            # time by restart_neard_instances. measure_run computes the phase
            # wait relative to that restart time, so only the remaining portion
            # of the 120s low-TPS phase will be waited — matching the curated run behavior.

        measurement = measure_run(
            paths=paths,
            n_shards=args.shards,
            accounts_per_shard=args.accounts,
            duration_s=args.duration,
            warnings=warnings,
            launched_at=launched_at,
            low_tps_phase_s=120,
        )

    except Exception as e:
        if not args.dry_run:
            write_failure_artifacts(
                paths=paths,
                n_shards=args.shards,
                storage_mode=args.storage,
                warnings=warnings,
                error_message=str(e),
                wall_clock_s=time.time() - start_wall,
            )
        raise
    finally:
        cleanup_processes(process_state, dry_run=args.dry_run)

    if measurement is not None:
        append_results(
            paths=paths,
            n_shards=args.shards,
            storage_mode=args.storage,
            readiness_time_s=readiness_time_s,
            measurement=measurement,
            warnings=warnings,
            wall_clock_s=time.time() - start_wall,
        )
        # Wipe run directory after results are written to free disk space.
        # Keys and configs are regenerated at the start of each run so
        # this is safe. Accounts are in a separate directory and untouched.
        if not args.dry_run and paths.run_base.exists():
            if measurement.get("aggregate_tps", 0) > 0:
                if args.keep_logs:
                    log_archive = paths.results_dir / f"neard_logs_S{args.shards:02d}"
                    log_archive.mkdir(parents=True, exist_ok=True)
                    for i in range(args.shards):
                        src = paths.run_base / f"node{i}" / "neard.log"
                        if src.exists():
                            shutil.copy2(src, log_archive / f"node{i}_neard.log")
                    info(f"Copied neard logs to {log_archive}")
                info(f"Wiping run directory to free disk space: {paths.run_base}")
                shutil.rmtree(paths.run_base)
                info(f"Run directory wiped: {paths.run_base}")
            else:
                info(f"Skipping wipe — zero TPS measurement, preserving logs at {paths.run_base}")


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    try:
        run_experiment(args)
    except ExperimentError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
