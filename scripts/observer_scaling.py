#!/usr/bin/env python3
"""16-validator + 8-observer single-machine NEAR benchmark.

Mirrors the structure of single_node_scaling.py but adds:
- 8 observer neard processes (no validator key, no stake) on cores 16..23 + HT siblings.
- Validators have tx_generator=null. Transactions are produced by external_tx_submitter.py
  processes (one per observer), each driving 2 shards.
- Boot nodes connect all 24 nodes into a single mesh.

Reuses small helpers from run_benchmark.py (historical name single_node_scaling.py):
HTTP scrapers, finality sampler, chunk-producer assignment, balance verifier,
cleanup — for parity with the existing measurement code path.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_DIR = Path(__file__).resolve().parent

from run_benchmark import (
    Paths,
    ExperimentError,
    HTTP_TIMEOUT_S,
    STABILIZATION_S,
    READINESS_SLOW_WARNING_S,
    FINALITY_SAMPLE_INTERVAL_S,
    MAX_MEASUREMENT_RETRIES,
    MEASUREMENT_RETRY_WAIT_S,
    utc_iso,
    phase,
    info,
    run_cmd,
    safe_pkill_neard,
    read_json,
    write_json,
    metrics_chunk_tx_total,
    fetch_status,
    fetch_metrics,
    fetch_shard_snapshot,
    _sample_finality_lag,
    verify_chunk_producer_shard_assignment,
    cleanup_processes,
    measurement_is_valid,
    verify_chunk_production_balance,
)

N_VALIDATORS = 16
N_OBSERVERS = 8
N_SHARDS = 16
VALIDATOR_RPC_BASE = 3030
VALIDATOR_P2P_BASE = 24567
OBSERVER_RPC_BASE = 3046
OBSERVER_P2P_BASE = 24583
READINESS_TIMEOUT_S = 1800
READINESS_POLL_INTERVAL_S = 30


@dataclass
class ObsProcessState:
    """Tracks neard validators+observers and external submitter subprocesses."""

    neard_processes: list[subprocess.Popen]
    submitters: list[subprocess.Popen] = field(default_factory=list)


def preflight_observer(paths: Paths) -> None:
    """Verifies neard binary, tools, templates, and the submitter script exist."""
    phase("Preflight checks (observer experiment)")
    failures: list[str] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        info(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))
        if not ok:
            failures.append(f"{name}: {detail}".strip(": "))

    record("neard binary", paths.neard_bin.exists() and os.access(paths.neard_bin, os.X_OK), str(paths.neard_bin))
    cp = subprocess.run([str(paths.neard_bin), "--version"], text=True, capture_output=True)
    record("neard --version", cp.returncode == 0, cp.stdout.strip() or cp.stderr.strip())
    for tool in ("numactl", "pkill"):
        record(f"tool: {tool}", shutil.which(tool) is not None, shutil.which(tool) or "not found")
    for name, p in (("config.json", paths.config_template), ("genesis.json", paths.genesis_template)):
        record(f"{name} exists", p.exists(), str(p))
        if p.exists():
            try:
                read_json(p)
                record(f"{name} valid JSON", True)
            except Exception as e:
                record(f"{name} valid JSON", False, str(e))
    submitter = _SCRIPT_DIR / "external_tx_submitter.py"
    record("external_tx_submitter.py", submitter.exists(), str(submitter))
    if failures:
        raise ExperimentError("Preflight failed:\n- " + "\n- ".join(failures))


def ensure_accounts_observer(paths: Paths, accounts_per_shard: int) -> None:
    """Spot-checks that all 16 shard account dirs exist with first/last keys."""
    phase(f"Verifying account key files for shards 0..{N_SHARDS - 1}")
    missing: list[str] = []
    for shard in range(N_SHARDS):
        shard_dir = paths.accounts_base / f"shard{shard}"
        if not shard_dir.exists():
            missing.append(str(shard_dir))
            continue
        for idx in (0, accounts_per_shard - 1):
            fp = shard_dir / f"a{shard:02d}_user_{idx}.node0.json"
            if not fp.is_file():
                missing.append(str(fp))
    if missing:
        raise ExperimentError("Missing account files:\n" + "\n".join(f"  {p}" for p in missing))
    info(f"Account files verified for {N_SHARDS} shards")


def ensure_node_keys(
    paths: Paths,
    run_dir: Path,
    skip_setup: bool,
) -> tuple[dict[int, dict[str, str]], dict[int, dict[str, str]]]:
    """Materializes per-instance keys.

    For validators: keeps both validator_key.json (account stake key) and node_key.json (P2P).
    For observers: keeps only node_key.json. Removes validator_key.json so neard runs as
    a non-validator peer that simply tracks shards.
    """
    phase(f"Ensuring keys for {N_VALIDATORS} validators + {N_OBSERVERS} observers")
    val_keys: dict[int, dict[str, str]] = {}
    obs_keys: dict[int, dict[str, str]] = {}

    for i in range(N_VALIDATORS):
        node_home = run_dir / f"node{i}"
        node_home.mkdir(parents=True, exist_ok=True)
        vkey_path = node_home / "validator_key.json"
        nkey_path = node_home / "node_key.json"
        if not (skip_setup and vkey_path.exists() and nkey_path.exists()):
            run_cmd([
                str(paths.neard_bin), "--home", str(node_home), "init",
                "--account-id", f"node{i}", "--chain-id", "localnet",
            ])
        vkey = read_json(vkey_path)
        nkey = read_json(nkey_path)
        val_keys[i] = {
            "account_id": f"node{i}",
            "public_key": vkey["public_key"],
            "node_public_key": nkey["public_key"],
        }

    for j in range(N_OBSERVERS):
        node_home = run_dir / f"observer{j}"
        node_home.mkdir(parents=True, exist_ok=True)
        nkey_path = node_home / "node_key.json"
        vkey_path = node_home / "validator_key.json"
        if not (skip_setup and nkey_path.exists()):
            run_cmd([
                str(paths.neard_bin), "--home", str(node_home), "init",
                "--account-id", f"observer{j}", "--chain-id", "localnet",
            ])
        if vkey_path.exists():
            vkey_path.unlink()
        nkey = read_json(nkey_path)
        obs_keys[j] = {
            "account_id": f"observer{j}",
            "public_key": nkey["public_key"],
            "node_public_key": nkey["public_key"],
        }

    return val_keys, obs_keys


def build_genesis_observer(
    paths: Paths,
    accounts_per_shard: int,
    val_keys: dict[int, dict[str, str]],
    obs_keys: dict[int, dict[str, str]],
) -> dict[str, Any]:
    """Builds the 16-shard genesis with 16 validator + 8 observer accounts."""
    reference = read_json(paths.genesis_template)
    genesis: dict[str, Any] = json.loads(json.dumps(reference))

    boundaries = [f"a{i:02d}" for i in range(1, N_SHARDS)]
    genesis["chain_id"] = "localnet"
    genesis["genesis_time"] = utc_iso()
    genesis["genesis_height"] = 0
    genesis["num_block_producer_seats"] = N_VALIDATORS
    genesis["num_block_producer_seats_per_shard"] = [1] * N_SHARDS
    genesis["num_chunk_producer_seats"] = N_VALIDATORS
    genesis["minimum_validators_per_shard"] = 1
    genesis["shuffle_shard_assignment_for_chunk_producers"] = False
    genesis["transaction_validity_period"] = 10000
    genesis["epoch_length"] = 500
    genesis["gas_limit"] = 30_000_000_000_000
    genesis["shard_layout"] = {
        "V1": {
            "boundary_accounts": boundaries,
            "shards_split_map": None,
            "to_parent_shard_map": None,
            "version": 1,
        }
    }

    stake = 50_000_000 * 10**24
    user_balance = 10**24
    obs_balance = 10**24
    records: list[dict[str, Any]] = []
    validators: list[dict[str, Any]] = []

    for i in range(N_VALIDATORS):
        a = f"node{i}"
        pk = val_keys[i]["public_key"]
        validators.append({"account_id": a, "public_key": pk, "amount": str(stake)})
        records.append({
            "Account": {
                "account_id": a,
                "account": {
                    "amount": str(stake + 10**24),
                    "locked": str(stake),
                    "code_hash": "11111111111111111111111111111111",
                    "storage_usage": 0,
                    "version": "V1",
                },
            }
        })
        records.append({
            "AccessKey": {
                "account_id": a,
                "public_key": pk,
                "access_key": {"nonce": 0, "permission": "FullAccess"},
            }
        })

    for j in range(N_OBSERVERS):
        a = f"observer{j}"
        pk = obs_keys[j]["public_key"]
        records.append({
            "Account": {
                "account_id": a,
                "account": {
                    "amount": str(obs_balance),
                    "locked": "0",
                    "code_hash": "11111111111111111111111111111111",
                    "storage_usage": 0,
                    "version": "V1",
                },
            }
        })
        records.append({
            "AccessKey": {
                "account_id": a,
                "public_key": pk,
                "access_key": {"nonce": 0, "permission": "FullAccess"},
            }
        })

    for s in range(N_SHARDS):
        shard_dir = paths.accounts_base / f"shard{s}"
        for idx in range(accounts_per_shard):
            account_id = f"a{s:02d}_user_{idx}.node0"
            account_file = shard_dir / f"{account_id}.json"
            if not account_file.exists():
                raise ExperimentError(f"Missing account file: {account_file}")
            public_key = read_json(account_file)["public_key"]
            records.append({
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
            })
            records.append({
                "AccessKey": {
                    "account_id": account_id,
                    "public_key": public_key,
                    "access_key": {"nonce": 0, "permission": "FullAccess"},
                }
            })

    total = 0
    for r in records:
        wrapped = r.get("Account")
        if not wrapped:
            continue
        bal = wrapped["account"]
        total += int(bal["amount"]) + int(bal.get("locked", "0"))

    genesis["validators"] = validators
    genesis["records"] = records
    genesis["total_supply"] = str(total)
    return genesis


def write_genesis_observer(run_dir: Path, genesis: dict[str, Any]) -> None:
    """Writes shared genesis and copies into each node home (24 total)."""
    phase("Writing run genesis to all node homes")
    shared_path = run_dir / "genesis.json"
    write_json(shared_path, genesis)
    info(f"Genesis size: {shared_path.stat().st_size / (1024 * 1024):.2f} MB")
    for i in range(N_VALIDATORS):
        h = run_dir / f"node{i}"
        h.mkdir(parents=True, exist_ok=True)
        shutil.copy2(shared_path, h / "genesis.json")
    for j in range(N_OBSERVERS):
        h = run_dir / f"observer{j}"
        h.mkdir(parents=True, exist_ok=True)
        shutil.copy2(shared_path, h / "genesis.json")


def _full_boot_nodes(
    val_keys: dict[int, dict[str, str]],
    obs_keys: dict[int, dict[str, str]],
    *,
    exclude: tuple[str, int] | None = None,
) -> str:
    """Builds the boot_nodes string with all 24 P2P endpoints, optionally excluding self."""
    parts: list[str] = []
    for i in range(N_VALIDATORS):
        if exclude == ("node", i):
            continue
        parts.append(f"{val_keys[i]['node_public_key']}@127.0.0.1:{VALIDATOR_P2P_BASE + i}")
    for j in range(N_OBSERVERS):
        if exclude == ("observer", j):
            continue
        parts.append(f"{obs_keys[j]['node_public_key']}@127.0.0.1:{OBSERVER_P2P_BASE + j}")
    return ",".join(parts)


def generate_configs_observer(
    paths: Paths,
    run_dir: Path,
    val_keys: dict[int, dict[str, str]],
    obs_keys: dict[int, dict[str, str]],
) -> None:
    """Writes per-instance config.json for validators and observers."""
    phase("Generating per-node config.json files (validators + observers)")
    base = read_json(paths.config_template)

    for i in range(N_VALIDATORS):
        cfg = json.loads(json.dumps(base))
        cfg.setdefault("rpc", {})["addr"] = f"0.0.0.0:{VALIDATOR_RPC_BASE + i}"
        cfg.setdefault("network", {})
        cfg["network"]["addr"] = f"0.0.0.0:{VALIDATOR_P2P_BASE + i}"
        cfg["network"]["skip_sync_wait"] = True
        cfg["network"]["boot_nodes"] = _full_boot_nodes(val_keys, obs_keys, exclude=("node", i))
        cfg["produce_chunk_add_transactions_time_limit"] = {"secs": 2, "nanos": 0}
        cfg["transaction_pool_size_limit"] = 100_000_000
        cfg["save_tx_outcomes"] = False
        cfg["save_state_changes"] = False
        cfg["disable_tx_routing"] = True
        if isinstance(cfg.get("consensus"), dict):
            cfg["consensus"]["doomslug_step_period"] = {"secs": 0, "nanos": 10_000_000}
        if isinstance(cfg.get("store"), dict):
            cfg["store"]["load_mem_tries_for_tracked_shards"] = True
        cfg["tx_generator"] = None
        node_home = run_dir / f"node{i}"
        node_home.mkdir(parents=True, exist_ok=True)
        write_json(node_home / "config.json", cfg)

    for j in range(N_OBSERVERS):
        cfg = json.loads(json.dumps(base))
        cfg.setdefault("rpc", {})["addr"] = f"0.0.0.0:{OBSERVER_RPC_BASE + j}"
        cfg.setdefault("network", {})
        cfg["network"]["addr"] = f"0.0.0.0:{OBSERVER_P2P_BASE + j}"
        cfg["network"]["skip_sync_wait"] = True
        cfg["network"]["boot_nodes"] = _full_boot_nodes(val_keys, obs_keys, exclude=("observer", j))
        cfg["transaction_pool_size_limit"] = 100_000_000
        cfg["save_tx_outcomes"] = False
        cfg["save_state_changes"] = False
        cfg["disable_tx_routing"] = True
        if isinstance(cfg.get("store"), dict):
            cfg["store"]["load_mem_tries_for_tracked_shards"] = True
        cfg["tx_generator"] = None
        cfg["tracked_shards_config"] = "AllShards"
        node_home = run_dir / f"observer{j}"
        node_home.mkdir(parents=True, exist_ok=True)
        write_json(node_home / "config.json", cfg)


def wipe_data_dirs_observer(run_dir: Path) -> None:
    """Removes per-instance data/ dirs for a clean chain start."""
    phase("Wiping node + observer data directories")
    targets = [run_dir / f"node{i}" / "data" for i in range(N_VALIDATORS)]
    targets += [run_dir / f"observer{j}" / "data" for j in range(N_OBSERVERS)]
    for d in targets:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)


def _spawn_neard(cmd: list[str], log_path: Path) -> subprocess.Popen:
    """Launches neard with stdout/stderr captured into log_path under its own pgroup."""
    log_f = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
        text=True,
    )


def launch_validators_then_observers(
    paths: Paths,
    run_dir: Path,
    process_state: ObsProcessState,
) -> float:
    """Launches 16 validator neard processes first, then 8 observer neard processes."""
    phase("Launching 16 validator neard processes")
    for i in range(N_VALIDATORS):
        node_home = run_dir / f"node{i}"
        cmd = [
            "numactl", f"--physcpubind={i},{i + 24}", f"--membind={i % 2}",
            str(paths.neard_bin), "--home", str(node_home), "run",
        ]
        lock_path = node_home / "data" / "LOCK"
        if lock_path.exists():
            try:
                lock_path.unlink()
            except OSError:
                pass
        p = _spawn_neard(cmd, node_home / "neard.log")
        process_state.neard_processes.append(p)
    time.sleep(5)

    phase("Launching 8 observer neard processes")
    for j in range(N_OBSERVERS):
        cpu1, cpu2 = j + 16, j + 40
        node_home = run_dir / f"observer{j}"
        cmd = [
            "numactl", f"--physcpubind={cpu1},{cpu2}", f"--membind={j % 2}",
            str(paths.neard_bin), "--home", str(node_home), "run",
        ]
        lock_path = node_home / "data" / "LOCK"
        if lock_path.exists():
            try:
                lock_path.unlink()
            except OSError:
                pass
        p = _spawn_neard(cmd, node_home / "neard.log")
        process_state.neard_processes.append(p)

    launched_at = time.time()
    time.sleep(5)
    return launched_at


def readiness_gate_observer(launched_at: float) -> tuple[float, list[str]]:
    """Waits for all 16 validators to satisfy c1..c4. Observer HTTP is advisory only."""
    phase("Entering readiness gate (16 validators full conditions, observers advisory)")
    gate_start = time.time()
    warnings: list[str] = []
    warned_slow: set[int] = set()

    while True:
        elapsed = time.time() - gate_start
        if elapsed > READINESS_TIMEOUT_S:
            raise ExperimentError(f"Readiness timeout reached ({READINESS_TIMEOUT_S}s)")

        rows: list[dict[str, Any]] = []
        heights: dict[int, int] = {}
        for i in range(N_VALIDATORS):
            port = VALIDATOR_RPC_BASE + i
            st = fetch_status(port)
            mt = fetch_metrics(port)
            c1 = st is not None
            c2 = bool(c1 and st["sync_info"].get("syncing") is False)
            h = int(st["sync_info"].get("latest_block_height", 0)) if c1 else 0
            c3 = h >= 10
            heights[i] = h
            if c3 and elapsed > READINESS_SLOW_WARNING_S and i not in warned_slow:
                msg = f"WARNING: node{i} reached height>=10 after {elapsed:.1f}s"
                warnings.append(msg)
                info(msg)
                warned_slow.add(i)
            cnt = metrics_chunk_tx_total(mt) if mt is not None else 0
            rows.append({"i": i, "c1": c1, "c2": c2, "c3": c3, "h": h, "cnt": cnt})

        spread_ok = (max(heights.values()) - min(heights.values()) <= 3) if heights else False

        observer_status: list[dict[str, Any]] = []
        for j in range(N_OBSERVERS):
            ok = fetch_status(OBSERVER_RPC_BASE + j) is not None
            observer_status.append({"j": j, "http": ok})

        info("Validator readiness:")
        print("node c1_http c2_not_sync c3_height c4_agree height tx_count")
        for r in rows:
            print(
                f"{r['i']:>4} {str(r['c1']):>7} {str(r['c2']):>11} {str(r['c3']):>9} "
                f"{str(spread_ok):>8} {r['h']:>6} {r['cnt']:>8}"
            )
        info(
            "Observer HTTP (advisory): "
            + ", ".join(f"observer{o['j']}={o['http']}" for o in observer_status)
        )

        all_pass = all(r["c1"] and r["c2"] and r["c3"] and spread_ok for r in rows)
        if all_pass:
            elapsed_total = time.time() - launched_at
            info(f"All {N_VALIDATORS} validators ready. Stabilizing for {STABILIZATION_S}s...")
            time.sleep(STABILIZATION_S)
            return elapsed_total, warnings

        time.sleep(READINESS_POLL_INTERVAL_S)


def launch_external_submitters(
    paths: Paths,
    accounts_per_shard: int,
    target_tps: int,
    assignment: dict[int, int],
    process_state: ObsProcessState,
    skip_initial_nonce_query: bool = False,
) -> None:
    """Launches 8 external_tx_submitter.py processes; each submitter k drives shards 2k..2k+1.

    `assignment` maps node index -> chunk-producer shard id. We invert it to find
    the validator RPC port that produces chunks for each shard, so each submitter
    submits to the validator that includes its shard's transactions.
    """
    phase("Launching 8 external tx submitters")
    submitter_path = _SCRIPT_DIR / "external_tx_submitter.py"
    shard_to_port: dict[int, int] = {
        shard: VALIDATOR_RPC_BASE + node for node, shard in assignment.items()
    }
    missing = [s for s in range(N_SHARDS) if s not in shard_to_port]
    if missing:
        raise ExperimentError(
            f"Cannot resolve validator port for shards {missing}; "
            f"chunk-producer assignment incomplete: {assignment}"
        )

    paths.results_dir.mkdir(parents=True, exist_ok=True)
    for k in range(N_OBSERVERS):
        shard_a = 2 * k
        shard_b = 2 * k + 1
        ports = f"{shard_to_port[shard_a]},{shard_to_port[shard_b]}"
        dirs = f"{paths.accounts_base / f'shard{shard_a}'},{paths.accounts_base / f'shard{shard_b}'}"
        log_path = paths.results_dir / f"submitter_{k}.log"
        log_f = log_path.open("w", encoding="utf-8")
        cmd = [
            sys.executable, "-u", str(submitter_path),
            "--validator-ports", ports,
            "--shard-dirs", dirs,
            "--accounts-per-shard", str(accounts_per_shard),
            "--target-tps", str(target_tps),
            "--submitter-id", str(k),
        ]
        if skip_initial_nonce_query:
            cmd.append("--skip-initial-nonce-query")
        info(f"submitter{k}: shards [{shard_a},{shard_b}] -> ports [{ports}]")
        p = subprocess.Popen(
            cmd, stdout=log_f, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid, text=True,
        )
        process_state.submitters.append(p)


def cleanup_submitters(process_state: ObsProcessState) -> None:
    """Best-effort SIGTERM of submitter process groups, then SIGKILL."""
    for p in process_state.submitters:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    time.sleep(1)
    for p in process_state.submitters:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def measure_run_observer(
    duration_s: int,
    ramp_up_s: int,
    warnings: list[str],
) -> dict[str, Any]:
    """Measurement window: counts near_chunk_transactions_total delta on validator ports."""
    phase(f"Submitter ramp-up: waiting {ramp_up_s}s before measurement")
    time.sleep(ramp_up_s)
    phase(f"Starting measurement window ({duration_s}s)")
    finality_samples: list[float] = []
    elapsed = 0.0
    counts_start: dict[int, int] = {}
    counts_end: dict[int, int] = {}
    h1: dict[int, int] = {}
    h2: dict[int, int] = {}
    processed_start: dict[int, tuple[int, int, int]] = {}
    processed_end: dict[int, tuple[int, int, int]] = {}

    for attempt in range(MAX_MEASUREMENT_RETRIES + 1):
        t1 = time.time()
        h1 = {}
        counts_start = {}
        processed_start = {}
        with ThreadPoolExecutor(max_workers=N_VALIDATORS) as ex:
            futs = {ex.submit(fetch_shard_snapshot, i, HTTP_TIMEOUT_S): i for i in range(N_VALIDATORS)}
            for f in as_completed(futs):
                snap = f.result()
                i = snap["shard"]
                h1[i] = snap["height"] if snap["status_ok"] else 0
                counts_start[i] = snap["chunk_tx"]
                processed_start[i] = snap["processed"]
        for i in range(N_VALIDATORS):
            info(f"counts_start node{i}: {counts_start[i]} processed={processed_start[i]}")

        finality_samples = []
        port0 = VALIDATOR_RPC_BASE
        s0 = _sample_finality_lag(port0)
        if s0 is not None:
            finality_samples.append(s0)
        remaining = duration_s
        while remaining > 0:
            chunk = min(FINALITY_SAMPLE_INTERVAL_S, remaining)
            time.sleep(chunk)
            remaining -= chunk
            s = _sample_finality_lag(port0)
            if s is not None:
                finality_samples.append(s)

        h2 = {}
        counts_end = {}
        processed_end = {}
        with ThreadPoolExecutor(max_workers=N_VALIDATORS) as ex:
            futs = {ex.submit(fetch_shard_snapshot, i, 30): i for i in range(N_VALIDATORS)}
            for f in as_completed(futs):
                snap = f.result()
                i = snap["shard"]
                h2[i] = snap["height"] if snap["status_ok"] else h1[i]
                counts_end[i] = snap["chunk_tx"]
                processed_end[i] = snap["processed"]
        t2 = time.time()
        elapsed = t2 - t1

        if measurement_is_valid(counts_start, counts_end, h1, h2, N_VALIDATORS):
            break
        if attempt < MAX_MEASUREMENT_RETRIES:
            info(
                f"WARNING: measurement attempt {attempt + 1} stalled; "
                f"waiting {MEASUREMENT_RETRY_WAIT_S}s and retrying..."
            )
            time.sleep(MEASUREMENT_RETRY_WAIT_S)
        else:
            info("WARNING: all measurement attempts stalled. Reporting as-is.")

    bal_warns, balance = verify_chunk_production_balance(N_VALIDATORS, counts_start, counts_end, elapsed)
    warnings.extend(bal_warns)

    tps_per_node: dict[int, float] = {}
    block_time_per_node: dict[int, float | None] = {}
    for i in range(N_VALIDATORS):
        d = counts_end[i] - counts_start[i]
        tps_per_node[i] = d / elapsed if elapsed > 0 else 0.0
        info(f"counts_end node{i}: {counts_end[i]} delta={d} tps={tps_per_node[i]:.2f}")
        dh = h2[i] - h1[i]
        info(f"block height snapshot: node{i} H1={h1[i]} H2={h2[i]} delta={dh} elapsed={elapsed:.1f}s")
        block_time_per_node[i] = (elapsed * 1000 / dh) if dh > 0 else None

    exec_rate: dict[int, float] = {}
    for i in range(N_VALIDATORS):
        ts, fs, ss = processed_start.get(i, (0, 0, 0))
        te, fe, se = processed_end.get(i, (0, 0, 0))
        tot = te - ts
        suc = se - ss
        fail = fe - fs
        rate = (suc / tot) if tot > 0 else 1.0
        exec_rate[i] = rate
        if fail > 0:
            warnings.append(f"WARNING: node{i} had {fail} failed transactions (rate={rate:.4f})")
        info(f"node{i} processed: total={tot} failed={fail} successful={suc} execution_rate={rate:.4f}")

    avg_rate = sum(exec_rate.values()) / N_VALIDATORS
    aggregate = sum(tps_per_node.values())
    avg_tps = aggregate / N_VALIDATORS
    valid_bts = [x for x in block_time_per_node.values() if x is not None]
    avg_block_ms = float(mean(valid_bts)) if valid_bts else 0.0
    fl_blocks = float(mean(finality_samples)) if finality_samples else None
    fl_ms = (fl_blocks * avg_block_ms) if (fl_blocks is not None and avg_block_ms) else None

    return {
        "tps_per_shard": tps_per_node,
        "aggregate_tps": aggregate,
        "avg_tps_per_shard": avg_tps,
        "counts_start": counts_start,
        "counts_end": counts_end,
        "elapsed_s": elapsed,
        "chunk_production_balance": balance,
        "block_heights_start": h1,
        "block_heights_end": h2,
        "block_time_ms_per_shard": block_time_per_node,
        "avg_block_time_ms": avg_block_ms,
        "finality_lag_blocks_avg": fl_blocks,
        "finality_lag_ms_avg": fl_ms,
        "finality_lag_sample_count": len(finality_samples),
        "execution_rate_avg": avg_rate,
        "execution_rate_per_shard": exec_rate,
    }


def write_results_observer(
    paths: Paths,
    readiness_s: float,
    measurement: dict[str, Any],
    warnings: list[str],
    wall_clock_s: float,
) -> None:
    """Writes JSON detail and a human-readable summary."""
    phase("Writing observer scaling result artifacts")
    paths.results_dir.mkdir(parents=True, exist_ok=True)

    detail = {
        "experiment": "observer_16v_8obs",
        "timestamp": utc_iso(),
        "n_validators": N_VALIDATORS,
        "n_observers": N_OBSERVERS,
        "n_shards": N_SHARDS,
        "readiness_time_s": readiness_s,
        "wall_clock_s_total": wall_clock_s,
        "measurement": {
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
            "finality_lag_blocks_avg": measurement["finality_lag_blocks_avg"],
            "finality_lag_ms_avg": measurement["finality_lag_ms_avg"],
            "finality_lag_sample_count": measurement["finality_lag_sample_count"],
            "execution_rate_per_shard": {
                str(k): v for k, v in measurement["execution_rate_per_shard"].items()
            },
            "execution_rate_avg": measurement["execution_rate_avg"],
        },
        "warnings": warnings,
    }
    json_path = paths.results_dir / "observer_16v_8obs_results.json"
    json_path.write_text(json.dumps(detail, indent=2), encoding="utf-8")

    lines = [
        "Observer scaling summary (16 validators, 8 observers, 16 shards)",
        f"Timestamp: {utc_iso()}",
        f"readiness_time_s: {readiness_s:.1f}",
        f"measurement_elapsed_s: {measurement['elapsed_s']:.1f}",
        f"wall_clock_s_total: {wall_clock_s:.1f}",
        "",
        "Per-validator (per-shard) TPS:",
    ]
    for i in range(N_VALIDATORS):
        lines.append(f"  node{i}: {measurement['tps_per_shard'][i]:.2f}")
    lines += [
        "",
        f"Aggregate TPS: {measurement['aggregate_tps']:.2f}",
        f"Avg TPS per shard: {measurement['avg_tps_per_shard']:.2f}",
        f"Avg block time (ms): {measurement['avg_block_time_ms']:.2f}",
    ]
    fl_b = measurement["finality_lag_blocks_avg"]
    fl_ms = measurement["finality_lag_ms_avg"]
    if fl_b is not None:
        lines.append(f"Finality lag blocks: {fl_b:.2f}")
    if fl_ms is not None:
        lines.append(f"Finality lag ms: {fl_ms:.1f}")
    lines.append(f"Execution rate avg: {measurement['execution_rate_avg']:.3f}")
    if warnings:
        lines += ["", "Warnings:"] + [f"  - {w}" for w in warnings]
    summary = "\n".join(lines) + "\n"
    summary_path = paths.results_dir / "observer_16v_8obs_summary.txt"
    summary_path.write_text(summary, encoding="utf-8")
    print(summary)


def parse_args_observer() -> argparse.Namespace:
    """Parses CLI args for the observer experiment."""
    p = argparse.ArgumentParser(description="16-validator + 8-observer NEAR benchmark")
    p.add_argument("--storage", choices=["hdd"], default="hdd")
    p.add_argument("--accounts", type=int, default=5000)
    p.add_argument("--duration", type=int, default=180)
    p.add_argument("--target-tps", type=int, default=625,
                   help="Per-submitter target TPS (8 submitters × 625 = 5000 total)")
    p.add_argument("--ramp-up", type=int, default=30,
                   help="Seconds after submitters start before measurement window")
    p.add_argument("--skip-setup", action="store_true",
                   help="Reuse existing keys/genesis/configs when present")
    p.add_argument("--dry-run", action="store_true",
                   help="Print planned actions and exit before launching neard")
    p.add_argument("--skip-initial-nonce-query", action="store_true",
                   help="Pass --skip-initial-nonce-query to each external tx submitter")
    return p.parse_args()


def run_experiment_observer() -> None:
    """End-to-end: preflight, build, launch, gate, submitters, measure, write."""
    start = time.time()
    args = parse_args_observer()
    home = Path.home()
    storage = args.storage
    paths = Paths(
        home=home,
        neard_bin=REPO_ROOT / "nearcore" / "target" / "release" / "neard",
        config_template=home / "bench" / "config.json",
        genesis_template=home / "bench" / "genesis.json",
        accounts_base=home / "bench" / "singlenode" / "accounts",
        run_base=home / "bench" / "singlenode" / "observer_16v_8obs",
        results_dir=REPO_ROOT / "results" / "single_node_scaling" / storage,
        monitor_out=(
            REPO_ROOT
            / "results"
            / "single_node_scaling"
            / storage
            / "monitor_observer_16v_8obs.json"
        ),
    )

    preflight_observer(paths)

    if args.dry_run:
        phase("Dry-run plan (no neard / submitter processes will be spawned)")
        info(f"run_base: {paths.run_base}")
        info(f"results_dir: {paths.results_dir}")
        info(f"monitor_out: {paths.monitor_out}")
        info(f"validators: 16 nodes (node0..node15) RPC 3030..3045 P2P 24567..24582")
        info(f"observers : 8 nodes (observer0..observer7) RPC 3046..3053 P2P 24583..24590")
        info("validator NUMA: numactl --physcpubind=i,i+24 --membind=i%2 (i=0..15)")
        info("observer NUMA : numactl --physcpubind=i+16,i+40 --membind=i%2 (i=0..7)")
        info(f"genesis: shards={N_SHARDS} epoch_length=500 gas_limit=30T "
             f"shuffle=False transaction_validity_period=10000")
        info(f"submitters: {N_OBSERVERS} processes, --target-tps {args.target_tps} each "
             f"({N_OBSERVERS * args.target_tps} aggregate), 2 shards each")
        info(f"--skip-initial-nonce-query: {args.skip_initial_nonce_query}")
        info(f"measurement: ramp_up={args.ramp_up}s window={args.duration}s")
        info(f"Run: python3 scripts/monitor.py --shards {N_VALIDATORS} "
             f"--output {paths.monitor_out} --wait")
        info("Dry-run complete — no processes started.")
        return

    process_state = ObsProcessState(neard_processes=[])

    def _handler(signum: int, frame: Any) -> None:  # noqa: ARG001
        info("Interrupted — cleaning up")
        try:
            cleanup_submitters(process_state)
            cleanup_processes(process_state, dry_run=False)  # type: ignore[arg-type]
        finally:
            raise SystemExit(1)

    signal.signal(signal.SIGINT, _handler)

    warnings: list[str] = []
    measurement: dict[str, Any] | None = None
    readiness_s = 0.0

    try:
        ensure_accounts_observer(paths, args.accounts)
        val_keys, obs_keys = ensure_node_keys(paths, paths.run_base, args.skip_setup)
        genesis = build_genesis_observer(paths, args.accounts, val_keys, obs_keys)
        write_genesis_observer(paths.run_base, genesis)
        generate_configs_observer(paths, paths.run_base, val_keys, obs_keys)

        phase("Pre-launch cleanup")
        safe_pkill_neard()
        time.sleep(3)
        wipe_data_dirs_observer(paths.run_base)

        info(f"Monitor output path: {paths.monitor_out}")
        info(
            f"Run: python3 scripts/monitor.py --shards {N_VALIDATORS} "
            f"--output {paths.monitor_out} --wait"
        )

        launch_validators_then_observers(paths, paths.run_base, process_state)
        readiness_s, gate_warnings = readiness_gate_observer(start)
        warnings.extend(gate_warnings)

        assignment = verify_chunk_producer_shard_assignment(N_VALIDATORS)
        if len(assignment) < N_VALIDATORS:
            raise ExperimentError(
                f"Could not resolve full chunk-producer assignment: "
                f"got {len(assignment)}/{N_VALIDATORS}"
            )

        launch_external_submitters(
            paths, args.accounts, args.target_tps, assignment, process_state,
            skip_initial_nonce_query=args.skip_initial_nonce_query,
        )
        measurement = measure_run_observer(args.duration, args.ramp_up, warnings)
    finally:
        cleanup_submitters(process_state)
        cleanup_processes(process_state, dry_run=False)  # type: ignore[arg-type]

    if measurement is not None:
        write_results_observer(paths, readiness_s, measurement, warnings, time.time() - start)


def main() -> None:
    """CLI entrypoint."""
    try:
        run_experiment_observer()
    except ExperimentError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
