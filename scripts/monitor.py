#!/usr/bin/env python3
"""
Resource monitor for NEAR single-node shard scaling experiment.

Run this alongside single_node_scaling.py. It automatically detects
running neard processes, samples CPU/memory/network/disk per process
every SAMPLE_INTERVAL_S seconds, and scrapes RocksDB metrics from
each shard's /metrics endpoint.

Usage:
    python3 scripts/monitor.py --shards N --output /tmp/monitor_SNN.json

The script runs until all neard processes exit or until interrupted.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

import requests

SAMPLE_INTERVAL_S = 5
HTTP_TIMEOUT_S = 5

ROCKSDB_METRICS = [
    "near_rocksdb_num_running_compactions",
    "near_rocksdb_num_running_flushes",
    "near_rocksdb_estimate_pending_compaction_bytes",
    "near_rocksdb_compaction_pending",
    "near_rocksdb_mem_table_flush_pending",
    "near_rocksdb_actual_delayed_write_rate",
    "near_rocksdb_is_write_stopped",
    "near_rocksdb_block_cache_usage",
    "near_rocksdb_cur_size_all_mem_tables",
]

PROCESSED_METRICS = [
    "near_transaction_processed_total",
    "near_transaction_processed_failed_total",
    "near_transaction_processed_successfully_total",
    "near_chunk_transactions_total",
]

TIMING_HISTOGRAM_METRICS = [
    # Block production timing
    "near_block_processing_time",                  # total block processing time (CONFIRMED WORKING)
    "near_produce_chunk_time",                     # time to produce a chunk
    "near_produce_and_distribute_chunk_time",      # chunk production + P2P distribution
    "near_apply_chunk_delay_seconds",              # chunk application delay (transaction execution)
    "near_applying_chunks_time",                   # wall time applying all chunks per block
    "near_apply_all_chunks_time",                  # total wall time to apply all chunks in block

    # Compute time breakdown per chunk
    "near_chunk_tx_compute",                       # compute time for transactions
    "near_chunk_local_receipt_compute",            # compute time for local receipts
    "near_chunk_delayed_receipt_compute",          # compute time for delayed cross-shard receipts
    "near_chunk_inc_receipt_compute",              # compute time for incoming cross-shard receipts
    "near_chunk_yield_timeouts_compute",           # compute time for yield/promise timeouts

    # Gas usage histograms (breakdown by receipt type)
    "near_chunk_tgas",                             # total gas per chunk (teragas)
    "near_chunk_tgas_used_hist",                   # gas used histogram
    "near_chunk_tx_tgas",                          # gas used by transactions specifically
    "near_chunk_delayed_receipt_tgas",             # gas used by delayed cross-shard receipts
    "near_chunk_inc_receipt_tgas",                 # gas used by incoming cross-shard receipts
    "near_chunk_local_receipt_tgas",               # gas used by local receipts

    # Chunk size metrics (effective block size in NEAR sharded model)
    "near_chunk_recorded_size",                    # actual recorded chunk size in bytes
    "near_chunk_recorded_size_upper_bound",        # chunk size ceiling
    "near_chunk_recorded_trie_nodes_values_size",  # trie node data size per chunk

    # State witness overhead (cross-shard communication cost)
    "near_chunk_state_witness_total_size",         # total state witness size
    "near_chunk_state_witness_raw_size",           # raw state witness before encoding
    "near_chunk_state_witness_encode_time",        # time to encode state witness
    "near_chunk_state_witness_decode_time",        # time to decode state witness

    # Stage-2 diagnostic histograms (added to attribute apply_chunk_delay_seconds).
    # near_chunk_state_witness_validation_time is gated behind a build feature in
    # this nearcore release, so we substitute the always-on near_validate_chunk_with_encoded_merkle_root_time
    # and a battery of partial-chunk/partial-witness propagation timers that
    # together account for the time between chunk-received and apply-START.
    "near_chunk_state_witness_network_roundtrip_time",      # loopback witness RTT (CP↔CV)
    "near_async_message_dequeue_time",                      # actor-system queue wait
    "near_async_message_processing_time",                   # actor-system handler wall-clock
    "near_partial_witness_encode_time",                     # partial-chunk witness encode
    "near_validate_chunk_with_encoded_merkle_root_time",    # chunk validation (replacement for validation_time)
    "near_partial_chunk_time_to_last_part_seconds",         # chunk-part gossip wait (header → all owned parts)
    "near_partial_chunk_time_to_last_receipt_part_seconds", # receipt-part gossip wait (header → last needed receipt)
    "near_partial_chunk_time_to_reconstruct_seconds",       # chunk reconstruction wait (header → enough parts)
    "near_partial_witness_time_to_last_part",               # witness reconstruction wait
    "near_partial_encoded_chunk_request_processing_time",   # chunk-fetch RPC handler time
]

SCALAR_GAUGE_METRICS = [
    # Production counters
    "near_block_produced_total",                   # blocks produced (CONFIRMED WORKING)
    "near_chunk_produced_total",                   # chunks produced
    "near_chunks_processed",                       # chunks processed
    "near_chunk_skipped_total",                    # chunks that failed to produce on time

    # Gas and receipts
    "near_gas_used",                               # total gas used (CONFIRMED WORKING)
    "near_delayed_receipts_count",                 # cross-shard delayed receipt queue depth (CONFIRMED WORKING)
    "near_chunk_endorsements_accepted",            # chunk endorsements accepted (approval signal)
    "near_chunk_receipts_limited_by",              # receipt inclusion throttling signal
    "near_orphan_chunk_state_witness_total_count", # witnesses arriving before parent block
    "near_block_producer_missing_endorsement_count",   # missing endorsements per block (substitute for endorsements_rejected)

    # Already in PROCESSED_METRICS — will be skipped by existing dedup logic
    "near_chunk_transactions_total",
]


def get_neard_pids() -> list[int]:
    """Returns PIDs of all running neard processes."""
    try:
        result = subprocess.run(
            ["pgrep", "-x", "neard"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            return [int(p) for p in result.stdout.strip().split() if p]
    except Exception:
        pass
    return []


def sample_pidstat(pids: list[int]) -> dict[int, dict[str, float]]:
    """
    Samples CPU, memory, network, disk for each PID using pidstat.
    Returns dict mapping PID to resource stats.
    """
    if not pids:
        return {}

    pid_str = ",".join(str(p) for p in pids)
    stats: dict[int, dict[str, float]] = {p: {} for p in pids}

    # CPU and memory: pidstat -u -r
    for flag, fields in [
        ("-u", ["cpu_pct"]),
        ("-r", ["minflt_s", "majflt_s", "vsz_kb", "rss_kb", "mem_pct"]),
        ("-d", ["kb_rd_s", "kb_wr_s", "kb_ccwr_s"]),
        ("-n", ["kb_recv_s", "kb_send_s"]),
    ]:
        try:
            result = subprocess.run(
                ["pidstat", flag, "-p", pid_str, "1", "1"],
                capture_output=True, text=True, timeout=15
            )
            for line in result.stdout.splitlines():
                parts = line.split()
                if len(parts) < 4:
                    continue
                # pidstat output: Time UID PID [fields...] Command
                # First field is time string (e.g. "16:23:45"), third is PID
                try:
                    pid = int(parts[2])
                    if pid not in stats:
                        continue
                    if flag == "-u":
                        # %usr %system %guest %wait %CPU CPU Command
                        stats[pid]["cpu_usr_pct"] = float(parts[3])
                        stats[pid]["cpu_sys_pct"] = float(parts[4])
                        stats[pid]["cpu_pct"] = float(parts[7])
                    elif flag == "-r":
                        # minflt/s majflt/s VSZ RSS %MEM Command
                        stats[pid]["vsz_kb"] = float(parts[5])
                        stats[pid]["rss_kb"] = float(parts[6])
                        stats[pid]["mem_pct"] = float(parts[7])
                    elif flag == "-d":
                        # kB_rd/s kB_wr/s kB_ccwr/s iodelay Command
                        stats[pid]["kb_rd_s"] = float(parts[3])
                        stats[pid]["kb_wr_s"] = float(parts[4])
                        stats[pid]["iodelay"] = float(parts[6])
                    elif flag == "-n":
                        # kB_rd/s kB_wr/s Command (network)
                        stats[pid]["kb_recv_s"] = float(parts[3])
                        stats[pid]["kb_send_s"] = float(parts[4])
                except (ValueError, IndexError):
                    continue
        except Exception:
            continue

    return stats


def scrape_shard_metrics(n_shards: int) -> dict[int, dict[str, Any]]:
    """
    Scrapes RocksDB and transaction processed metrics from each shard's
    /metrics endpoint. Returns dict mapping shard index to metrics.
    """
    results: dict[int, dict[str, Any]] = {}

    for i in range(n_shards):
        port = 3030 + i
        shard_metrics: dict[str, Any] = {}
        try:
            resp = requests.get(
                f"http://127.0.0.1:{port}/metrics",
                timeout=HTTP_TIMEOUT_S
            )
            if resp.status_code != 200:
                results[i] = shard_metrics
                continue

            text = resp.text
            # Extract scalar metrics (non-labeled)
            for metric in PROCESSED_METRICS:
                prefix = f"{metric} "
                for line in text.splitlines():
                    if line.startswith(prefix):
                        try:
                            shard_metrics[metric] = float(line.split()[-1])
                        except (ValueError, IndexError):
                            pass
                        break

            # Extract RocksDB metrics — sum across all column families
            for metric in ROCKSDB_METRICS:
                total = 0.0
                found = False
                prefix = f"{metric}{{"
                for line in text.splitlines():
                    if line.startswith(prefix):
                        try:
                            total += float(line.split()[-1])
                            found = True
                        except (ValueError, IndexError):
                            pass
                if found:
                    shard_metrics[metric] = total

            # Histogram timing metrics — sum _sum / _count across all series; optional mean in ms
            for metric in TIMING_HISTOGRAM_METRICS:
                sum_key = f"{metric}_sum"
                count_key = f"{metric}_count"
                mean_key = f"{metric}_mean_ms"
                try:
                    sum_total = 0.0
                    sum_found = False
                    count_total = 0.0
                    count_found = False
                    sum_prefix_plain = f"{sum_key} "
                    sum_prefix_labeled = f"{sum_key}{{"
                    count_prefix_plain = f"{count_key} "
                    count_prefix_labeled = f"{count_key}{{"
                    for line in text.splitlines():
                        if line.startswith(sum_prefix_plain) or line.startswith(
                            sum_prefix_labeled
                        ):
                            try:
                                sum_total += float(line.split()[-1])
                                sum_found = True
                            except (ValueError, IndexError):
                                pass
                        elif line.startswith(count_prefix_plain) or line.startswith(
                            count_prefix_labeled
                        ):
                            try:
                                count_total += float(line.split()[-1])
                                count_found = True
                            except (ValueError, IndexError):
                                pass
                    shard_metrics[sum_key] = sum_total if sum_found else None
                    shard_metrics[count_key] = count_total if count_found else None
                    if sum_found and count_found and count_total > 0:
                        shard_metrics[mean_key] = (sum_total / count_total) * 1000.0
                    else:
                        shard_metrics[mean_key] = None
                except Exception:
                    shard_metrics[sum_key] = None
                    shard_metrics[count_key] = None
                    shard_metrics[mean_key] = None

            # Scalar gauges / counters — unlabeled line or sum of labeled series
            for metric in SCALAR_GAUGE_METRICS:
                try:
                    if (
                        metric == "near_chunk_transactions_total"
                        and metric in shard_metrics
                    ):
                        continue
                    prefix_plain = f"{metric} "
                    found_plain = False
                    plain_val = 0.0
                    for line in text.splitlines():
                        if line.startswith(prefix_plain):
                            try:
                                plain_val = float(line.split()[-1])
                                found_plain = True
                            except (ValueError, IndexError):
                                pass
                            break
                    if found_plain:
                        shard_metrics[metric] = plain_val
                    else:
                        total = 0.0
                        found_labeled = False
                        prefix_labeled = f"{metric}{{"
                        for line in text.splitlines():
                            if line.startswith(prefix_labeled):
                                try:
                                    total += float(line.split()[-1])
                                    found_labeled = True
                                except (ValueError, IndexError):
                                    pass
                        shard_metrics[metric] = total if found_labeled else None
                except Exception:
                    if not (
                        metric == "near_chunk_transactions_total"
                        and metric in shard_metrics
                    ):
                        shard_metrics[metric] = None

        except Exception:
            pass

        results[i] = shard_metrics

    return results


def wait_for_neard(timeout_s: int = 300) -> list[int]:
    """Waits until at least one neard process is running."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        pids = get_neard_pids()
        if pids:
            return pids
        time.sleep(2)
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resource monitor for NEAR shard scaling experiment"
    )
    parser.add_argument(
        "--shards", type=int, required=True,
        help="Number of shards being benchmarked"
    )
    parser.add_argument(
        "--output", type=str, required=True,
        help="Output JSON file path"
    )
    parser.add_argument(
        "--wait", action="store_true",
        help="Wait for neard to start before sampling"
    )
    args = parser.parse_args()

    wait_timeout = 300 if args.wait else 30
    print(f"[monitor] Waiting up to {wait_timeout}s for neard processes...", flush=True)
    pids = wait_for_neard(timeout_s=wait_timeout)
    if not pids:
        print("[monitor] No neard processes found within 300s. Exiting.")
        return

    print(f"[monitor] Found {len(pids)} neard PIDs: {pids}", flush=True)
    print(f"[monitor] Sampling every {SAMPLE_INTERVAL_S}s. Ctrl+C to stop.", flush=True)

    samples: list[dict[str, Any]] = []

    try:
        while True:
            current_pids = get_neard_pids()
            if not current_pids:
                print("[monitor] All neard processes exited. Stopping.", flush=True)
                break

            ts = time.time()
            pid_stats = sample_pidstat(current_pids)
            shard_metrics = scrape_shard_metrics(args.shards)

            sample: dict[str, Any] = {
                "timestamp": ts,
                "pids": current_pids,
                "pid_stats": {str(k): v for k, v in pid_stats.items()},
                "shard_metrics": {str(k): v for k, v in shard_metrics.items()},
            }
            samples.append(sample)

            # Print brief summary
            total_cpu = sum(
                v.get("cpu_pct", 0) for v in pid_stats.values()
            )
            total_rss_gb = sum(
                v.get("rss_kb", 0) for v in pid_stats.values()
            ) / (1024 * 1024)
            total_kb_wr = sum(
                v.get("kb_wr_s", 0) for v in pid_stats.values()
            )
            print(
                f"[monitor] t={int(ts % 10000):05d} "
                f"pids={len(current_pids)} "
                f"cpu={total_cpu:.0f}% "
                f"rss={total_rss_gb:.2f}GB "
                f"disk_wr={total_kb_wr:.0f}KB/s",
                flush=True
            )

            time.sleep(SAMPLE_INTERVAL_S)

    except KeyboardInterrupt:
        print("\n[monitor] Interrupted.", flush=True)

    # Write output
    output = {
        "n_shards": args.shards,
        "sample_interval_s": SAMPLE_INTERVAL_S,
        "sample_count": len(samples),
        "samples": samples,
    }
    Path(args.output).write_text(
        json.dumps(output, indent=2), encoding="utf-8"
    )
    print(f"[monitor] Wrote {len(samples)} samples to {args.output}", flush=True)


if __name__ == "__main__":
    main()
