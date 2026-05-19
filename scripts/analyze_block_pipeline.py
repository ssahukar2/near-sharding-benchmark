#!/usr/bin/env python3
"""Block & chunk timing pipeline analysis across N ∈ {1,2,4,8,16,24}.

Decomposes the per-chunk and per-block timeline using **Δsum / Δcount** across
a steady-state window — i.e. the mean wall-clock cost of each event *during*
the measurement window, immune to warm-up bias in cumulative `mean_ms` values.

Pipeline stages tracked (per chunk):
    PRODUCE      near_produce_chunk_time
    PROD+DIST    near_produce_and_distribute_chunk_time
                 → "distribute" = (prod+dist) − produce
    WITNESS_ENC  near_chunk_state_witness_encode_time
    APPLY_DELAY  near_apply_chunk_delay_seconds   (pre-apply queueing/wait)
    APPLY        near_applying_chunks_time
    APPLY_ALL    near_apply_all_chunks_time       (sum over shards on a validator)
    WITNESS_DEC  near_chunk_state_witness_decode_time

Per block (head-of-chain):
    BLOCK_PROC   near_block_processing_time

Plus rate-derived signals: chunk skip ratio, endorsement throughput,
delayed-receipt queue growth.
"""
from __future__ import annotations
import argparse, json, math, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NS = [1, 2, 4, 8, 16, 24]
STEADY_FRACTION = 0.4   # discard first 40% of samples


# ------------------------------------------------------------------ helpers

def load(n):
    return json.loads((ROOT / f"monitor_S{n:02d}.json").read_text())

def steady(samples):
    n = len(samples)
    start = int(math.ceil(n * STEADY_FRACTION))
    return [s for s in samples[start:]
            if any(s.get("shard_metrics", {}).get(str(k))
                   for k in s.get("shard_metrics", {}))]

def _delta_event_per_shard(samples, ns, hist, scale=1.0):
    """Per-shard Δsum/Δcount values, multiplied by `scale`. Skips samples
    where the metric is present-but-None (i.e. monitor.py recorded the key
    but neard did not emit a value for it)."""
    out = []
    for s in range(ns):
        first = last = None
        for sample in samples:
            sm = sample.get("shard_metrics", {}).get(str(s)) or {}
            sval = sm.get(f"{hist}_sum")
            cval = sm.get(f"{hist}_count")
            if sval is None or cval is None:
                continue
            if first is None:
                first = (sval, cval)
            last = (sval, cval)
        if first and last and last[1] - first[1] > 0:
            out.append(scale * (last[0] - first[0]) / (last[1] - first[1]))
    return out

def _summarize(per_shard):
    if not per_shard:
        return None
    m = statistics.fmean(per_shard)
    return {
        "mean": m,
        "min": min(per_shard),
        "max": max(per_shard),
        "cv":  statistics.pstdev(per_shard) / m if m > 0 else 0.0,
        "values": per_shard,
    }

def delta_event_mean(samples, ns, hist):
    """Δsum/Δcount in milliseconds (assumes hist values are seconds)."""
    return _summarize(_delta_event_per_shard(samples, ns, hist, scale=1000.0))

def delta_event_mean_raw(samples, ns, hist):
    """Δsum/Δcount in the histogram's native unit (no ×1000)."""
    return _summarize(_delta_event_per_shard(samples, ns, hist, scale=1.0))

def delta_event_count_per_s(samples, ns, hist):
    """Total Δcount across shards per second of the steady window."""
    if len(samples) < 2: return 0.0
    win = samples[-1]["timestamp"] - samples[0]["timestamp"]
    if win <= 0: return 0.0
    total = 0.0
    for s in range(ns):
        first = last = None
        for sample in samples:
            sm = sample.get("shard_metrics", {}).get(str(s)) or {}
            v = sm.get(f"{hist}_count")
            if v is not None:
                if first is None: first = v
                last = v
        if first is not None and last is not None:
            total += last - first
    return total / win

def delta_event_sum_per_s(samples, ns, hist, scale=1.0):
    """Total Δsum across shards per second (e.g. bytes/s, gas/s)."""
    if len(samples) < 2: return 0.0
    win = samples[-1]["timestamp"] - samples[0]["timestamp"]
    if win <= 0: return 0.0
    total = 0.0
    for s in range(ns):
        first = last = None
        for sample in samples:
            sm = sample.get("shard_metrics", {}).get(str(s)) or {}
            v = sm.get(f"{hist}_sum")
            if v is not None:
                if first is None: first = v
                last = v
        if first is not None and last is not None:
            total += last - first
    return scale * total / win

def delta_count_per_s(samples, ns, name):
    """Total Δ across shards per second of steady window."""
    if len(samples) < 2: return 0.0
    win = samples[-1]["timestamp"] - samples[0]["timestamp"]
    if win <= 0: return 0.0
    total = 0.0
    for s in range(ns):
        first = last = None
        for sample in samples:
            v = sample.get("shard_metrics", {}).get(str(s), {}).get(name)
            if v is not None:
                if first is None: first = v
                last = v
        if first is not None and last is not None:
            total += last - first
    return total / win

def gauge_delta(samples, ns, name):
    """Last - first across the steady window, summed over shards."""
    total_first = total_last = 0.0
    for s in range(ns):
        first = last = None
        for sample in samples:
            v = sample.get("shard_metrics", {}).get(str(s), {}).get(name)
            if v is not None:
                if first is None: first = v
                last = v
        if first is not None and last is not None:
            total_first += first
            total_last += last
    return total_last - total_first, total_first, total_last


# ------------------------------------------------------------------ analysis

def collect(mon: dict) -> tuple[int, dict] | None:
    """Build the per-run record dict for one parsed monitor JSON."""
    st = steady(mon["samples"])
    if not st:
        return None
    ns = mon["n_shards"]
    win = st[-1]["timestamp"] - st[0]["timestamp"]
    rec = {"n": ns, "ns": ns, "window_s": win, "steady_samples": len(st)}
    for h in ["near_produce_chunk_time",
              "near_produce_and_distribute_chunk_time",
              "near_chunk_state_witness_encode_time",
              "near_chunk_state_witness_decode_time",
              "near_apply_chunk_delay_seconds",
              "near_applying_chunks_time",
              "near_apply_all_chunks_time",
              "near_block_processing_time"]:
        rec[h] = delta_event_mean(st, ns, h)
    rec["chunks_per_s"]    = delta_count_per_s(st, ns, "near_chunks_processed")
    rec["produced_per_s"]  = delta_count_per_s(st, ns, "near_chunk_produced_total")
    rec["skipped_per_s"]   = delta_count_per_s(st, ns, "near_chunk_skipped_total")
    rec["endorse_per_s"]   = delta_count_per_s(st, ns, "near_chunk_endorsements_accepted")
    rec["block_per_s"]     = delta_count_per_s(st, ns, "near_block_produced_total")
    rec["limited_per_s"]   = delta_count_per_s(st, ns, "near_chunk_receipts_limited_by")
    rec["tx_per_s"]        = delta_count_per_s(st, ns, "near_chunk_transactions_total")
    rec["delayed_growth"], rec["delayed_first"], rec["delayed_last"] = gauge_delta(
        st, ns, "near_delayed_receipts_count")

    # Witness payload sizes (raw=uncompressed, total=compressed-on-wire). bytes.
    rec["witness_raw_bytes"]   = delta_event_mean_raw(st, ns, "near_chunk_state_witness_raw_size")
    rec["witness_total_bytes"] = delta_event_mean_raw(st, ns, "near_chunk_state_witness_total_size")
    # Bytes/sec on the loopback per shard producer
    rec["witness_bytes_per_s"] = delta_event_sum_per_s(st, ns, "near_chunk_state_witness_total_size")

    # Receipt compute breakdown — values are already in ms per nearcore source ("as a histogram in ms"),
    # so use the raw extractor (no ×1000).
    rec["inc_receipt_compute"]     = delta_event_mean_raw(st, ns, "near_chunk_inc_receipt_compute")
    rec["local_receipt_compute"]   = delta_event_mean_raw(st, ns, "near_chunk_local_receipt_compute")
    rec["delayed_receipt_compute"] = delta_event_mean_raw(st, ns, "near_chunk_delayed_receipt_compute")
    rec["chunk_compute_ms"]        = delta_event_mean_raw(st, ns, "near_chunk_compute")
    # Tx-only compute (separate from receipt classes), also already in ms
    rec["tx_compute"] = delta_event_mean_raw(st, ns, "near_chunk_tx_compute")

    # Receipt gas breakdown — Tgas per chunk by receipt class (raw histogram = Tgas)
    rec["inc_receipt_tgas"]     = delta_event_mean_raw(st, ns, "near_chunk_inc_receipt_tgas")
    rec["local_receipt_tgas"]   = delta_event_mean_raw(st, ns, "near_chunk_local_receipt_tgas")
    rec["delayed_receipt_tgas"] = delta_event_mean_raw(st, ns, "near_chunk_delayed_receipt_tgas")
    rec["tx_tgas"]              = delta_event_mean_raw(st, ns, "near_chunk_tx_tgas")
    rec["chunk_tgas"]           = delta_event_mean_raw(st, ns, "near_chunk_tgas")

    # Recorded-trie size (proof footprint inside witness)
    rec["recorded_size_bytes"] = delta_event_mean_raw(st, ns, "near_chunk_recorded_size")

    # Stage-2 diagnostic histograms (added to attribute apply_chunk_delay)
    for h in ["near_validate_chunk_with_encoded_merkle_root_time",
              "near_chunk_state_witness_network_roundtrip_time",
              "near_async_message_dequeue_time",
              "near_async_message_processing_time",
              "near_partial_witness_encode_time",
              "near_partial_chunk_time_to_last_part_seconds",
              "near_partial_chunk_time_to_last_receipt_part_seconds",
              "near_partial_chunk_time_to_reconstruct_seconds",
              "near_partial_witness_time_to_last_part"]:
        rec[h] = delta_event_mean(st, ns, h)
    # Orphan witness count (gauge — current value)
    rec["orphan_witness_growth"], rec["orphan_witness_first"], rec["orphan_witness_last"] = gauge_delta(
        st, ns, "near_orphan_chunk_state_witness_total_count")
    return ns, rec


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path,
                    help="Analyze a single monitor_S{NN}.json file (overrides the default sweep). "
                         "Output JSON is written next to it.")
    args = ap.parse_args()

    out: dict[int, dict] = {}
    out_dir = ROOT

    if args.input is not None:
        path = args.input.resolve() if args.input.is_absolute() \
               else (Path.cwd() / args.input).resolve()
        if not path.exists():
            ap.error(f"--input file not found: {path}")
        mon = json.loads(path.read_text())
        result = collect(mon)
        if result is None:
            ap.error(f"--input file has no usable steady-state samples: {path}")
        ns_key, rec = result
        out[ns_key] = rec
        out_dir = path.parent
        print(f"Loaded {path}  (N={ns_key}, samples={rec['steady_samples']}, "
              f"window={rec['window_s']:.1f}s)")
        ns_list = [ns_key]
    else:
        ns_list = NS
        for n in NS:
            try: mon = load(n)
            except FileNotFoundError: continue
            result = collect(mon)
            if result is None: continue
            _, rec = result
            out[n] = rec

    # ─────────────────────────────────────────────────────── print

    p = lambda *a, **k: print(*a, **k)
    head = lambda s: p(f"\n{s}\n" + "=" * len(s))

    head("Pipeline stage decomposition  (per-chunk wall-clock, ms — mean across shards)")
    p(f"{'N':>3} {'produce':>8} {'distribute':>11} {'wit_enc':>8} "
      f"{'apply_delay':>12} {'applying':>9} {'apply_all':>10} "
      f"{'wit_dec':>8} {'block_proc':>11}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        prod    = r["near_produce_chunk_time"]["mean"]
        prod_dist = r["near_produce_and_distribute_chunk_time"]["mean"]
        dist = max(0, prod_dist - prod)
        we = r["near_chunk_state_witness_encode_time"]["mean"]
        wd = r["near_chunk_state_witness_decode_time"]["mean"]
        ad = r["near_apply_chunk_delay_seconds"]["mean"]
        ap = r["near_applying_chunks_time"]["mean"]
        aa = r["near_apply_all_chunks_time"]["mean"]
        bp = r["near_block_processing_time"]["mean"]
        p(f"{n:>3} {prod:>8.2f} {dist:>11.2f} {we:>8.2f} {ad:>12.2f} "
          f"{ap:>9.2f} {aa:>10.2f} {wd:>8.2f} {bp:>11.2f}")

    head("Stage scaling factor relative to N=1  (×)")
    p(f"{'N':>3} {'produce':>8} {'distribute':>11} {'wit_enc':>8} "
      f"{'apply_delay':>12} {'applying':>9} {'block_proc':>11}")
    base = out.get(1)
    if base:
        b_prod = base["near_produce_chunk_time"]["mean"]
        b_dist = max(0.001, base["near_produce_and_distribute_chunk_time"]["mean"]
                            - base["near_produce_chunk_time"]["mean"])
        b_we = base["near_chunk_state_witness_encode_time"]["mean"]
        b_ad = base["near_apply_chunk_delay_seconds"]["mean"]
        b_ap = base["near_applying_chunks_time"]["mean"]
        b_bp = base["near_block_processing_time"]["mean"]
        for n in ns_list:
            if n not in out: continue
            r = out[n]
            prod = r["near_produce_chunk_time"]["mean"] / b_prod
            dist = max(0, r["near_produce_and_distribute_chunk_time"]["mean"]
                           - r["near_produce_chunk_time"]["mean"]) / b_dist
            we = r["near_chunk_state_witness_encode_time"]["mean"] / b_we
            ad = r["near_apply_chunk_delay_seconds"]["mean"] / b_ad
            ap = r["near_applying_chunks_time"]["mean"] / b_ap
            bp = r["near_block_processing_time"]["mean"] / b_bp
            p(f"{n:>3} {prod:>8.2f} {dist:>11.2f} {we:>8.2f} {ad:>12.2f} {ap:>9.2f} {bp:>11.2f}")

    head("Per-shard tail behavior  (max/mean ratio — straggler severity)")
    p(f"{'N':>3} {'produce':>8} {'apply_delay':>12} {'applying':>9} {'block_proc':>11} {'wit_enc':>8}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        def ratio(h):
            v = r[h]
            return v["max"] / v["mean"] if v["mean"] > 0 else 1.0
        p(f"{n:>3} {ratio('near_produce_chunk_time'):>8.2f} "
          f"{ratio('near_apply_chunk_delay_seconds'):>12.2f} "
          f"{ratio('near_applying_chunks_time'):>9.2f} "
          f"{ratio('near_block_processing_time'):>11.2f} "
          f"{ratio('near_chunk_state_witness_encode_time'):>8.2f}")

    head("Block-budget accounting  (Σ stages vs measured block_processing_time, ms)")
    p(f"{'N':>3} {'Σ_stages':>10} {'block_proc':>11} {'overhead':>10} {'%overhead':>10}")
    p(" "*3 + "  (produce + apply_delay + applying)")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        s = (r["near_produce_chunk_time"]["mean"]
             + r["near_apply_chunk_delay_seconds"]["mean"]
             + r["near_applying_chunks_time"]["mean"])
        bp = r["near_block_processing_time"]["mean"]
        ov = bp - s
        p(f"{n:>3} {s:>10.2f} {bp:>11.2f} {ov:>10.2f} {(100*ov/bp if bp else 0):>9.1f}%")

    head("Throughput counters per second  (steady-state)")
    p(f"{'N':>3} {'tx/s':>8} {'chunks_proc/s':>14} {'produced/s':>11} "
      f"{'skipped/s':>11} {'endorse/s':>11} {'block/s':>9} {'limited/s':>11}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        p(f"{n:>3} {r['tx_per_s']:>8.1f} {r['chunks_per_s']:>14.2f} "
          f"{r['produced_per_s']:>11.2f} {r['skipped_per_s']:>11.2f} "
          f"{r['endorse_per_s']:>11.2f} {r['block_per_s']:>9.4f} "
          f"{r['limited_per_s']:>11.2f}")

    head("Skip / production health")
    p(f"{'N':>3} {'skip_ratio':>11} {'endorse/produced':>17} {'expected/s':>12} {'observed/s':>12}")
    p(" "*3 + "  skip = skipped/(skipped+produced); expected ≈ ns/block_time")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        prod = r["produced_per_s"]
        sk = r["skipped_per_s"]
        skip_ratio = sk / (prod + sk) if (prod + sk) > 0 else 0
        ep = r["endorse_per_s"] / prod if prod > 0 else 0
        bp = r["near_block_processing_time"]["mean"] / 1000.0  # s
        expected = r["ns"] / bp if bp > 0 else 0
        p(f"{n:>3} {skip_ratio*100:>10.2f}% {ep:>17.2f} {expected:>12.2f} {prod:>12.2f}")

    head("Delayed-receipts queue (back-pressure)")
    p(f"{'N':>3} {'first_total':>12} {'last_total':>11} {'Δ':>9} {'Δ/s':>8} {'Δ/shard/s':>11}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        d = r["delayed_growth"]
        ds = d / r["window_s"] if r["window_s"] else 0
        p(f"{n:>3} {r['delayed_first']:>12.0f} {r['delayed_last']:>11.0f} "
          f"{d:>9.0f} {ds:>8.2f} {(ds/r['ns']):>11.3f}")

    head("Witness payload  (per-chunk, mean across shards)")
    p(f"{'N':>3} {'raw_KB':>10} {'wire_KB':>10} {'compress':>10} "
      f"{'enc_ms':>8} {'dec_ms':>8} {'wire_MB/s/shard':>17} {'recorded_KB':>13}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        raw_b = (r["witness_raw_bytes"] or {}).get("mean") or 0
        tot_b = (r["witness_total_bytes"] or {}).get("mean") or 0
        ratio = raw_b / tot_b if tot_b > 0 else 0
        we = r["near_chunk_state_witness_encode_time"]["mean"]
        wd = r["near_chunk_state_witness_decode_time"]["mean"]
        bps = r.get("witness_bytes_per_s", 0) / max(1, r["ns"])
        rec_b = (r["recorded_size_bytes"] or {}).get("mean") or 0
        p(f"{n:>3} {raw_b/1024:>10.1f} {tot_b/1024:>10.1f} {ratio:>9.2f}× "
          f"{we:>8.2f} {wd:>8.2f} {bps/(1024*1024):>17.2f} {rec_b/1024:>13.1f}")

    head("Apply-time compute breakdown  (per-chunk, ms — components inside `applying_chunks_time`)")
    p(f"{'N':>3} {'tx_val':>8} {'inc_recpt':>10} {'local_recpt':>12} "
      f"{'delayed_recpt':>14} {'chunk_total':>12} {'applying':>9} {'compute/applying':>17}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        tx  = (r["tx_compute"] or {}).get("mean") or 0
        ic  = (r["inc_receipt_compute"] or {}).get("mean") or 0
        lc  = (r["local_receipt_compute"] or {}).get("mean") or 0
        dc  = (r["delayed_receipt_compute"] or {}).get("mean") or 0
        cc  = (r["chunk_compute_ms"] or {}).get("mean") or 0
        ap = r["near_applying_chunks_time"]["mean"]
        p(f"{n:>3} {tx:>8.2f} {ic:>10.2f} {lc:>12.2f} {dc:>14.2f} "
          f"{cc:>12.2f} {ap:>9.2f} {(cc/ap if ap else 0):>16.1%}")

    head("Stage-2 diagnostics — chunk-pipeline propagation & apply-time decomposition (per-event mean, ms)")
    p(f"{'N':>3} {'validate_chunk':>14} {'witness_RTT':>12} {'async_deq':>10} "
      f"{'async_proc':>11} {'partial_w_enc':>14} {'pchunk_recon':>13} "
      f"{'witness_recon':>14} {'orphan_witΔ/s':>14}")
    p(" "*3 + "  validate_chunk = chunk-validator validation; witness_RTT = loopback witness ack;")
    p(" "*3 + "  pchunk_recon = chunk header→reconstruction; witness_recon = witness 1st part→decode")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        def gm(k):
            v = r.get(k)
            return (v or {}).get("mean") or 0
        owg = r.get("orphan_witness_growth", 0) or 0
        ow_per_s = owg / r["window_s"] if r["window_s"] else 0
        p(f"{n:>3} {gm('near_validate_chunk_with_encoded_merkle_root_time'):>14.2f} "
          f"{gm('near_chunk_state_witness_network_roundtrip_time'):>12.2f} "
          f"{gm('near_async_message_dequeue_time'):>10.2f} "
          f"{gm('near_async_message_processing_time'):>11.2f} "
          f"{gm('near_partial_witness_encode_time'):>14.2f} "
          f"{gm('near_partial_chunk_time_to_reconstruct_seconds'):>13.2f} "
          f"{gm('near_partial_witness_time_to_last_part'):>14.2f} "
          f"{ow_per_s:>14.3f}")

    head("apply_chunk_delay attribution  (does the sum of diagnostic stages explain the 726ms?)")
    p(f"{'N':>3} {'apply_chunk_delay':>17} {'Σdiag_stages':>13} {'unattributed':>13} {'%attributed':>12}")
    p(" "*3 + "  Σ = validate + witness_RTT + async_deq + witness_reconstruct + pchunk_reconstruct")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        def gm(k):
            v = r.get(k)
            return (v or {}).get("mean") or 0
        ssum = (gm('near_validate_chunk_with_encoded_merkle_root_time')
                + gm('near_chunk_state_witness_network_roundtrip_time')
                + gm('near_async_message_dequeue_time')
                + gm('near_partial_witness_time_to_last_part')
                + gm('near_partial_chunk_time_to_reconstruct_seconds'))
        ad = r["near_apply_chunk_delay_seconds"]["mean"]
        p(f"{n:>3} {ad:>17.2f} {ssum:>13.2f} {(ad-ssum):>13.2f} {(100*ssum/ad if ad else 0):>11.2f}%")

    head("Apply-time gas accounting  (Tgas per chunk, mean across shards)")
    p(f"{'N':>3} {'tx_Tgas':>10} {'inc_Tgas':>10} {'local_Tgas':>11} "
      f"{'delayed_Tgas':>13} {'chunk_Tgas':>11}")
    for n in ns_list:
        if n not in out: continue
        r = out[n]
        tx  = (r["tx_tgas"] or {}).get("mean") or 0
        ic  = (r["inc_receipt_tgas"] or {}).get("mean") or 0
        lc  = (r["local_receipt_tgas"] or {}).get("mean") or 0
        dc  = (r["delayed_receipt_tgas"] or {}).get("mean") or 0
        ct  = (r["chunk_tgas"] or {}).get("mean") or 0
        p(f"{n:>3} {tx:>10.2f} {ic:>10.2f} {lc:>11.2f} {dc:>13.2f} {ct:>11.2f}")

    out_path = out_dir / "block_pipeline_insights.json"
    out_path.write_text(json.dumps(
        {k: {kk: vv for kk, vv in v.items() if kk != "values"} for k, v in
         {n: {kk: (vv if not isinstance(vv, dict) else
                    {k2: v2 for k2, v2 in vv.items() if k2 != "values"})
              for kk, vv in r.items()} for n, r in out.items()}.items()},
        indent=2))
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
