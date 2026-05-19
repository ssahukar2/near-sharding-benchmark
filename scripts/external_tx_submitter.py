#!/usr/bin/env python3
"""External NEAR transaction submitter for the observer scaling experiment.

Submits NEAR Transfer transactions at a target TPS to one or more validator
RPC ports. Designed to be launched as N parallel processes (one per observer),
each handling a subset of shards mapped to the validator that produces chunks
for that shard.

Wire format references (verified against ~/nearcore/core/primitives/src/):
- transaction.rs:
    pub enum Transaction { V0(TransactionV0) = 0, V1(TransactionV1) = 1 }
    TransactionV0 { signer_id, public_key, nonce, receiver_id, block_hash, actions }
    SignedTransaction { transaction: Transaction, signature: Signature, ... } -- borsh
      derives in declaration order, so wire = transaction || signature.
- action/mod.rs:
    Action::Transfer(TransferAction) = 3, with TransferAction { deposit: u128 }.

We hand-roll borsh encoding for these few types to keep the dependency surface small.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import random
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


HTTP_TIMEOUT_S = 10
BLOCK_HASH_REFRESH_S = 30
NONCE_QUERY_WORKERS = 32


# -------------------- base58 (no extra deps) --------------------

_B58_ALPHA = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_MAP = {c: i for i, c in enumerate(_B58_ALPHA)}


def b58decode(s: str) -> bytes:
    """Decodes a Bitcoin/NEAR base58 string into bytes."""
    n = 0
    for c in s:
        n = n * 58 + _B58_MAP[c]
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = 0
    for c in s:
        if c == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + body


# -------------------- borsh primitives --------------------

def b_u8(v: int) -> bytes:
    return struct.pack("<B", v)


def b_u32(v: int) -> bytes:
    return struct.pack("<I", v)


def b_u64(v: int) -> bytes:
    return struct.pack("<Q", v)


def b_u128(v: int) -> bytes:
    return v.to_bytes(16, "little")


def b_string(s: str) -> bytes:
    enc = s.encode("utf-8")
    return b_u32(len(enc)) + enc


def encode_pubkey_ed25519(pk32: bytes) -> bytes:
    """PublicKey enum: tag 0 (ED25519) + 32 raw bytes."""
    if len(pk32) != 32:
        raise ValueError(f"ed25519 pubkey must be 32 bytes, got {len(pk32)}")
    return b_u8(0) + pk32


def encode_signature_ed25519(sig64: bytes) -> bytes:
    """Signature enum: tag 0 (ED25519) + 64 raw bytes."""
    if len(sig64) != 64:
        raise ValueError(f"ed25519 signature must be 64 bytes, got {len(sig64)}")
    return b_u8(0) + sig64


def encode_action_transfer(yocto_deposit: int) -> bytes:
    """Action::Transfer is variant 3 with payload TransferAction { deposit: u128 }."""
    return b_u8(3) + b_u128(yocto_deposit)


def encode_transaction_v0(
    *,
    signer_id: str,
    public_key_ed25519_bytes: bytes,
    nonce: int,
    receiver_id: str,
    block_hash32: bytes,
    actions: list[bytes],
) -> bytes:
    """Encodes Transaction::V0(TransactionV0) in NEAR borsh wire format.

    NOTE: V0 has NO leading version byte — only V1 prefixes a `u8(1)` tag.
    See nearcore core/primitives/src/transaction.rs `impl BorshSerialize for Transaction`.
    """
    if len(block_hash32) != 32:
        raise ValueError(f"block_hash must be 32 bytes, got {len(block_hash32)}")
    return (
        b_string(signer_id)
        + encode_pubkey_ed25519(public_key_ed25519_bytes)
        + b_u64(nonce)
        + b_string(receiver_id)
        + block_hash32
        + b_u32(len(actions))
        + b"".join(actions)
    )


# -------------------- key parsing --------------------

def parse_ed25519_secret_key(sk_str: str) -> tuple[bytes, bytes]:
    """Parses 'ed25519:<base58>' secret key. Returns (seed_32, public_32).

    NEAR's secret key format is base58(seed || pub) → 64 bytes after decode.
    Some tools store seed-only (32 bytes); handle both shapes.
    """
    if not sk_str.startswith("ed25519:"):
        raise ValueError(f"Unsupported secret key prefix: {sk_str[:16]}...")
    raw = b58decode(sk_str[len("ed25519:"):])
    if len(raw) == 64:
        return raw[:32], raw[32:]
    if len(raw) == 32:
        priv = Ed25519PrivateKey.from_private_bytes(raw)
        pub = priv.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        return raw, pub
    raise ValueError(f"Unexpected ed25519 secret key length: {len(raw)}")


def parse_ed25519_public_key(pk_str: str) -> bytes:
    """Parses 'ed25519:<base58>' public key into 32 raw bytes."""
    if not pk_str.startswith("ed25519:"):
        raise ValueError(f"Unsupported public key prefix: {pk_str[:16]}...")
    raw = b58decode(pk_str[len("ed25519:"):])
    if len(raw) != 32:
        raise ValueError(f"Unexpected ed25519 public key length: {len(raw)}")
    return raw


# -------------------- RPC --------------------

def _rpc(port: int, method: str, params: Any) -> dict[str, Any]:
    """Minimal JSON-RPC POST against neard."""
    payload = {"jsonrpc": "2.0", "id": "x", "method": method, "params": params}
    resp = requests.post(f"http://127.0.0.1:{port}/", json=payload, timeout=HTTP_TIMEOUT_S)
    resp.raise_for_status()
    return resp.json()


def fetch_block_hash(port: int) -> bytes | None:
    """Fetches latest_block_hash from /status and decodes to raw 32 bytes."""
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/status", timeout=HTTP_TIMEOUT_S)
        resp.raise_for_status()
        h = resp.json()["sync_info"]["latest_block_hash"]
        return b58decode(h)
    except Exception as e:
        print(f"[submitter] WARNING: fetch_block_hash port={port}: {e}", flush=True)
        return None


def query_nonce(port: int, account_id: str, public_key_str: str) -> int | None:
    """Returns the current access-key nonce, or None if the lookup failed."""
    try:
        body = _rpc(port, "query", {
            "request_type": "view_access_key",
            "finality": "final",
            "account_id": account_id,
            "public_key": public_key_str,
        })
        if body.get("error"):
            return None
        result = body.get("result") or {}
        return int(result.get("nonce", 0))
    except Exception:
        return None


def submit_tx_async(port: int, signed_b64: str) -> tuple[bool, str | None]:
    """Submits via broadcast_tx_async; returns (ok, short_error_or_None)."""
    try:
        body = _rpc(port, "broadcast_tx_async", [signed_b64])
        err = body.get("error")
        if err:
            return False, json.dumps(err)[:200]
        return True, None
    except Exception as e:
        return False, str(e)[:200]


# -------------------- account loading --------------------

def load_shard_accounts(shard_dir: Path, n_accounts: int) -> list[dict[str, Any]]:
    """Loads up to `n_accounts` account JSON files from a shard directory.

    Each file is expected to contain {account_id, public_key, secret_key, nonce}.
    """
    files = sorted(shard_dir.glob("*.json"))[:n_accounts]
    if len(files) < n_accounts:
        print(
            f"[submitter] WARNING: only {len(files)} files found in {shard_dir} "
            f"(expected {n_accounts})",
            flush=True,
        )
    accounts: list[dict[str, Any]] = []
    for fp in files:
        d = json.loads(fp.read_text())
        seed, pub = parse_ed25519_secret_key(d["secret_key"])
        accounts.append({
            "account_id": d.get("account_id") or fp.stem,
            "public_key_str": d["public_key"],
            "public_key_bytes": parse_ed25519_public_key(d["public_key"]),
            "signer": Ed25519PrivateKey.from_private_bytes(seed),
            "_pub_check": pub,  # available for sanity-checks if needed
        })
    return accounts


def query_initial_nonces(
    port: int, accounts: list[dict[str, Any]], workers: int = NONCE_QUERY_WORKERS,
) -> list[int]:
    """Queries view_access_key nonce for every account in parallel."""
    nonces = [0] * len(accounts)

    def _q(idx_acct: tuple[int, dict[str, Any]]) -> tuple[int, int | None]:
        idx, acct = idx_acct
        return idx, query_nonce(port, acct["account_id"], acct["public_key_str"])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_q, ia) for ia in enumerate(accounts)]
        for f in as_completed(futs):
            idx, n = f.result()
            nonces[idx] = n if n is not None else 0
    return nonces


# -------------------- signing & submitting loop --------------------

def build_signed_tx_b64(
    *,
    account: dict[str, Any],
    nonce: int,
    receiver_id: str,
    block_hash32: bytes,
    deposit_yocto: int = 1,
) -> str:
    """Builds and base64-encodes a SignedTransaction (Transfer action)."""
    tx = encode_transaction_v0(
        signer_id=account["account_id"],
        public_key_ed25519_bytes=account["public_key_bytes"],
        nonce=nonce,
        receiver_id=receiver_id,
        block_hash32=block_hash32,
        actions=[encode_action_transfer(deposit_yocto)],
    )
    sig = account["signer"].sign(hashlib.sha256(tx).digest())
    signed = tx + encode_signature_ed25519(sig)
    return base64.b64encode(signed).decode("ascii")


def submitter_loop(args: argparse.Namespace, shards: list[dict[str, Any]]) -> None:
    """Round-robins submissions across configured shards at the target TPS."""
    target_tps = max(1, int(args.target_tps))
    interval = 1.0 / target_tps

    bh: bytes | None = None
    while bh is None:
        bh = fetch_block_hash(shards[0]["port"])
        if bh is None:
            time.sleep(2)
    last_bh = time.time()

    rng = random.Random(os.getpid())
    submitted = 0
    failed = 0
    err_samples_logged = 0
    err_samples_max = 3
    last_report = time.time()
    start_t = last_report
    next_t = time.time()

    while True:
        now = time.time()
        if now - last_bh > BLOCK_HASH_REFRESH_S:
            new_bh = fetch_block_hash(shards[0]["port"])
            if new_bh is not None:
                bh = new_bh
            last_bh = now

        s_idx = submitted % len(shards)
        shard = shards[s_idx]
        accts: list[dict[str, Any]] = shard["accounts"]
        nonces: list[int] = shard["nonces"]
        a_idx = (submitted // len(shards)) % len(accts)
        sender = accts[a_idx]
        nonce = nonces[a_idx] + 1

        r_idx = rng.randrange(len(accts))
        if accts[r_idx]["account_id"] == sender["account_id"]:
            r_idx = (r_idx + 1) % len(accts)
        receiver_id = accts[r_idx]["account_id"]

        signed_b64 = build_signed_tx_b64(
            account=sender,
            nonce=nonce,
            receiver_id=receiver_id,
            block_hash32=bh,
        )
        ok, err = submit_tx_async(shard["port"], signed_b64)
        if ok:
            nonces[a_idx] = nonce
            submitted += 1
        else:
            failed += 1
            if err_samples_logged < err_samples_max:
                print(
                    f"[submitter{args.submitter_id}] ERR sample "
                    f"port={shard['port']} signer={sender['account_id']} "
                    f"nonce={nonce} err={err}",
                    flush=True,
                )
                err_samples_logged += 1
            if err and ("nonce" in err.lower() or "Invalid" in err):
                new_n = query_nonce(shard["port"], sender["account_id"], sender["public_key_str"])
                if new_n is not None:
                    nonces[a_idx] = new_n

        next_t += interval
        slack = next_t - time.time()
        if slack > 0:
            time.sleep(slack)
        else:
            next_t = time.time()

        if time.time() - last_report >= 10:
            elapsed = time.time() - start_t
            actual = submitted / elapsed if elapsed > 0 else 0.0
            print(
                f"[submitter{args.submitter_id}] submitted={submitted} failed={failed} "
                f"avg_tps={actual:.1f}",
                flush=True,
            )
            last_report = time.time()


def parse_args() -> argparse.Namespace:
    """Parses CLI args for the submitter."""
    p = argparse.ArgumentParser(description="External NEAR tx submitter (Transfer actions)")
    p.add_argument("--validator-ports", required=True,
                   help="Comma-separated validator RPC ports (one per shard handled).")
    p.add_argument("--shard-dirs", required=True,
                   help="Comma-separated account dirs (one per shard, aligned to ports).")
    p.add_argument("--accounts-per-shard", type=int, default=5000)
    p.add_argument("--target-tps", type=int, default=625,
                   help="Per-process target TPS across all assigned shards combined.")
    p.add_argument("--submitter-id", type=int, default=0)
    p.add_argument(
        "--skip-initial-nonce-query",
        action="store_true",
        help="Assume nonce=0 for all accounts (valid for fresh genesis runs).",
    )
    return p.parse_args()


def main() -> None:
    """CLI entrypoint: load accounts, query nonces, run the submission loop."""
    args = parse_args()
    ports = [int(p) for p in args.validator_ports.split(",")]
    dirs = [Path(d) for d in args.shard_dirs.split(",")]
    if len(ports) != len(dirs):
        raise SystemExit(
            f"--validator-ports and --shard-dirs must align: "
            f"{len(ports)} vs {len(dirs)}"
        )

    shards: list[dict[str, Any]] = []
    for port, d in zip(ports, dirs):
        accts = load_shard_accounts(d, args.accounts_per_shard)
        if args.skip_initial_nonce_query:
            nonces = [0] * len(accts)
            nonce_note = "skipped (assumed 0 from fresh genesis)"
        else:
            nonces = query_initial_nonces(port, accts)
            nonce_note = (
                f"min={min(nonces) if nonces else 0} max={max(nonces) if nonces else 0}"
            )
        shards.append({"port": port, "accounts": accts, "nonces": nonces})
        print(
            f"[submitter{args.submitter_id}] port={port} dir={d}: "
            f"loaded {len(accts)} accounts, nonce_{nonce_note}",
            flush=True,
        )

    print(
        f"[submitter{args.submitter_id}] target_tps={args.target_tps} "
        f"shards={len(shards)}",
        flush=True,
    )
    try:
        submitter_loop(args, shards)
    except KeyboardInterrupt:
        print(f"[submitter{args.submitter_id}] interrupted", flush=True)


if __name__ == "__main__":
    main()
