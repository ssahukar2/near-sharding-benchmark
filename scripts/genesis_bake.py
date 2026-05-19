#!/usr/bin/env python3
"""
Bake benchmark sub-accounts into genesis and synth-bm user-data (offline).

Account IDs match bench.sh + near-synth-bm create-sub-accounts:
  shard s, index i ->  a{s:02d}_user_{i}.node0
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import base58
from nacl.signing import SigningKey

DEPOSIT_AMOUNT = "9530606018750000000100000000"
CODE_HASH = "11111111111111111111111111111111"
SIGNER_SUFFIX = "node0"


def near_ed25519_public(signing_key: SigningKey) -> str:
    raw = signing_key.verify_key.encode()
    return "ed25519:" + base58.b58encode(raw).decode("ascii")


def near_ed25519_secret(signing_key: SigningKey) -> str:
    # NEAR ED25519SecretKey is 64 bytes (seed || public), matching nearcore / ed25519-dalek keypair bytes.
    raw = signing_key.encode() + signing_key.verify_key.encode()
    return "ed25519:" + base58.b58encode(raw).decode("ascii")


def account_record(account_id: str) -> dict:
    return {
        "Account": {
            "account_id": account_id,
            "account": {
                "amount": DEPOSIT_AMOUNT,
                "locked": "0",
                "code_hash": CODE_HASH,
                "storage_usage": 0,
                "version": "V1",
            },
        }
    }


def access_key_record(account_id: str, public_key: str) -> dict:
    return {
        "AccessKey": {
            "account_id": account_id,
            "public_key": public_key,
            "access_key": {"nonce": 0, "permission": "FullAccess"},
        }
    }


def recalc_total_supply(genesis: dict) -> None:
    """Set genesis total_supply to the sum of every Account record (yoctoNEAR strings)."""
    total = 0
    for rec in genesis.get("records", []):
        acc = rec.get("Account")
        if not acc:
            continue
        inner = acc.get("account", {})
        total += int(inner.get("amount", 0))
        total += int(inner.get("locked", 0))
    genesis["total_supply"] = str(total)


def wipe_node_data(bench_dir: Path) -> list[Path]:
    wiped: list[Path] = []
    for node_home in sorted(bench_dir.glob("node*")):
        if not node_home.is_dir():
            continue
        data_dir = node_home / "data"
        if data_dir.is_dir():
            shutil.rmtree(data_dir)
            data_dir.mkdir(parents=True, exist_ok=True)
            wiped.append(data_dir)
    return wiped


def main() -> int:
    p = argparse.ArgumentParser(description="Genesis + user-data account creation for sharded-bm.")
    p.add_argument("--num-accounts", type=int, required=True, help="Accounts per shard (N).")
    p.add_argument("--num-shards", type=int, required=True, help="Number of shards (e.g. 4).")
    p.add_argument(
        "--bench-dir",
        type=Path,
        default=Path("/home/ubuntu/bench"),
        help="Directory containing node0, node1, ...",
    )
    p.add_argument(
        "--users-data-dir",
        type=Path,
        required=True,
        help="Root user-data dir (e.g. .../sharded-bm/user-data); writes shard<N>/ subdirs.",
    )
    p.add_argument(
        "--skip-wipe-data",
        action="store_true",
        help="Do not remove node*/data (default: wipe after writing genesis).",
    )
    args = p.parse_args()

    if args.num_accounts < 1:
        print("error: --num-accounts must be >= 1", file=sys.stderr)
        return 2
    if args.num_shards < 1:
        print("error: --num-shards must be >= 1", file=sys.stderr)
        return 2

    bench_dir: Path = args.bench_dir.expanduser().resolve()
    users_root: Path = args.users_data_dir.expanduser().resolve()
    genesis_path = bench_dir / "node0" / "genesis.json"
    if not genesis_path.is_file():
        print(f"error: missing genesis: {genesis_path}", file=sys.stderr)
        return 1

    users_root.mkdir(parents=True, exist_ok=True)

    total_files = 0
    records_to_append: list[dict] = []

    for shard in range(args.num_shards):
        prefix = f"a{shard:02d}"
        shard_dir = users_root / f"shard{shard}"
        if shard_dir.exists():
            shutil.rmtree(shard_dir)
        shard_dir.mkdir(parents=True, exist_ok=True)

        for i in range(args.num_accounts):
            account_id = f"{prefix}_user_{i}.{SIGNER_SUFFIX}"
            sk = SigningKey.generate()
            public_key = near_ed25519_public(sk)
            secret_key = near_ed25519_secret(sk)

            user_obj = {
                "account_id": account_id,
                "public_key": public_key,
                "secret_key": secret_key,
                "nonce": 0,
            }
            out_path = shard_dir / f"{account_id}.json"
            out_path.write_text(json.dumps(user_obj, separators=(",", ":")) + "\n", encoding="utf-8")
            total_files += 1

            records_to_append.append(account_record(account_id))
            records_to_append.append(access_key_record(account_id, public_key))

    genesis = json.loads(genesis_path.read_text(encoding="utf-8"))
    if "records" not in genesis or not isinstance(genesis["records"], list):
        print("error: genesis.json missing list field 'records'", file=sys.stderr)
        return 1
    genesis["records"].extend(records_to_append)
    recalc_total_supply(genesis)
    genesis_path.write_text(json.dumps(genesis, separators=(",", ":")) + "\n", encoding="utf-8")

    node_homes = sorted(
        d for d in bench_dir.iterdir() if d.is_dir() and d.name.startswith("node")
    )
    for nh in node_homes:
        if nh.name == "node0":
            continue
        dst = nh / "genesis.json"
        shutil.copy2(genesis_path, dst)

    wiped: list[Path] = []
    if not args.skip_wipe_data:
        wiped = wipe_node_data(bench_dir)

    per_shard = args.num_accounts
    total_accounts = per_shard * args.num_shards
    print("genesis_create_accounts: done")
    print(f"  accounts per shard: {per_shard}")
    print(f"  num_shards:         {args.num_shards}")
    print(f"  total accounts:     {total_accounts}")
    print(f"  user-data JSON:     {total_files} files under {users_root}")
    print(f"  genesis records appended: {len(records_to_append)} ({total_accounts} Account + {total_accounts} AccessKey)")
    print(f"  genesis written:    {genesis_path}")
    print(f"  genesis copied to:  {len(node_homes) - 1} other node home(s)")
    if wiped:
        print(f"  wiped node data:    {len(wiped)} dir(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
