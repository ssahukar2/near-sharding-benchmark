import time, os
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey
)

N = 1000
key = Ed25519PrivateKey.generate()
pub = key.public_key()
msg = os.urandom(256)
sig = key.sign(msg)

start = time.perf_counter()
for _ in range(N):
    pub.verify(sig, msg)
elapsed = time.perf_counter() - start

per_sig_us = elapsed / N * 1e6
print(f"{N} verifications in {elapsed*1000:.1f}ms")
print(f"Per signature: {per_sig_us:.1f} µs")
print(f"At 100 sigs/chunk × 4 shards = {100*4*per_sig_us/1e6*1000:.0f}ms per block")
