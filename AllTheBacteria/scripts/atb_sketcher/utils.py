import asyncio
import os
import re
import sys
import time
import logging
import hashlib
from typing import Optional

LOG = logging.getLogger("atb_sketcher")

INCLUDE_RE_DEFAULT = r"\\.fa\\.gz$"

def build_logger(log_path: Optional[str]=None, level: int=logging.INFO):
    LOG.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    handler.setFormatter(fmt)
    LOG.handlers = [handler]
    if log_path:
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            fh = logging.FileHandler(log_path)
            fh.setFormatter(fmt)
            LOG.addHandler(fh)
        except Exception as e:
            LOG.warning("Failed to attach file logger: %r", e)

def is_target_file(name: str, include_re: Optional[str]=None) -> bool:
    pat = include_re or INCLUDE_RE_DEFAULT
    return re.search(pat, name) is not None

def hash_to_bucket(name: str, n_buckets: int) -> int:
    h = hashlib.sha1(name.encode('utf-8')).hexdigest()
    return int(h, 16) % int(n_buckets)

def shard_subdir_for(filename: str, n_buckets: int=1024) -> str:
    """Map an ATB filename to a stable shard directory via SHA1 modulo n_buckets."""
    b = os.path.basename(filename)
    return f"bucket={hash_to_bucket(b, n_buckets):04d}"

class RateLimiter:
    """Token-bucket style rate limiter (bytes per second)."""
    def __init__(self, limit_bps: Optional[int] = None):
        self.limit_bps = limit_bps
        self._lock = asyncio.Lock()
        self.allowance = float(limit_bps or 0)
        self.last_check = time.monotonic()

    async def throttle(self, nbytes: int):
        if not self.limit_bps:
            return
        async with self._lock:
            current = time.monotonic()
            elapsed = current - self.last_check
            self.last_check = current
            self.allowance += elapsed * self.limit_bps
            if self.allowance > self.limit_bps:
                self.allowance = float(self.limit_bps)
            if self.allowance < nbytes:
                needed = (nbytes - self.allowance) / self.limit_bps
                await asyncio.sleep(needed)
                self.allowance = 0.0
            else:
                self.allowance -= nbytes
