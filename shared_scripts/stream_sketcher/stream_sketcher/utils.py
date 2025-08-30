
import asyncio
import os
import re
import sys
import time
import logging
import hashlib
from typing import Optional

LOG = logging.getLogger("stream_sketcher")

def build_logger(log_path: Optional[str]=None, level: int=logging.INFO):
    LOG.setLevel(level)
    fmt = logging.Formatter("%(asctime)s | %(levelname)7s | %(name)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    LOG.addHandler(sh)
    if log_path:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)
        LOG.addHandler(fh)

def is_target_file(filename: str, include_re: str, exclude_re: Optional[str] = None) -> bool:
    """Return True if filename matches include_re and not exclude_re."""
    name = os.path.basename(filename)
    if exclude_re and re.match(exclude_re, name):
        return False
    return re.match(include_re, name) is not None

def shard_subdir_for(filename: str, modulus: int) -> str:
    """Hash the filename and return a shard bucket as a string."""
    h = hashlib.sha256(filename.encode("utf-8")).hexdigest()
    return str(int(h, 16) % modulus)

class RateLimiter:
    """Token-bucket style rate limiter (bytes per second)."""
    def __init__(self, limit_bps: Optional[int] = None):
        self.limit_bps = limit_bps
        self.allowance = float(limit_bps) if limit_bps else None
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

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
