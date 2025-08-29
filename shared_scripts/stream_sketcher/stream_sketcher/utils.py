
import asyncio
import os
import re
import sys
import time
import logging
from typing import Optional

LOG = logging.getLogger("wgs_sketcher")

INCLUDE_RE_DEFAULT = r"^wgs\.[A-Z0-9]+(?:\.\d+)?\.fsa_nt\.gz$"
EXCLUDE_RE_DEFAULT = r"\.(?:gbff|faa|fsa_aa|faa_aa|aa)\.gz$"

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

def is_target_file(filename: str, include_re: str=INCLUDE_RE_DEFAULT) -> bool:
    return re.match(include_re, os.path.basename(filename)) is not None

def shard_subdir_for(filename: str) -> str:
    """Choose an output shard directory for a given WGS file.
    Mirrors upstream: use first 3 letters after 'wgs.' as subdir.
    """
    b = os.path.basename(filename)
    if b.startswith("wgs."):
        rest = b[4:]
        parts = rest.split(".")
        if parts:
            prefix = parts[0][:3]
            return prefix if prefix else "misc"
    return "misc"

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
