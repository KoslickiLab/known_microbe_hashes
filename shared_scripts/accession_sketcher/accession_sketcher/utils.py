import asyncio
import hashlib
import logging
import os
import sys
import time
from typing import Optional

LOG = logging.getLogger("accession_sketcher")

def build_logger(log_path: Optional[str] = None, level: int = logging.INFO):
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

def shard_subdir_for(name: str, modulus: int) -> str:
    h = hashlib.sha256(name.encode("utf-8")).hexdigest()
    return str(int(h, 16) % modulus)

class RequestRateLimiter:
    """Strict rate limiter for requests per second."""
    def __init__(self, requests_per_sec: Optional[float] = None):
        self.requests_per_sec = requests_per_sec
        self.interval = (1.0 / requests_per_sec) if requests_per_sec else None
        self.next_allowed = time.monotonic()
        self._lock = asyncio.Lock()

    async def throttle(self):
        if not self.requests_per_sec:
            return
        async with self._lock:
            now = time.monotonic()
            if now < self.next_allowed:
                await asyncio.sleep(self.next_allowed - now)
            self.next_allowed = max(self.next_allowed, now) + self.interval
