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
    """Token-bucket rate limiter for requests per second."""
    def __init__(self, requests_per_sec: Optional[float] = None):
        self.requests_per_sec = requests_per_sec
        self.allowance = float(requests_per_sec) if requests_per_sec else None
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def throttle(self):
        if not self.requests_per_sec:
            return
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_check
            self.last_check = now
            self.allowance += elapsed * self.requests_per_sec
            if self.allowance > self.requests_per_sec:
                self.allowance = float(self.requests_per_sec)
            if self.allowance < 1.0:
                wait = (1.0 - self.allowance) / self.requests_per_sec
                await asyncio.sleep(wait)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0
