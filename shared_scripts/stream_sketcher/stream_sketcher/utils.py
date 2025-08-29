
import asyncio
import os
import re
import sys
import time
import logging
from typing import Optional

# Use a generic logger name so the package is not tied to the original WGS use
# case.  Downstream callers can still configure logging however they like, but
# internally we refer to this as ``stream_sketcher``.
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

def is_target_file(filename: str,
                   include_re: str,
                   exclude_re: Optional[str] = None) -> bool:
    """Return ``True`` if ``filename`` matches ``include_re`` and does not
    match ``exclude_re`` (if provided).

    Both regular expressions are expected to be strings as supplied in the
    configuration file.  ``include_re`` is required so callers must be explicit
    about what files they are interested in.
    """
    b = os.path.basename(filename)
    if exclude_re and re.match(exclude_re, b):
        return False
    return re.match(include_re, b) is not None

def shard_subdir_for(filename: str) -> str:
    """Choose a shard directory for a given file.

    Historically the code used the ``wgs.`` prefix to determine a shard.  To
    make this tool usable for arbitrary directory structures we simply take the
    first seven characters of the basename.  This provides a reasonably even
    distribution while remaining deterministic.  Callers may override this
    function if they require different sharding behaviour.
    """
    b = os.path.basename(filename)
    prefix = b[:7]
    return prefix if prefix else "misc"

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
