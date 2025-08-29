
import asyncio
import aiohttp
import os
import logging
from typing import Optional, Tuple
from .utils import LOG, RateLimiter

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB

async def download_file(session: aiohttp.ClientSession, url: str, dest_path: str, rate: Optional[RateLimiter], timeout: int=3600) -> None:
    tmp_path = dest_path + ".part"
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    try:
        async with session.get(url, timeout=timeout) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if not chunk:
                        continue
                    if rate:
                        await rate.throttle(len(chunk))
                    f.write(chunk)
        os.replace(tmp_path, dest_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise

async def run_sourmash(input_path: str, output_path: str, params: str, rayon_threads: int=1, extra_env=None, log: Optional[logging.Logger]=None) -> Tuple[int, str]:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    env = os.environ.copy()
    env["RAYON_NUM_THREADS"] = str(rayon_threads)
    if extra_env:
        env.update(extra_env)
    cmd = [
        "sourmash", "sketch", "dna",
        "-p", params,
        "-o", output_path,
        input_path
    ]
    if log:
        log.debug("Running: %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    out, err = await proc.communicate()
    rc = proc.returncode
    out_combined = (out or b"").decode() + (err or b"").decode()
    return rc, out_combined
