import asyncio
import aiohttp
import os
import logging
import zipfile
from typing import Optional, Tuple
from .utils import LOG, RequestRateLimiter

CHUNK_SIZE = 4 * 1024 * 1024
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

class RateLimitError(RuntimeError):
    pass

async def download_accession(
    session: aiohttp.ClientSession,
    accession: str,
    dest_path: str,
    rate: Optional[RequestRateLimiter],
    api_key: Optional[str],
    timeout: int = 3600,
    email: Optional[str] = None,
    tool: str = "accession_sketcher",
):
    tmp_path = dest_path + ".part"
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    params = {
        "db": "nuccore",
        "id": accession,
        "rettype": "fasta",
        "retmode": "text",
        "tool": tool,
    }
    if api_key:
        params["api_key"] = api_key
    if email:
        params["email"] = email

    if rate:
        await rate.throttle()

    try:
        async with session.get(EFETCH_URL, params=params, timeout=timeout) as resp:
            text_sample = ""
            if resp.status != 200:
                text_sample = (await resp.text())[:500]
                if resp.status in (429, 503) or "rate limit" in text_sample.lower():
                    raise RateLimitError(text_sample)
                raise RuntimeError(f"efetch status {resp.status}: {text_sample}")
            with open(tmp_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if not chunk:
                        continue
                    f.write(chunk)
            os.replace(tmp_path, dest_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise

    with open(dest_path, "rb") as f:
        head = f.read(2048).lstrip()
    if not head.startswith(b">"):
        raise RuntimeError(f"Downloaded file does not look like FASTA for {accession}")

async def run_sourmash(
    input_path: str,
    output_path: str,
    params: str,
    rayon_threads: int = 1,
    extra_env=None,
    log: Optional[logging.Logger] = None,
) -> Tuple[int, str]:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(input_path):
        return 1, f"input file not found: {input_path}"
    env = os.environ.copy()
    env["RAYON_NUM_THREADS"] = str(rayon_threads)
    if extra_env:
        env.update(extra_env)
    cmd = [
        "sourmash",
        "sketch",
        "dna",
        "-p",
        params,
        "-o",
        output_path,
        input_path,
    ]
    if log:
        log.debug("Running: %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    out, err = await proc.communicate()
    rc = proc.returncode
    out_combined = (out or b"").decode() + (err or b"").decode()
    if rc == 0 and not zipfile.is_zipfile(output_path):
        return 1, f"output not a valid zip: {output_path}"
    return rc, out_combined
