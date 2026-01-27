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

class EmptyResultError(RuntimeError):
    def __init__(self, message: str, size: int = 0):
        super().__init__(message)
        self.size = size

async def download_accessions(
    session: aiohttp.ClientSession,
    accession_paths: dict[str, str],
    rate: Optional[RequestRateLimiter],
    api_key: Optional[str],
    timeout: int = 3600,
    email: Optional[str] = None,
    tool: str = "accession_sketcher",
) -> tuple[dict[str, str], dict[str, int], dict[str, str]]:
    accessions = list(accession_paths.keys())
    params = {
        "db": "nuccore",
        "id": ",".join(accessions),
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

    accession_map = {acc.split(".")[0]: acc for acc in accessions}
    paths: dict[str, str] = {}
    sizes: dict[str, int] = {acc: 0 for acc in accessions}
    file_handles: dict[str, tuple[str, object]] = {}
    current_acc: Optional[str] = None
    head_bytes = bytearray()

    try:
        async with session.get(EFETCH_URL, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                text_sample = (await resp.text())[:500]
                if resp.status in (429, 503) or "rate limit" in text_sample.lower():
                    raise RateLimitError(text_sample)
                raise RuntimeError(f"efetch status {resp.status}: {text_sample}")

            buffer = b""
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                if not chunk:
                    continue
                if len(head_bytes) < 2048:
                    head_bytes.extend(chunk[: max(0, 2048 - len(head_bytes))])
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    line += b"\n"
                    if line.startswith(b">"):
                        header = line[1:].strip()
                        header_str = header.decode(errors="replace")
                        header_token = header_str.split()[0]
                        header_base = header_token.split(".")[0]
                        current_acc = accession_map.get(header_base)
                        if current_acc and current_acc not in file_handles:
                            tmp_path = accession_paths[current_acc] + ".part"
                            os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
                            fhandle = open(tmp_path, "wb")
                            file_handles[current_acc] = (tmp_path, fhandle)
                            paths[current_acc] = accession_paths[current_acc]
                        if current_acc:
                            file_handles[current_acc][1].write(line)
                            sizes[current_acc] += len(line)
                    else:
                        if current_acc:
                            file_handles[current_acc][1].write(line)
                            sizes[current_acc] += len(line)

            if buffer:
                if len(head_bytes) < 2048:
                    head_bytes.extend(buffer[: max(0, 2048 - len(head_bytes))])
                if current_acc:
                    file_handles[current_acc][1].write(buffer)
                    sizes[current_acc] += len(buffer)
    except Exception:
        for tmp_path, handle in file_handles.values():
            try:
                handle.close()
            except Exception:
                pass
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
        raise

    for acc, (tmp_path, handle) in file_handles.items():
        handle.close()
        os.replace(tmp_path, paths[acc])

    empty_reason = None
    if not paths:
        head_text = head_bytes.decode(errors="replace").strip()
        if "EMPTY RESULT" in head_text.upper() or "FAILURE" in head_text.upper():
            empty_reason = head_text or "EMPTY RESULT"

    missing_reasons = {}
    for acc in accessions:
        if acc not in paths or sizes.get(acc, 0) == 0:
            missing_reasons[acc] = empty_reason or f"EMPTY RESULT for {acc}"

    return paths, sizes, missing_reasons

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
