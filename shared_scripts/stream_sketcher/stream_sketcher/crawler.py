import asyncio
import aiohttp
import os
import re
from typing import List, Tuple, Optional, AsyncGenerator
from .utils import LOG, is_target_file

LISTING_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)

async def fetch_text(session: aiohttp.ClientSession, url: str, timeout: int = 120) -> Optional[str]:
    """Fetch ``url`` returning the response text or ``None`` on failure."""
    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                LOG.warning("GET %s -> %s", url, resp.status)
                return None
            return await resp.text()
    except Exception as e:  # pragma: no cover - network errors
        LOG.warning("Error fetching %s: %r", url, e)
        return None

def parse_listing_for_hrefs(html: str) -> List[str]:
    return LISTING_HREF_RE.findall(html)

def partition_dirs_and_files(hrefs: List[str]) -> Tuple[List[str], List[str]]:
    """Split a list of hrefs from a directory listing into directories and files."""
    subdirs: List[str] = []
    files: List[str] = []
    for h in hrefs:
        if h in ("../", "./"):
            continue
        if h.startswith('/') or h.startswith('http'):
            # absolute links would cause us to re-crawl from the root
            continue
        if h.endswith('/'):
            subdirs.append(h.rstrip('/'))
        else:
            files.append(h)
        return subdirs, files

async def crawl_ftp(base_url: str,
                    include_re: str,
                    exclude_re: Optional[str] = None,
                    max_depth: Optional[int] = 1,
                    max_concurrency: int = 4,
                    headers: Optional[dict] = None) -> AsyncGenerator[Tuple[str, str, str, Optional[int], Optional[str]], None]:
    """Recursively crawl ``base_url`` yielding files that match ``include_re``.

    ``max_depth`` specifies how many directory levels below ``base_url`` should
    be traversed.  ``None`` means unlimited depth, ``0`` only inspects the root
    directory, ``1`` (the default) inspects direct children, etc.
    """
    base_url = base_url.rstrip('/')
    conn = aiohttp.TCPConnector(limit_per_host=max_concurrency, limit=max_concurrency)
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        sem = asyncio.Semaphore(max_concurrency)

        async def crawl_dir(rel: str, depth: int):
            url = f"{base_url}/{rel}" if rel else base_url
            async with sem:
                html = await fetch_text(session, url + '/')
            if not html:
                return []
            hrefs = parse_listing_for_hrefs(html)
            subdirs, files = partition_dirs_and_files(hrefs)
            results = []
            for f in files:
                if is_target_file(f, include_re, exclude_re):
                    results.append((rel, f, f"{url}/{f}", None, None))
            if max_depth is None or depth < max_depth:
                tasks = [asyncio.create_task(crawl_dir(os.path.join(rel, sd), depth + 1)) for sd in subdirs]
                for task in asyncio.as_completed(tasks):
                    results.extend(await task)
            return results

        top_html = await fetch_text(session, base_url + '/')
        if not top_html:
            raise RuntimeError(f"Failed to fetch root listing: {base_url}/")
        hrefs = parse_listing_for_hrefs(top_html)
        subdirs, files = partition_dirs_and_files(hrefs)
        for f in files:
            if is_target_file(f, include_re, exclude_re):
                yield ('', f, f"{base_url}/{f}", None, None)
        tasks = [asyncio.create_task(crawl_dir(sd, 1)) for sd in subdirs]
        for task in asyncio.as_completed(tasks):
            for item in await task:
                yield item

