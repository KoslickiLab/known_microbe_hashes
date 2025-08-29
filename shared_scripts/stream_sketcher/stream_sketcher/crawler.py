import asyncio
import aiohttp
import re
import posixpath
from urllib.parse import urljoin
from typing import List, Optional, AsyncGenerator, Tuple, Set
from .utils import LOG, is_target_file

LISTING_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)


async def fetch_text(session: aiohttp.ClientSession, url: str, timeout: int = 120) -> Optional[str]:
    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                LOG.warning("GET %s -> %s", url, resp.status)
                return None
            return await resp.text()
    except Exception as e:
        LOG.warning("Error fetching %s: %r", url, e)
        return None


def parse_listing_for_hrefs(html: str) -> List[str]:
    return LISTING_HREF_RE.findall(html)


async def crawl(
    base_url: str,
    include_re: str,
    exclude_re: Optional[str] = None,
    max_depth: Optional[int] = None,
    max_concurrency: int = 4,
    headers: Optional[dict] = None,
) -> AsyncGenerator[Tuple[str, str, str, Optional[int], Optional[str]], None]:
    """Generic recursive crawler for FTP directory listings."""

    base_url = base_url.rstrip("/") + "/"
    conn = aiohttp.TCPConnector(limit_per_host=max_concurrency, limit=max_concurrency)
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        dir_queue: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
        file_queue: asyncio.Queue[Optional[Tuple[str, str, str, Optional[int], Optional[str]]]] = asyncio.Queue()
        visited: Set[str] = set()

        await dir_queue.put(("", 0))

        async def dir_worker():
            while True:
                rel_dir, depth = await dir_queue.get()
                url = urljoin(base_url, rel_dir + "/")
                LOG.debug("Listing %s", url)
                html = await fetch_text(session, url)
                if html:
                    hrefs = parse_listing_for_hrefs(html)
                    for h in hrefs:
                        h = h.strip().split("?")[0]
                        if not h or h in ("../", "./") or h.startswith("/") or "://" in h:
                            continue
                        if h.endswith("/"):
                            if max_depth is None or depth < max_depth:
                                child_rel = posixpath.normpath(posixpath.join(rel_dir, h))
                                if child_rel == "." or child_rel in visited:
                                    continue
                                visited.add(child_rel)
                                LOG.debug("Queueing subdir %s", child_rel)
                                await dir_queue.put((child_rel, depth + 1))
                        else:
                            if is_target_file(h, include_re, exclude_re):
                                file_url = urljoin(url, h)
                                LOG.debug("Found file %s", file_url)
                                await file_queue.put((rel_dir, h, file_url, None, None))
                dir_queue.task_done()

        workers = [asyncio.create_task(dir_worker()) for _ in range(max_concurrency)]

        async def finalize():
            await dir_queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await file_queue.put(None)

        fin_task = asyncio.create_task(finalize())

        while True:
            item = await file_queue.get()
            if item is None:
                break
            yield item

        await fin_task

