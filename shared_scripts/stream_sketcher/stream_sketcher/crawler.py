
import asyncio
import aiohttp
import re
from typing import List, Tuple, Optional
from .utils import LOG, is_target_file

LISTING_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)
SUBDIR_RE = re.compile(r'^(?:[A-Z]/|[A-Z0-9]{3}/)$')  # A/ or AAA/

async def fetch_text(session: aiohttp.ClientSession, url: str, timeout: int=120) -> Optional[str]:
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

def partition_dirs_and_files(hrefs: List[str]) -> Tuple[List[str], List[str]]:
    subdirs = []
    files = []
    for h in hrefs:
        if h in ("../", "./"):
            continue
        if SUBDIR_RE.match(h):
            subdirs.append(h)
        elif h.endswith(".gz"):
            files.append(h)
        else:
            pass
    return subdirs, files

async def crawl_wgs(base_url: str, include_re: str, max_concurrency: int = 4, headers: Optional[dict] = None):

    conn = aiohttp.TCPConnector(limit_per_host=max_concurrency, limit=max_concurrency)
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        index_html = await fetch_text(session, base_url + "/")
        if not index_html:
            raise RuntimeError(f"Failed to fetch root listing: {base_url}/")
        hrefs = parse_listing_for_hrefs(index_html)
        subdirs, _ = partition_dirs_and_files(hrefs)
        LOG.info("Found %d subdirs at root.", len(subdirs))
        sem = asyncio.Semaphore(max_concurrency)

        async def crawl_subdir(subdir: str):
            url = f"{base_url}/{subdir}"
            async with sem:
                html = await fetch_text(session, url)
            if not html:
                return []
            hrefs2 = parse_listing_for_hrefs(html)
            _, files = partition_dirs_and_files(hrefs2)
            results = []
            for f in files:
                if is_target_file(f, include_re=include_re):
                    results.append((subdir.strip("/"), f, f"{base_url}/{subdir}{f}", None, None))
            LOG.info("Subdir %s: %d target files.", subdir.strip("/"), len(results))
            return results

        tasks = [asyncio.create_task(crawl_subdir(sd)) for sd in subdirs]
        for task in asyncio.as_completed(tasks):
            for item in await task:
                yield item
