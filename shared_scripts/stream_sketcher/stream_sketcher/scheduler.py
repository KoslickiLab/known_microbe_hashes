
import asyncio
import os
import aiohttp
import signal
import logging
from .utils import LOG, build_logger, shard_subdir_for, RateLimiter
from .db import DB
from .crawler import crawl
from .worker import download_file, run_sourmash

DEFAULT_PARAMS = "k=15,k=31,k=33,scaled=1000,noabund"

class Sketcher:
    def __init__(self, config: dict):
        self.cfg = config
        level_name = str(self.cfg.get("log_level", "INFO")).upper()
        level = getattr(logging, level_name, logging.INFO)
        build_logger(self.cfg.get("log_path"), level)
        self.db = DB(self.cfg["state_db"])
        self.stop_flag = False

    async def crawl_and_enqueue(self):
        base_url = self.cfg["base_url"].rstrip("/")
        include_re = self.cfg.get("include_regex")
        exclude_re = self.cfg.get("exclude_regex")
        max_depth = self.cfg.get("max_depth", None)
        crawl_conc = int(self.cfg.get("max_crawl_concurrency", 4))
        headers = {"User-Agent": self.cfg.get("user_agent", "Stream Sketcher/1.0")}
        limit = self.cfg.get("smoke_test_limit")
        dry_run = bool(self.cfg.get("dry_run", False))
        count = 0
        async for subdir, filename, url, size, mtime in crawl(base_url, include_re, exclude_re, max_depth, crawl_conc, headers=headers):
            if dry_run:
                LOG.info("Would enqueue %s", url)
            else:
                self.db.upsert_file(subdir, filename, url, size, mtime)
            count += 1
            if limit and count >= int(limit):
                LOG.info("Reached smoke test limit of %s files", limit)
                break
        LOG.info("Crawl finished with %d files.", count)

    async def worker(self, session: aiohttp.ClientSession, net_sem: asyncio.Semaphore, rate: RateLimiter):
        params = self.cfg.get("sourmash_params", DEFAULT_PARAMS)
        rayon_threads = int(self.cfg.get("sourmash_threads", 1))
        tmp_root = self.cfg["tmp_root"]
        out_root = self.cfg["output_root"]
        retry_max = int(self.cfg.get("max_retries", 6))
        timeout = int(self.cfg.get("request_timeout_seconds", 3600))
        shard_mod = int(self.cfg.get("shard_modulus", 512))

        error_cooldown = int(self.cfg.get("error_retry_cooldown_seconds", 1800))
        error_max_total = int(self.cfg.get("error_max_total_tries", 20))
        idle_cycles = 0
        while not self.stop_flag:
            try:
                claim = self.db.claim_next(error_cooldown, error_max_total)
            except Exception as e:
                LOG.exception("DB error while claiming work: %r", e)
                await asyncio.sleep(2.0)
                continue
            if not claim:
                idle_cycles += 1
                if idle_cycles % 30 == 0:
                    st = self.db.stats()
                    LOG.info("No claimable work yet. by_status=%s", st.get("by_status"))
                await asyncio.sleep(2.0)
                continue
            idle_cycles = 0
            file_id, subdir, filename, url = claim
            LOG.debug("Claimed id=%s subdir=%s file=%s", file_id, subdir, filename)

            rel_dir = shard_subdir_for(filename, shard_mod)
            local_tmp = os.path.join(tmp_root, rel_dir, filename)
            local_out = os.path.join(out_root, rel_dir, filename + ".sig.zip")

            if os.path.exists(local_out):
                self.db.mark_status(file_id, "DONE", out_path=local_out)
                continue

            tries = 0
            while tries <= retry_max and not self.stop_flag:
                try:
                    async with net_sem:
                        await download_file(session, url, local_tmp, rate, timeout=timeout)
                    self.db.mark_status(file_id, "SKETCHING")
                    rc, out = await run_sourmash(local_tmp, local_out, params, rayon_threads, log=LOG)
                    if rc != 0:
                        raise RuntimeError(f"sourmash failed rc={rc}: {out[:500]}")
                    self.db.mark_status(file_id, "DONE", out_path=local_out)
                    try:
                        os.remove(local_tmp)
                    except FileNotFoundError:
                        pass
                    break
                except Exception as e:
                    tries += 1
                    self.db.mark_status(file_id, "ERROR", error=str(e), inc_tries=True)
                    backoff = min(300, (2 ** tries))
                    await asyncio.sleep(backoff)
                    try:
                        if os.path.exists(local_tmp):
                            os.remove(local_tmp)
                    except Exception:
                        pass

    async def run(self):
        # ensure dirs exist
        for p in (self.cfg["output_root"], self.cfg["tmp_root"],
                  os.path.dirname(self.cfg["state_db"]),
                  os.path.dirname(self.cfg.get("log_path", "/tmp/void.log"))):
            if p:
                os.makedirs(p, exist_ok=True)

        if self.cfg.get("dry_run", False):
            await self.crawl_and_enqueue()
            return

        stale = int(self.cfg.get("stale_seconds", 3600))
        self.db.reset_stuck(stale)

        crawl_task = asyncio.create_task(self.crawl_and_enqueue())

        max_dl = int(self.cfg.get("max_concurrent_downloads", 8))
        net_sem = asyncio.Semaphore(max_dl)
        rate_bps = self.cfg.get("rate_limit_bytes_per_sec", None)
        rate = RateLimiter(rate_bps) if rate_bps else None

        conn = aiohttp.TCPConnector(limit_per_host=max_dl, limit=max_dl)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=120, sock_read=3600)

        headers = {"User-Agent": self.cfg.get("user_agent", "Stream Sketcher/1.0")}

        async with aiohttp.ClientSession(connector=conn, timeout=timeout, headers=headers) as session:
            total_workers = int(self.cfg.get("max_total_workers", 96))
            workers = [asyncio.create_task(self.worker(session, net_sem, rate)) for _ in range(total_workers)]
            # Log worker crashes immediately
            for w in workers:
                w.add_done_callback(lambda t: LOG.exception("Worker crashed: %r", t.exception()) if t.exception() else None)

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._request_stop)

            await crawl_task
            LOG.info("Crawler finished; continuing until queue is exhausted...")

            while True:
                st = self.db.stats()
                pending = st["by_status"].get("PENDING", 0) + st["by_status"].get("ERROR", 0) + st["by_status"].get("DOWNLOADING", 0) + st["by_status"].get("SKETCHING", 0)
                if pending == 0:
                    self.stop_flag = True
                    break
                await asyncio.sleep(10)

            await asyncio.gather(*workers, return_exceptions=True)

    def _request_stop(self):
        LOG.warning("Stop requested; will finish current tasks then exit.")
        self.stop_flag = True

def load_config(path: str) -> dict:
    import yaml, os
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("base_url", "https://ftp.ncbi.nlm.nih.gov/genomes/all")
    cfg.setdefault("output_root", "/scratch/genbank_genomes_all/genomes_all_sketches")
    cfg.setdefault("tmp_root", "/scratch/genbank_genomes_all/genomes_all_tmp")
    cfg.setdefault("state_db", "/scratch/genbank_genomes_all/_state/sketcher.sqlite")
    cfg.setdefault("log_path", "/scratch/genbank_genomes_all/logs/sketcher.log")
    cfg.setdefault("include_regex", r".*\.fna\.gz$")
    cfg.setdefault("exclude_regex", None)
    cfg.setdefault("max_crawl_concurrency", 4)
    cfg.setdefault("max_depth", None)
    cfg.setdefault("max_concurrent_downloads", 8)
    cfg.setdefault("max_total_workers", 96)
    cfg.setdefault("rate_limit_bytes_per_sec", None)
    cfg.setdefault("user_agent", "Stream Sketcher/1.0 (+dmk333@psu.edu; admin=dmk333@psu.edu)")
    cfg.setdefault("error_retry_cooldown_seconds", 1800)
    cfg.setdefault("error_max_total_tries", 20)
    cfg.setdefault("stale_seconds", 3600)
    cfg.setdefault("dry_run", True)
    cfg.setdefault("smoke_test_limit", None)
    cfg.setdefault("sourmash_params", DEFAULT_PARAMS)
    cfg.setdefault("sourmash_threads", 1)
    cfg.setdefault("request_timeout_seconds", 3600)
    cfg.setdefault("max_retries", 8)
    cfg.setdefault("shard_modulus", 512)
    cfg.setdefault("log_level", "INFO")
    for key in ("output_root", "tmp_root", "state_db", "log_path"):
        cfg[key] = os.path.abspath(os.path.expanduser(cfg[key]))
    return cfg

async def main_async(cfg_path: str):
    cfg = load_config(cfg_path)
    sk = Sketcher(cfg)
    await sk.run()

def main():
    import argparse, asyncio
    p = argparse.ArgumentParser(description="Stream sourmash sketcher")
    p.add_argument("--config", "-c", required=True, help="Path to YAML config")
    args = p.parse_args()
    asyncio.run(main_async(args.config))

if __name__ == "__main__":
    main()
