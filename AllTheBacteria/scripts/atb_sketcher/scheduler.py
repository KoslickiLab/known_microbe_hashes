import asyncio
import os
import signal
import aiohttp
import yaml
from .utils import LOG, build_logger, shard_subdir_for, RateLimiter
from .db import DB
from .filelist import iter_atb_filelist
from .worker import download_file, run_sourmash

DEFAULT_PARAMS = "k=15,k=31,k=33,scaled=1000,noabund"

class Sketcher:
    def __init__(self, config: dict):
        self.cfg = config
        build_logger(self.cfg.get("log_path"))
        self.db = DB(self.cfg["state_db"])
        self.stop_flag = False

    async def seed_from_filelist(self):
        path = self.cfg["filelist_path"]
        region = self.cfg.get("s3_region", "eu-west-2")
        limit = self.cfg.get("seed_limit", None)
        limit = int(limit) if limit not in (None, "",) else None
        LOG.info("Seeding DB from filelist: %s (region=%s, limit=%s)", path, region, limit)
        added = 0
        for subdir, filename, url, size, mtime in iter_atb_filelist(path, region, limit=limit):
            self.db.upsert_file(subdir, filename, url, size, mtime)
            added += 1
        LOG.info("Seeding complete. %d items added or updated.", added)

    async def worker(self, session: aiohttp.ClientSession, net_sem: asyncio.Semaphore, rate: RateLimiter):
        params = self.cfg.get("sourmash_params", DEFAULT_PARAMS)
        rayon_threads = int(self.cfg.get("sourmash_threads", 1))
        tmp_root = self.cfg["tmp_root"]
        out_root = self.cfg["output_root"]
        retry_max = int(self.cfg.get("max_retries", 6))
        timeout = int(self.cfg.get("request_timeout_seconds", 3600))
        n_buckets = int(self.cfg.get("n_shards", 1024))

        error_cooldown = int(self.cfg.get("error_retry_cooldown_seconds", 1800))
        error_max_total = int(self.cfg.get("error_max_total_tries", 20))
        idle_cycles = 0

        while not self.stop_flag:
            try:
                claim = self.db.claim_next(error_cooldown, error_max_total)
            except Exception as e:
                LOG.exception("DB error while claiming work: %r", e)
                await asyncio.sleep(1.0)
                continue

            if not claim:
                idle_cycles += 1
                if idle_cycles > 20:
                    await asyncio.sleep(2.0)
                else:
                    await asyncio.sleep(0.25)
                continue

            idle_cycles = 0
            file_id, subdir, filename, url = claim

            # Hash-based sharding
            rel_dir = subdir or shard_subdir_for(filename, n_buckets)
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
        # Ensure expected directories exist
        for p in (self.cfg["output_root"], self.cfg["tmp_root"],
                  os.path.dirname(self.cfg["state_db"]),
                  os.path.dirname(self.cfg.get("log_path", "/tmp/void.log"))):
            if p:
                os.makedirs(p, exist_ok=True)

        # Requeue any stuck rows from a prior run
        stale = int(self.cfg.get("stale_seconds", 3600))
        self.db.reset_stuck(stale)

        # Seed database synchronously (local file)
        await self.seed_from_filelist()

        max_dl = int(self.cfg.get("max_concurrent_downloads", 16))
        net_sem = asyncio.Semaphore(max_dl)

        rate = RateLimiter(self.cfg.get("rate_limit_bytes_per_sec"))

        timeout = aiohttp.ClientTimeout(total=self.cfg.get("request_timeout_seconds", 3600))
        headers = {"User-Agent": self.cfg.get("user_agent", "ATB Sketcher/1.0")}

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            workers = int(self.cfg.get("max_total_workers", 128))
            tasks = [asyncio.create_task(self.worker(session, net_sem, rate)) for _ in range(workers)]

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self._signal_stop)
                except NotImplementedError:
                    pass

            try:
                await asyncio.gather(*tasks)
            finally:
                for t in tasks:
                    t.cancel()

    def _signal_stop(self):
        LOG.warning("Stop requested; finishing in-flight tasks.")
        self.stop_flag = True

def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # defaults
    cfg.setdefault("include_regex", None)  # not used; filter happens in filelist reader
    cfg.setdefault("max_concurrent_downloads", 32)
    cfg.setdefault("max_total_workers", max(32, os.cpu_count() or 64))
    cfg.setdefault("sourmash_params", DEFAULT_PARAMS)
    cfg.setdefault("sourmash_threads", max(1, (os.cpu_count() or 64) // 2))
    cfg.setdefault("request_timeout_seconds", 3600)
    cfg.setdefault("max_retries", 6)
    cfg.setdefault("rate_limit_bytes_per_sec", None)
    cfg.setdefault("n_shards", 2048)
    for key in ("filelist_path", "output_root", "tmp_root", "state_db", "log_path"):
        if key not in cfg:
            raise ValueError(f"Missing required config key: {key}")
        if key.endswith("_root") or key.endswith("_db") or key.endswith("_path"):
            cfg[key] = os.path.abspath(os.path.expanduser(cfg[key]))
    return cfg

async def main_async(cfg_path: str):
    cfg = load_config(cfg_path)
    # Set sourmash threads via env for rayon
    os.environ.setdefault("RAYON_NUM_THREADS", str(int(cfg.get("sourmash_threads", 1))))
    sk = Sketcher(cfg)
    await sk.run()

def main():
    import argparse, asyncio
    p = argparse.ArgumentParser(description="AllTheBacteria sourmash sketcher (ATB)")
    p.add_argument("--config", "-c", required=True, help="Path to YAML config")
    args = p.parse_args()
    asyncio.run(main_async(args.config))

if __name__ == "__main__":
    main()
