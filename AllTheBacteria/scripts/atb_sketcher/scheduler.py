# atb_sketcher/scheduler.py
import asyncio
import os
import signal
import aiohttp
import yaml
from typing import Optional, Dict, Any, Set, Tuple

from .utils import LOG, build_logger, shard_subdir_for, RateLimiter
from .db import DB
from .worker import download_file, run_sourmash

DEFAULT_PARAMS = "k=15,k=31,k=33,scaled=1000,noabund"

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)

class Sketcher:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        build_logger(self.cfg.get("log_path"))
        self.db = DB(self.cfg["state_db"])
        self.stop_flag = False
        self._active: Set[asyncio.Task] = set()
        self._last_idle_ts: Optional[float] = None

    # --- seeding from ATB filelist-latest.tsv (CSV) ---
    def seed_from_filelist(self) -> None:
        import csv
        filelist_path = self.cfg["filelist_path"]
        region = self.cfg.get("s3_region", "eu-west-2")
        seed_limit = self.cfg.get("seed_limit")
        n_shards = int(self.cfg.get("n_shards", 4096))

        LOG.info("Seeding from filelist: %s", filelist_path)
        added = 0
        with open(filelist_path, newline="") as fh:
            reader = csv.reader(fh)
            for idx, row in enumerate(reader):
                # Expected columns: bucket, filename, size, mtime, md5, (ignored)
                if not row or len(row) < 2:
                    continue
                bucket = row[0].strip().strip('"')
                filename = row[1].strip().strip('"')
                size = None
                if len(row) > 2 and row[2].strip():
                    try:
                        size = int(row[2].strip().strip('"'))
                    except Exception:
                        size = None
                mtime = row[3].strip().strip('"') if len(row) > 3 else None

                subdir = shard_subdir_for(filename, n_shards)
                url = f"https://{bucket}.s3.{region}.amazonaws.com/{filename}"
                self.db.upsert_file(subdir=subdir, filename=filename, url=url, size=size, mtime=mtime)
                added += 1
                if seed_limit and added >= int(seed_limit):
                    break
        stats = self.db.stats()
        LOG.info("Seeded %d rows. DB now has %d total rows.", added, stats.get("total", 0))

    async def _process_one(self,
                            session: aiohttp.ClientSession,
                            claim: Tuple[int, str, str, str],
                            rate: Optional[RateLimiter],
                            timeout: int,
                            params: str,
                            sm_threads: int,
                            out_root: str,
                            tmp_root: str) -> None:
        file_id, subdir, filename, url = claim
        dl_path = os.path.join(tmp_root, subdir, filename)
        out_dir = os.path.join(out_root, subdir)
        out_path = os.path.join(out_dir, f"{filename}.sig.zip")

        # Ensure directories exist
        os.makedirs(os.path.dirname(dl_path), exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        try:
            # Download
            await download_file(session, url=url, dest_path=dl_path, rate=rate, timeout=timeout)
            # Mark sketching (optional)
            try:
                self.db.mark_status(file_id, "SKETCHING", out_path=None, error=None)
            except Exception:
                pass

            # Sketch
            extra_env = {"RAYON_NUM_THREADS": str(sm_threads)}
            rc, out_combined = await run_sourmash(input_path=dl_path,
                                                  output_path=out_path,
                                                  params=params,
                                                  threads=sm_threads,
                                                  extra_env=extra_env)
            if rc == 0 and os.path.exists(out_path):
                self.db.mark_status(file_id, "DONE", out_path=out_path, error=None)
                try:
                    os.remove(dl_path)  # save space
                except FileNotFoundError:
                    pass
            else:
                err = out_combined or f"sourmash returned {rc}"
                self.db.mark_status(file_id, "ERROR", out_path=None, error=err, inc_tries=True)

        except Exception as e:
            self.db.mark_status(file_id, "ERROR", out_path=None, error=str(e), inc_tries=True)

    async def run(self) -> None:
        # Seed DB first time (idempotent due to upsert)
        self.seed_from_filelist()

        exit_when_done = bool(self.cfg.get("exit_when_done", True))
        idle_exit_grace = int(self.cfg.get("idle_exit_grace_seconds", 60))
        check_interval = float(self.cfg.get("completion_check_every_seconds", 5.0))
        treat_error_as_done = bool(self.cfg.get("treat_error_as_done", False))

        out_root = self.cfg["output_root"]
        tmp_root = self.cfg["tmp_root"]
        stale_seconds = int(self.cfg.get("stale_seconds", 3600))

        max_workers = int(self.cfg.get("max_total_workers", 96))
        error_cooldown = int(self.cfg.get("error_retry_cooldown_seconds", 1800))
        error_max_total = int(self.cfg.get("error_max_total_tries", 20))
        timeout = int(self.cfg.get("request_timeout_seconds", 3600))
        sm_params = self.cfg.get("sourmash_params", DEFAULT_PARAMS)
        sm_threads = int(self.cfg.get("sourmash_threads", 1))

        user_agent = self.cfg.get("user_agent", "ATB Sketcher/1.0")
        headers = {"User-Agent": user_agent}
        conn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
        session_timeout = aiohttp.ClientTimeout(total=None)

        rate_limit = self.cfg.get("rate_limit_bytes_per_sec")
        rate = RateLimiter(int(rate_limit)) if rate_limit else None

        loop = asyncio.get_running_loop()
        def _signal_handler():
            LOG.warning("Signal received; will stop after in-flight tasks complete.")
            self.stop_flag = True
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                pass  # Windows

        async with aiohttp.ClientSession(connector=conn, timeout=session_timeout, headers=headers) as session:
            while not self.stop_flag:
                # 1) Requeue stale in-progress rows (crash/kill recovery)
                try:
                    self.db.reset_stuck(stale_seconds=stale_seconds)
                except Exception as e:
                    LOG.exception("DB.reset_stuck failed: %r", e)

                # 2) Fill the active set up to max_workers
                try:
                    while not self.stop_flag and len(self._active) < max_workers:
                        claim = self.db.claim_next(error_cooldown, error_max_total)
                        if not claim:
                            break
                        t = asyncio.create_task(
                            self._process_one(session, claim, rate, timeout, sm_params, sm_threads, out_root, tmp_root)
                        )
                        self._active.add(t)
                        t.add_done_callback(self._active.discard)
                except Exception as e:
                    LOG.exception("Claim/launch failed: %r", e)
                    await asyncio.sleep(1.0)

                # 3) Completion check & idle-exit
                if exit_when_done:
                    try:
                        if len(self._active) == 0:
                            claim = self.db.claim_next(error_cooldown, error_max_total)
                            if claim:
                                t = asyncio.create_task(
                                    self._process_one(session, claim, rate, timeout, sm_params, sm_threads, out_root, tmp_root)
                                )
                                self._active.add(t)
                                t.add_done_callback(self._active.discard)
                                self._last_idle_ts = None
                            else:
                                now = asyncio.get_event_loop().time()
                                if self._last_idle_ts is None:
                                    self._last_idle_ts = now
                                idle_elapsed = now - self._last_idle_ts
                                if idle_elapsed >= idle_exit_grace:
                                    stats = self.db.stats()
                                    unfinished = 0
                                    for key in ("PENDING","DOWNLOADING","SKETCHING"):
                                        unfinished += int(stats["by_status"].get(key, 0))
                                    if not treat_error_as_done:
                                        unfinished += int(stats["by_status"].get("ERROR", 0))
                                    if unfinished == 0:
                                        LOG.info("All work completed (idle %.1fs). Exiting.", idle_elapsed)
                                        break
                        else:
                            self._last_idle_ts = None
                    except Exception as e:
                        LOG.exception("completion check failed: %r", e)

                # 4) Wait a bit for tasks to complete, then loop
                if self._active:
                    await asyncio.wait(self._active, timeout=check_interval, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.sleep(check_interval)

        # Ensure all tasks finished before returning
        if self._active:
            LOG.info("Waiting for %d in-flight task(s) to finish...", len(self._active))
            await asyncio.gather(*self._active, return_exceptions=True)

# --- module entry points ---

async def main_async(cfg_path: str):
    cfg = load_config(cfg_path)
    sk = Sketcher(cfg)
    await sk.run()

def main():
    import argparse
    p = argparse.ArgumentParser(description="ATB sourmash sketcher")
    p.add_argument("--config", "-c", required=True, help="Path to YAML config")
    args = p.parse_args()
    asyncio.run(main_async(args.config))

if __name__ == "__main__":
    main()
