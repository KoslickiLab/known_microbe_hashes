import asyncio
import contextlib
import logging
import os
import signal
import zipfile
from typing import List

import aiohttp

from .db import DB
from .utils import LOG, build_logger, shard_subdir_for, RequestRateLimiter
from .worker import download_accessions, run_sourmash, RateLimitError

DEFAULT_PARAMS = "k=15,k=31,k=33,scaled=1000,noabund"

class Sketcher:
    def __init__(self, config: dict):
        self.cfg = config
        level_name = str(self.cfg.get("log_level", "INFO")).upper()
        level = getattr(logging, level_name, logging.INFO)
        build_logger(self.cfg.get("log_path"), level)
        self.db = DB(self.cfg["state_db"])
        self.stop_flag = False

    def _load_accessions(self) -> List[str]:
        path = self.cfg["accessions_path"]
        accessions = []
        seen = set()
        with open(path, "r") as handle:
            for line in handle:
                acc = line.strip()
                if not acc:
                    continue
                if acc.startswith("#"):
                    continue
                if acc not in seen:
                    seen.add(acc)
                    accessions.append(acc)
        return accessions

    async def enqueue_accessions(self):
        dry_run = bool(self.cfg.get("dry_run", False))
        accessions = self._load_accessions()
        if dry_run:
            for acc in accessions:
                LOG.info("Would enqueue %s", acc)
            LOG.info("Loaded %d accessions (dry run)", len(accessions))
            return

        existing = self.db.existing_filenames(accessions)
        count = 0
        for acc in accessions:
            if acc in existing:
                continue
            url = f"nuccore:{acc}"
            self.db.upsert_file("nuccore", acc, url, None, None)
            count += 1
            if count % 1000 == 0:
                LOG.info("Enqueued %d new accessions so far", count)
        LOG.info("Loaded %d accessions (%d new, %d already present)", len(accessions), count, len(existing))

    async def worker(self, session: aiohttp.ClientSession, net_sem: asyncio.Semaphore, rate: RequestRateLimiter):
        params = self.cfg.get("sourmash_params", DEFAULT_PARAMS)
        rayon_threads = int(self.cfg.get("sourmash_threads", 1))
        tmp_root = self.cfg["tmp_root"]
        out_root = self.cfg["output_root"]
        retry_max = int(self.cfg.get("max_retries", 6))
        timeout = int(self.cfg.get("request_timeout_seconds", 3600))
        shard_mod = int(self.cfg.get("shard_modulus", 512))
        batch_size = int(self.cfg.get("batch_size", 1))
        api_key = self.cfg.get("ncbi_api_key")
        email = self.cfg.get("email")
        error_cooldown = int(self.cfg.get("error_retry_cooldown_seconds", 1800))
        error_max_total = int(self.cfg.get("error_max_total_tries", 20))
        idle_cycles = 0

        while not self.stop_flag:
            try:
                claims = self.db.claim_batch(batch_size, error_cooldown, error_max_total)
            except Exception as exc:
                LOG.exception("DB error while claiming work: %r", exc)
                await asyncio.sleep(2.0)
                continue
            if not claims:
                idle_cycles += 1
                if idle_cycles % 30 == 0:
                    st = self.db.stats()
                    LOG.info("No claimable work yet. by_status=%s", st.get("by_status"))
                await asyncio.sleep(2.0)
                continue
            idle_cycles = 0
            accession_paths = {}
            for file_id, _, accession, _ in claims:
                LOG.debug("Claimed id=%s accession=%s", file_id, accession)
                rel_dir = shard_subdir_for(accession, shard_mod)
                local_tmp = os.path.join(tmp_root, rel_dir, f"{accession}.fasta")
                local_out = os.path.join(out_root, rel_dir, f"{accession}.sig.zip")
                accession_paths[accession] = local_tmp

                if os.path.exists(local_out):
                    try:
                        with zipfile.ZipFile(local_out):
                            pass
                        LOG.debug("Skipping existing output %s", local_out)
                        self.db.mark_status(file_id, "DONE", out_path=local_out)
                    except zipfile.BadZipFile:
                        LOG.warning("Removing corrupt output %s", local_out)
                        try:
                            os.remove(local_out)
                        except Exception:
                            pass

            if not accession_paths:
                continue

            try:
                async with net_sem:
                    LOG.debug("Downloading batch of %d accessions", len(accession_paths))
                    paths, sizes, missing = await download_accessions(
                        session,
                        accession_paths,
                        rate,
                        api_key,
                        timeout=timeout,
                        email=email,
                    )
            except RateLimitError as exc:
                LOG.warning("Rate limit hit for batch (size %d): %s", len(accession_paths), exc)
                for file_id, _, _, _ in claims:
                    self.db.mark_status(file_id, "ERROR", error=str(exc), inc_tries=True)
                    if self.db.get_tries(file_id) >= retry_max:
                        self.db.mark_status(file_id, "FAILED", error=str(exc))
                await asyncio.sleep(5)
                continue
            except Exception as exc:
                LOG.exception("Error downloading batch")
                for file_id, _, _, _ in claims:
                    self.db.mark_status(file_id, "ERROR", error=str(exc), inc_tries=True)
                    if self.db.get_tries(file_id) >= retry_max:
                        self.db.mark_status(file_id, "FAILED", error=str(exc))
                await asyncio.sleep(2)
                continue

            id_by_accession = {acc: fid for fid, _, acc, _ in claims}
            for accession, reason in missing.items():
                file_id = id_by_accession.get(accession)
                if file_id is None:
                    continue
                self.db.update_size(file_id, sizes.get(accession, 0))
                self.db.mark_status(file_id, "EMPTY", error=reason)

            for accession, local_tmp in paths.items():
                file_id = id_by_accession.get(accession)
                if file_id is None:
                    continue
                rel_dir = shard_subdir_for(accession, shard_mod)
                local_out = os.path.join(out_root, rel_dir, f"{accession}.sig.zip")
                self.db.update_size(file_id, sizes.get(accession, 0))
                try:
                    self.db.mark_status(file_id, "SKETCHING")
                    LOG.debug("Sketching %s", local_tmp)
                    if not os.path.exists(local_tmp):
                        raise FileNotFoundError(f"Missing downloaded file {local_tmp}")
                    rc, out = await run_sourmash(local_tmp, local_out, params, rayon_threads, log=LOG)
                    if rc != 0:
                        LOG.error("sourmash failed rc=%s for %s\n%s", rc, local_tmp, out)
                        raise RuntimeError(f"sourmash failed rc={rc} for {local_tmp}")
                    self.db.mark_status(file_id, "DONE", out_path=local_out)
                    LOG.info("Finished %s", accession)
                except Exception as exc:
                    LOG.exception("Error processing %s", accession)
                    self.db.mark_status(file_id, "ERROR", error=str(exc), inc_tries=True)
                    if self.db.get_tries(file_id) >= retry_max:
                        self.db.mark_status(file_id, "FAILED", error=str(exc))
                finally:
                    try:
                        if os.path.exists(local_tmp):
                            os.remove(local_tmp)
                    except Exception:
                        pass

    async def run(self):
        for p in (
            self.cfg["output_root"],
            self.cfg["tmp_root"],
            os.path.dirname(self.cfg["state_db"]),
            os.path.dirname(self.cfg.get("log_path", "/tmp/void.log")),
        ):
            if p:
                os.makedirs(p, exist_ok=True)

        await self.enqueue_accessions()
        if self.cfg.get("dry_run", False):
            LOG.info("Dry run complete; exiting.")
            return

        stale = int(self.cfg.get("stale_seconds", 3600))
        self.db.reset_stuck(stale)

        max_dl = int(self.cfg.get("max_concurrent_downloads", 8))
        net_sem = asyncio.Semaphore(max_dl)
        reqs_per_sec = self.cfg.get("requests_per_second")
        rate = RequestRateLimiter(float(reqs_per_sec)) if reqs_per_sec else None

        conn = aiohttp.TCPConnector(limit_per_host=max_dl, limit=max_dl)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=120, sock_read=3600)

        headers = {"User-Agent": self.cfg.get("user_agent", "Accession Sketcher/1.0")}

        async with aiohttp.ClientSession(connector=conn, timeout=timeout, headers=headers) as session:
            total_workers = int(self.cfg.get("max_total_workers", 96))
            workers = [asyncio.create_task(self.worker(session, net_sem, rate)) for _ in range(total_workers)]
            for worker in workers:
                worker.add_done_callback(
                    lambda task: LOG.exception("Worker crashed: %r", task.exception()) if task.exception() else None
                )
            monitor_task = asyncio.create_task(self._monitor(workers))

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._request_stop)

            while True:
                st = self.db.stats()
                claimable = self.db.count_claimable(
                    int(self.cfg.get("error_retry_cooldown_seconds", 1800)),
                    int(self.cfg.get("error_max_total_tries", 20)),
                )
                pending = (
                    claimable
                    + st["by_status"].get("DOWNLOADING", 0)
                    + st["by_status"].get("SKETCHING", 0)
                )
                if pending == 0:
                    self.stop_flag = True
                    break
                await asyncio.sleep(10)

            await asyncio.gather(*workers, return_exceptions=True)
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

    def _request_stop(self):
        LOG.warning("Stop requested; will finish current tasks then exit.")
        self.stop_flag = True

    async def _monitor(self, workers):
        while not self.stop_flag:
            st = self.db.stats()
            alive = sum(not worker.done() for worker in workers)
            LOG.info("Monitor: alive_workers=%d stats=%s", alive, st.get("by_status"))
            for worker in workers:
                if worker.done() and worker.exception():
                    LOG.error("Worker died: %r", worker.exception())
            await asyncio.sleep(30)

def load_config(path: str) -> dict:
    import yaml

    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("accessions_path", "/scratch/accessions.txt")
    cfg.setdefault("output_root", "/scratch/accession_sketches")
    cfg.setdefault("tmp_root", "/scratch/accession_tmp")
    cfg.setdefault("state_db", "/scratch/accession_state/sketcher.sqlite")
    cfg.setdefault("log_path", "/scratch/accession_logs/sketcher.log")
    cfg.setdefault("max_concurrent_downloads", 8)
    cfg.setdefault("max_total_workers", 96)
    cfg.setdefault("user_agent", "Accession Sketcher/1.0 (+dmk333@psu.edu; admin=dmk333@psu.edu)")
    cfg.setdefault("error_retry_cooldown_seconds", 1800)
    cfg.setdefault("error_max_total_tries", 20)
    cfg.setdefault("stale_seconds", 3600)
    cfg.setdefault("dry_run", True)
    cfg.setdefault("sourmash_params", DEFAULT_PARAMS)
    cfg.setdefault("sourmash_threads", 1)
    cfg.setdefault("request_timeout_seconds", 3600)
    cfg.setdefault("max_retries", 8)
    cfg.setdefault("batch_size", 1)
    cfg.setdefault("shard_modulus", 512)
    cfg.setdefault("log_level", "INFO")
    cfg.setdefault("ncbi_api_key", None)
    cfg.setdefault("email", None)
    cfg.setdefault("requests_per_second", None)

    if cfg.get("requests_per_second") is None:
        cfg["requests_per_second"] = 10 if cfg.get("ncbi_api_key") else 3

    for key in ("output_root", "tmp_root", "state_db", "log_path", "accessions_path"):
        cfg[key] = os.path.abspath(os.path.expanduser(cfg[key]))
    return cfg

async def main_async(cfg_path: str):
    cfg = load_config(cfg_path)
    sk = Sketcher(cfg)
    await sk.run()

def main():
    import argparse

    p = argparse.ArgumentParser(description="Accession sourmash sketcher")
    p.add_argument("--config", "-c", required=True, help="Path to YAML config")
    args = p.parse_args()
    asyncio.run(main_async(args.config))

if __name__ == "__main__":
    main()
