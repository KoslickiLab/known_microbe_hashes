#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
wgs_sketch_union.py

Parallel pipeline to:
  (1) Extract 64-bit FracMinHash values from sourmash .sig.gz files inside .sig.zip archives,
      writing to partitioned on-disk spool files by k-mer size and a partition function over the hash.
  (2) Reduce each partition to exact unique values and write a partitioned Parquet (or NPY) dataset.
  (3) Optionally compute "rhs minus lhs" set-difference counts between two reduced datasets.
  (4) mk-mock: generate a small synthetic tree of .sig.zip files for testing.

Notes:
- Partitioning robust to scaled minhash using SplitMix64 ("mix" mode) or "low" bits.
- JSON parser supports your exact format: top-level list of objects with 'signatures': [...]
- Fault tolerance: per-archive and per-member error handling.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
from pathlib import Path
import sys
import time
import zipfile
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple
from itertools import repeat
from concurrent.futures import ProcessPoolExecutor
import itertools
import concurrent.futures
import atexit
from collections import OrderedDict

# Optional faster JSON
try:
    import orjson as fastjson
except ImportError:
    fastjson = None

import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None


# -----------------------
# Helpers / parsing
# -----------------------

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _hex_width(bits: int) -> int:
    return (bits + 3) // 4


def _iter_sigzip_files(root: Path) -> Iterator[Path]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".sig.zip"):
                yield Path(dirpath) / fn


def _load_json_bytes(b: bytes):
    if fastjson is not None:
        return fastjson.loads(b)
    return json.loads(b.decode("utf-8"))


def _iter_raw_signatures(obj) -> Iterator[dict]:
    """
    Iterate inner signature dicts from sourmash JSON:
      - dict with 'signatures': [...]
      - list of dicts, each possibly with 'signatures': [...]
      - direct signature dicts (fallback)
    """
    if isinstance(obj, dict):
        if "signatures" in obj and isinstance(obj["signatures"], list):
            for s in obj["signatures"]:
                if isinstance(s, dict):
                    yield s
        else:
            yield obj
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict) and "signatures" in item and isinstance(item["signatures"], list):
                for s in item["signatures"]:
                    if isinstance(s, dict):
                        yield s
            elif isinstance(item, dict):
                yield item


def _extract_hashes_from_signature_json_bytes(b: bytes,
                                              ksizes: Set[int]) -> Dict[int, np.ndarray]:
    """
    Return dict: ksize -> np.unique(np.uint64 array of hash values) from a single .sig(.gz) JSON.
    Supports 'mins' or 'hashes' (dict or list forms).
    """
    obj = _load_json_bytes(b)
    out: Dict[int, List[np.ndarray]] = {}
    for sig in _iter_raw_signatures(obj):
        k = sig.get("ksize")
        if k not in ksizes:
            continue

        hashes = None
        if "mins" in sig and sig["mins"]:
            hashes = sig["mins"]
        elif "hashes" in sig and sig["hashes"]:
            h = sig["hashes"]
            if isinstance(h, dict):
                hashes = list(map(int, h.keys()))
            else:
                hashes = h
        else:
            continue

        arr = np.asarray(hashes, dtype=np.uint64)
        if arr.size:
            arr = np.unique(arr)
            out.setdefault(int(k), []).append(arr)

    combined: Dict[int, np.ndarray] = {}
    for k, arrs in out.items():
        combined[k] = arrs[0] if len(arrs) == 1 else np.unique(np.concatenate(arrs))
    return combined


def _splitmix64(arr: np.ndarray) -> np.ndarray:
    """SplitMix64 mixer; keeps dtype uint64 and is vectorized."""
    mask = np.uint64(0xFFFFFFFFFFFFFFFF)
    z = (arr + np.uint64(0x9E3779B97F4A7C15)) & mask
    z ^= (z >> np.uint64(30))
    z = (z * np.uint64(0xBF58476D1CE4E5B9)) & mask
    z ^= (z >> np.uint64(27))
    z = (z * np.uint64(0x94D049BB133111EB)) & mask
    z ^= (z >> np.uint64(31))
    return z


def _partition_ids_for(arr: np.ndarray, partition_bits: int, mode: str = "mix") -> np.ndarray:
    """
    Map each 64-bit hash to a partition id in [0, 2^partition_bits).
      - "mix" (default): SplitMix64(arr) then take top bits (robust to scaled).
      - "low": take low bits (fast).
      - "high": take high bits (avoid with scaled).
    """
    if partition_bits <= 0:
        raise ValueError("partition_bits must be > 0")
    if mode not in ("mix", "low", "high"):
        raise ValueError("mode must be one of: mix, low, high")

    if mode == "mix":
        arr2 = _splitmix64(arr)
        shift = np.uint64(64 - partition_bits)
        return (arr2 >> shift).astype(np.uint32)
    elif mode == "low":
        mask = np.uint64((1 << partition_bits) - 1)
        return (arr & mask).astype(np.uint32)
    else:
        shift = np.uint64(64 - partition_bits)
        return (arr >> shift).astype(np.uint32)


def _pid_tag() -> str:
    return f"w{os.getpid()}"


# -----------------------
# EXTRACT stage
# -----------------------

class _SpoolFDCache:
    """Per-worker cache of open files + memoized directories."""
    def __init__(self, spool_dir: Path, max_open: int = 512):
        self.spool_dir = spool_dir
        self.max_open = max_open
        self._fds = OrderedDict()         # key=(k, part_hex) -> file object
        self._dirs_done = set()           # Path objects already created

    def _ensure_dir(self, d: Path):
        if d not in self._dirs_done:
            d.mkdir(parents=True, exist_ok=True)
            self._dirs_done.add(d)

    def get(self, k: int, part_hex: str):
        key = (k, part_hex)
        f = self._fds.pop(key, None)
        if f is None:
            part_dir = self.spool_dir / f"ksize={k}" / f"part={part_hex}"
            self._ensure_dir(part_dir)
            # Larger Python-level buffer reduces syscalls; OS will still buffer writes.
            f = open(part_dir / f"{_pid_tag()}.bin", "ab", buffering=1 << 20)
        # Push to MRU position
        self._fds[key] = f
        if len(self._fds) > self.max_open:
            old_key, old_f = self._fds.popitem(last=False)
            try: old_f.close()
            except: pass
        return f

    def close_all(self):
        for f in self._fds.values():
            try: f.close()
            except: pass
        self._fds.clear()

_FD_CACHE = None
def _get_fd_cache(spool_dir: Path, max_open: int):
    global _FD_CACHE
    if _FD_CACHE is None:
        _FD_CACHE = _SpoolFDCache(spool_dir, max_open)
        atexit.register(_FD_CACHE.close_all)
    return _FD_CACHE


def _process_one_sigzip(path: Path,
                        ksizes: Set[int],
                        spool_dir: Path,
                        partition_bits: int,
                        partition_mode: str,
                        open_files_per_worker: int) -> Tuple[int, int, int, int]:
    """
    Stream a .sig.zip, harvest hashes for given ksizes, and append to spool files.
    Returns: (archives_seen, siggz_seen, members_ok, members_err).
    """
    archives_seen, siggz_seen, members_ok, members_err = 1, 0, 0, 0
    try:
        with zipfile.ZipFile(path, "r") as zf:
            for zinfo in zf.infolist():
                name = zinfo.filename
                if not name.endswith(".sig.gz"):
                    continue
                if "/signatures/" not in name and not name.startswith("signatures/"):
                    continue
                try:
                    with zf.open(zinfo, "r") as zf_fp:
                        with gzip.GzipFile(fileobj=zf_fp, mode="rb") as gz:
                            raw = gz.read()
                    siggz_seen += 1
                    by_k = _extract_hashes_from_signature_json_bytes(raw, ksizes)
                    if not by_k:
                        continue

                    for k, arr in by_k.items():
                        if arr.size == 0:
                            continue
                        parts = _partition_ids_for(arr, partition_bits, mode=partition_mode)
                        order = np.argsort(parts, kind="mergesort")
                        arr_sorted = arr[order]
                        parts_sorted = parts[order]

                        uniq_parts, idx = np.unique(parts_sorted, return_index=True)
                        idx = list(idx) + [arr_sorted.size]
                        k_dir = spool_dir / f"ksize={k}"
                        fdcache = _get_fd_cache(spool_dir, open_files_per_worker)
                        for i, pid_ in enumerate(uniq_parts):
                            start, end = idx[i], idx[i + 1]
                            chunk = arr_sorted[start:end]
                            p_hex = format(int(pid_), f"0{_hex_width(partition_bits)}x")
                            f = fdcache.get(k, p_hex)
                            # one large write, no reopen, no remkdir
                            chunk.tofile(f)
                    members_ok += 1
                except (OSError, gzip.BadGzipFile, json.JSONDecodeError, UnicodeDecodeError) as e:
                    logging.warning(f"Skipping bad member in {path}: {name} ({e})")
                    members_err += 1
                except Exception as e:
                    logging.exception(f"Unexpected error for member {name} in {path}: {e}")
                    members_err += 1
    except zipfile.BadZipFile as e:
        logging.warning(f"Skipping corrupted zip archive {path}: {e}")
    except Exception as e:
        logging.exception(f"ERROR opening zip {path}: {e}")

    return (archives_seen, siggz_seen, members_ok, members_err)


def cmd_extract(args: argparse.Namespace) -> None:
    root = Path(args.input).resolve()
    spool_dir = Path(args.out).resolve() / "spool"
    _ensure_dir(spool_dir)

    ksizes = set(args.ksizes) if args.ksizes else {15, 31, 33}

    # Stream discovery; do NOT materialize 2M+ paths.
    path_iter = _iter_sigzip_files(root)
    logging.info("Scanning for .sig.zip under %s (streaming)...", root)

    start = time.time()
    archives = siggz = ok = err = 0

    # Bounded, out-of-order execution avoids head-of-line blocking
    max_in_flight = getattr(args, "max_in_flight", None) or (args.processes * 4)
    in_flight = set()

    # Use a safer start method if available (avoids forking a huge object graph)
    ctx = None
    try:
        import multiprocessing as _mp
        ctx = _mp.get_context("forkserver")
    except Exception:
        ctx = None

    Executor = (lambda **kw: ProcessPoolExecutor(mp_context=ctx, **kw)) if ctx else ProcessPoolExecutor
    with Executor(max_workers=args.processes) as ex:
        # Prime the pump
        for path in itertools.islice(path_iter, max_in_flight):
            in_flight.add(ex.submit(_process_one_sigzip, path, ksizes, spool_dir,
          args.partition_bits, args.partition_mode, args.open_files_per_worker)
)

        while in_flight:
            done, in_flight = concurrent.futures.wait(
                in_flight, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for fut in done:
                a, s, m_ok, m_err = fut.result()
                archives += a; siggz += s; ok += m_ok; err += m_err

                # Refill to keep a bounded number of tasks in flight
                try:
                    next_path = next(path_iter)
                    in_flight.add(ex.submit(
                        _process_one_sigzip,
                        next_path, ksizes, spool_dir,
                        args.partition_bits, args.partition_mode,
                        args.open_files_per_worker,  # ← add this
                    ))

                except StopIteration:
                    pass

                if archives % 1000 == 0:
                    elapsed = time.time() - start
                    logging.info(
                        "Processed %s zips / %s sig.gz; members ok=%s bad=%s in %.1fs",
                        f"{archives:,}", f"{siggz:,}", f"{ok:,}", f"{err:,}", elapsed
                    )

    logging.info("DONE extract: processed %s zips, %s sig.gz; members ok=%s bad=%s",
                 f"{archives:,}", f"{siggz:,}", f"{ok:,}", f"{err:,}")
    logging.info("SPOOL at %s", spool_dir)



# -----------------------
# REDUCE stage
# -----------------------

def _read_uint64_bins(bin_files: List[Path]) -> np.ndarray:
    arrays: List[np.ndarray] = []
    for p in bin_files:
        if p.stat().st_size == 0:
            continue
        try:
            arr = np.fromfile(p, dtype=np.uint64)
        except Exception as e:
            logging.warning(f"Skipping unreadable bin file {p}: {e}")
            continue
        if arr.size:
            arrays.append(arr)
    if not arrays:
        return np.zeros((0,), dtype=np.uint64)
    return np.concatenate(arrays, axis=0)


def _write_parquet_unique(out_dir: Path,
                          k: int,
                          part_hex: str,
                          uniq: np.ndarray,
                          compression: str = "zstd") -> None:
    if pa is None or pq is None:
        raise RuntimeError("pyarrow is required for --format parquet")
    k_dir = out_dir / f"ksize={k}" / f"part={part_hex}"
    _ensure_dir(k_dir)
    table = pa.table({"hash": pa.array(uniq, type=pa.uint64())})
    pq.write_table(table, k_dir / "part.parquet", compression=compression)


def _write_npy_unique(out_dir: Path,
                      k: int,
                      part_hex: str,
                      uniq: np.ndarray) -> None:
    k_dir = out_dir / f"ksize={k}" / f"part={part_hex}"
    _ensure_dir(k_dir)
    np.save(k_dir / "part.npy", uniq, allow_pickle=False)


def _reduce_one_partition(spool_dir: Path,
                          out_dir: Path,
                          k: int,
                          part_hex: str,
                          fmt: str,
                          mem_limit_gb: float) -> int:
    part_dir = spool_dir / f"ksize={k}" / f"part={part_hex}"
    if not part_dir.exists():
        return 0
    bin_files = sorted(p for p in part_dir.glob("*.bin") if p.is_file())
    if not bin_files:
        return 0

    total_bytes = sum(p.stat().st_size for p in bin_files)
    if total_bytes > mem_limit_gb * (1024**3) * 0.6:
        raise MemoryError(
            f"Partition k={k} part={part_hex} total_bytes={total_bytes:,} "
            f"exceeds ~60% of mem-limit; re-run with larger --partition-bits or bigger --mem-limit-gb."
        )

    all_arr = _read_uint64_bins(bin_files)
    uniq = np.unique(all_arr) if all_arr.size else all_arr

    if fmt == "parquet":
        _write_parquet_unique(out_dir, k, part_hex, uniq)
    else:
        _write_npy_unique(out_dir, k, part_hex, uniq)

    return int(uniq.size)


def _reduce_task(spool_dir: Path, out_dir: Path, k: int, part_hex: str, fmt: str, mem_limit_gb: float):
    n = _reduce_one_partition(spool_dir, out_dir, k, part_hex, fmt, mem_limit_gb)
    return (k, part_hex, n)


def cmd_reduce(args: argparse.Namespace) -> None:
    spool_dir = Path(args.spool).resolve()
    out_dir = Path(args.out).resolve()
    _ensure_dir(out_dir)

    k_dirs = sorted(d for d in spool_dir.glob("ksize=*") if d.is_dir())
    ksizes = sorted(int(d.name.split("=")[1]) for d in k_dirs)
    if args.ksizes:
        ksizes = [k for k in ksizes if k in set(args.ksizes)]
    if not ksizes:
        logging.error("No ksize directories found under spool.")
        sys.exit(2)

    any_part = next((p for p in spool_dir.rglob("part=*") if p.is_dir()), None)
    if not any_part:
        logging.error("No partitions found under spool.")
        sys.exit(2)
    part_name = any_part.name.split("=")[1]
    partition_bits = len(part_name) * 4

    fmt = args.format.lower()
    if fmt not in ("parquet", "npy"):
        logging.error("--format must be parquet or npy")
        sys.exit(2)
    if fmt == "parquet" and (pa is None or pq is None):
        logging.error("pyarrow is required for parquet output.")
        sys.exit(2)

    manifest = {
        "format": fmt,
        "partition_bits": partition_bits,
        "partition_mode": args.partition_mode,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ksizes": {},
        "total_distinct": 0
    }

    # Build task list
    tasks: List[Tuple[int, str]] = []
    for k in ksizes:
        parts = sorted(d for d in (spool_dir / f"ksize={k}").glob("part=*") if d.is_dir())
        for pdir in parts:
            tasks.append((k, pdir.name.split("=")[1]))

    counts_by_k: Dict[int, int] = {k: 0 for k in ksizes}
    written_by_k: Dict[int, int] = {k: 0 for k in ksizes}

    with ProcessPoolExecutor(max_workers=args.processes) as ex:
        for k, part_hex, n in ex.map(
            _reduce_task,
            repeat(spool_dir),
            repeat(out_dir),
            (t[0] for t in tasks),
            (t[1] for t in tasks),
            repeat(fmt),
            repeat(args.mem_limit_gb),
            chunksize=8
        ):
            counts_by_k[k] += n
            written_by_k[k] += 1

    for k in ksizes:
        manifest["ksizes"][str(k)] = {
            "partitions": written_by_k[k],
            "distinct_count": counts_by_k[k]
        }
        manifest["total_distinct"] += counts_by_k[k]
        logging.info(f"k={k}: distinct={counts_by_k[k]:,} across {written_by_k[k]} partitions")

    with open(out_dir / "_MANIFEST.json", "w") as f:
        json.dump(manifest, f, indent=2)
    logging.info(f"Wrote manifest to {out_dir / '_MANIFEST.json'}")


# -----------------------
# DIFF stage
# -----------------------

def _load_unique_partition(dir_: Path, k: int, part_hex: str) -> np.ndarray:
    p_parquet = dir_ / f"ksize={k}" / f"part={part_hex}" / "part.parquet"
    p_npy = dir_ / f"ksize={k}" / f"part={part_hex}" / "part.npy"
    if p_parquet.exists():
        if pa is None or pq is None:
            raise RuntimeError("pyarrow required to read parquet")
        table = pq.read_table(p_parquet, columns=["hash"])
        arr = table.column("hash").to_numpy(zero_copy_only=False)
        return np.asarray(arr, dtype=np.uint64)
    elif p_npy.exists():
        return np.load(p_npy)
    else:
        return np.zeros((0,), dtype=np.uint64)


def _read_manifest_if_exists(dir_: Path) -> Optional[dict]:
    man = dir_ / "_MANIFEST.json"
    if man.exists():
        try:
            return json.load(open(man, "r"))
        except Exception:
            return None
    return None


def cmd_diff(args: argparse.Namespace) -> None:
    lhs = Path(args.lhs).resolve()
    rhs = Path(args.rhs).resolve()
    out_dir = Path(args.out).resolve() if args.write_diff.lower() == "yes" else None
    if out_dir:
        _ensure_dir(out_dir)

    k_dirs = sorted(d for d in rhs.glob("ksize=*") if d.is_dir())
    ksizes = sorted(int(d.name.split("=")[1]) for d in k_dirs)
    if args.ksizes:
        ksizes = [k for k in ksizes if k in set(args.ksizes)]
    if not ksizes:
        logging.error("No ksizes detected in rhs dataset.")
        sys.exit(2)

    any_part = next((p for p in rhs.rglob("part=*") if p.is_dir()), None)
    if not any_part:
        logging.error("No partitions found under rhs.")
        sys.exit(2)

    lhs_man = _read_manifest_if_exists(lhs)
    rhs_man = _read_manifest_if_exists(rhs)
    if lhs_man and rhs_man:
        if lhs_man.get("partition_bits") != rhs_man.get("partition_bits") \
           or lhs_man.get("partition_mode") != rhs_man.get("partition_mode"):
            logging.error("Partition config mismatch between datasets. "
                          "lhs: bits=%s mode=%s; rhs: bits=%s mode=%s",
                          lhs_man.get("partition_bits"), lhs_man.get("partition_mode"),
                          rhs_man.get("partition_bits"), rhs_man.get("partition_mode"))
            sys.exit(4)

    totals = {}
    grand = 0

    for k in ksizes:
        parts = sorted(d for d in (rhs / f"ksize={k}").glob("part=*") if d.is_dir())
        subtotal = 0
        for pdir in parts:
            part_hex = pdir.name.split("=")[1]
            rhs_arr = _load_unique_partition(rhs, k, part_hex)
            lhs_arr = _load_unique_partition(lhs, k, part_hex)
            if rhs_arr.size == 0:
                continue
            if lhs_arr.size == 0:
                diff_arr = rhs_arr
            else:
                diff_arr = np.setdiff1d(rhs_arr, lhs_arr, assume_unique=True)

            subtotal += int(diff_arr.size)

            if out_dir and diff_arr.size:
                if pa is not None and pq is not None:
                    _write_parquet_unique(out_dir, k, part_hex, diff_arr)
                else:
                    _write_npy_unique(out_dir, k, part_hex, diff_arr)

        totals[str(k)] = subtotal
        grand += subtotal
        logging.info(f"k={k}: rhs-not-in-lhs = {subtotal:,}")

    print(json.dumps({"rhs_not_in_lhs": totals, "total_rhs_not_in_lhs": grand}, indent=2))


# -----------------------
# MOCK DATA (for testing)
# -----------------------

def _mk_mock_one_sig_json(ksize: int, scaled: int, n_hashes: int, rng: np.random.Generator, seed: int = 42) -> dict:
    # emulate "scaled": keep values below ~2^64 / scaled
    # using ceil for a conservative upper bound
    max_hash = int((1 << 64) / scaled)
    vals = rng.integers(low=0, high=max_hash, size=n_hashes, dtype=np.uint64)
    vals = np.unique(vals).tolist()
    return {
        "num": 0,
        "ksize": int(ksize),
        "seed": seed,
        "max_hash": max_hash,
        "mins": vals,
        "md5sum": f"deadbeef{ksize}",
        "molecule": "DNA"
    }


def cmd_mk_mock(args: argparse.Namespace) -> None:
    """
    Create a small tree of mock .sig.zip archives with:
      - top-level list
      - each element has 'signatures': [ {ksize=k, mins=[...]} ]
      - one .sig.gz per k inside signatures/ folder
    """
    out = Path(args.out).resolve()
    _ensure_dir(out)
    rng = np.random.default_rng(args.seed)

    # Put files in a couple of subdirs to mimic your layout
    subdirs = ["AAA", "AAB", "AAC"]
    for sd in subdirs:
        _ensure_dir(out / sd)

    archives = args.archives
    members = args.members
    ksizes = args.ksizes
    scaled = args.scaled

    for i in range(archives):
        sd = subdirs[i % len(subdirs)]
        zip_name = f"wgs.MOCK{i:06d}.1.fsa_nt.gz.sig.zip"
        zpath = out / sd / zip_name
        with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            zf.writestr("SOURMASH-MANIFEST.csv", "not,used,here\n")
            for m in range(members):
                for k in ksizes:
                    # Build JSON as a top-level list with one object containing 'signatures': [...]
                    sig = _mk_mock_one_sig_json(k, scaled, n_hashes=200 + (i + m + k) % 50, rng=rng)
                    record = [{
                        "class": "sourmash_signature",
                        "email": "",
                        "hash_function": "0.murmur64",
                        "filename": f"/mock/{sd}/{zip_name}",
                        "license": "CC0",
                        "signatures": [sig],
                        "version": 0.4
                    }]
                    raw = (fastjson.dumps(record) if fastjson else json.dumps(record).encode("utf-8"))

                    # gzip-compress into memory
                    import io as _io
                    buf = _io.BytesIO()
                    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                        gz.write(raw)
                    gz_bytes = buf.getvalue()

                    # one .sig.gz per k; name resembles md5
                    zf.writestr(f"signatures/{sig['md5sum']}.{m}.{k}.sig.gz", gz_bytes)

    logging.info(f"Mock data created at {out} ({archives} zip archives)")


# -----------------------
# CLI
# -----------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Collect and count distinct FracMinHash hashes from sourmash .sig.gz inside .sig.zip at scale."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pe = sub.add_parser("extract", help="Extract hash values into partitioned spool files.")
    pe.add_argument("--input", required=True, help="Root directory containing .sig.zip files.")
    pe.add_argument("--out", required=True, help="Output root directory (will create 'spool').")
    pe.add_argument("--processes", type=int, default=os.cpu_count() or 8, help="Parallel processes.")
    pe.add_argument("--partition-bits", type=int, default=14,
                    help="Number of bits to partition on (default 14 => 16384 partitions).")
    pe.add_argument("--partition-mode", default="mix", choices=["mix", "low", "high"],
                    help="Partition function over 64-bit hashes. Default 'mix' uses SplitMix64+top-bits (robust with scaled).")
    pe.add_argument("--ksizes", type=int, nargs="*", default=[15,31,33], help="k-mer sizes to include.")
    pe.add_argument("--max-in-flight", type=int, default=None,
                    help="Max tasks in flight (default: 4×processes).")
    pe.add_argument("--open-files-per-worker", type=int, default=512,
                    help="Max spool files kept open per worker (default 512).")
    pe.set_defaults(func=cmd_extract)

    pr = sub.add_parser("reduce", help="Reduce spool partitions to unique and write Parquet or NPY dataset.")
    pr.add_argument("--spool", required=True, help="Spool directory created by 'extract' (…/out/spool).")
    pr.add_argument("--out", required=True, help="Output dataset directory for unique partitions.")
    pr.add_argument("--format", default="parquet", help="parquet | npy (default parquet).")
    pr.add_argument("--mem-limit-gb", type=float, default=8.0, help="Approx RAM budget per partition.")
    pr.add_argument("--ksizes", type=int, nargs="*", help="Optionally restrict to these ksizes.")
    pr.add_argument("--partition-mode", default="mix", choices=["mix", "low", "high"],
                    help="Echoed into manifest for later compatibility checks.")
    pr.add_argument("--processes", type=int, default=os.cpu_count() or 8,
                    help="Parallel processes for reduce (per partition).")
    pr.set_defaults(func=cmd_reduce)

    pdiff = sub.add_parser("diff", help="Compute counts of hashes in RHS not in LHS (both reduced datasets).")
    pdiff.add_argument("--lhs", required=True, help="Left-hand (e.g., GenBank-WGS) reduced dataset (parquet/npy).")
    pdiff.add_argument("--rhs", required=True, help="Right-hand reduced dataset to compare against LHS.")
    pdiff.add_argument("--out", required=False, help="Output directory to write difference partitions.")
    pdiff.add_argument("--write-diff", default="no", choices=["yes", "no"],
                       help="Also write per-partition difference dataset (default no).")
    pdiff.add_argument("--ksizes", type=int, nargs="*", help="Optionally restrict to these ksizes.")
    pdiff.set_defaults(func=cmd_diff)

    pmock = sub.add_parser("mk-mock", help="Create a small synthetic dataset of .sig.zip files for testing.")
    pmock.add_argument("--out", required=True, help="Directory to create with mock data.")
    pmock.add_argument("--archives", type=int, default=20, help="Number of .sig.zip to create.")
    pmock.add_argument("--members", type=int, default=2, help="Number of .sig.gz members per archive.")
    pmock.add_argument("--ksizes", type=int, nargs="*", default=[15,31,33])
    pmock.add_argument("--scaled", type=int, default=1000, help="Scaled denominator to emulate.")
    pmock.add_argument("--seed", type=int, default=42)
    pmock.set_defaults(func=cmd_mk_mock)

    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )
    args.func(args)


if __name__ == "__main__":
    main()
