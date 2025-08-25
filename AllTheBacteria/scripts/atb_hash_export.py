#!/usr/bin/env python3
"""
Export per-(sample_id, ksize, hash, bucket) rows from *.fa.gz.sig.zip -> Parquet.

- Input: root directory containing bucket=XXXX/ subdirs with .fa.gz.sig.zip
- Output (non-dedup): partitioned Parquet dataset at out_root/
    out_root/
      bucket=000/
        worker=0001/
          part-<uuid>.parquet
      bucket=001/
        worker=0001/
          part-<uuid>.parquet
      ...

Columns: sample_id (string), ksize (int16), hash (uint64), bucket (int32)

Behavior:
- No extraction to disk. Zip entries are read in memory. Each *.sig.gz is decompressed
  in memory and parsed as JSON.
- Supports sourmash "wrapper" JSON (a list or dict that contains a "signatures" array);
  each inner element must have "mins" (the 64-bit hashes) and "ksize".
"""

from __future__ import annotations
import argparse
import concurrent.futures as futures
import gzip
import json
import logging
import math
import os
import re
import uuid
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


# -----------------------------
# Helpers
# -----------------------------

def _digits_for(n: int) -> int:
    if n <= 1:
        return 1
    return int(math.ceil(math.log10(n + 1)))

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

_SAMPLE_ID_RXES = [
    (re.compile(r'\.sig\.zip$', re.IGNORECASE), ''),
    (re.compile(r'\.(fa|fna|fasta)(\.gz)?$', re.IGNORECASE), ''),
]

def infer_sample_id(sigzip_path: Path) -> str:
    base = sigzip_path.name
    base = re.sub(r'\.sig\.zip$', '', base, flags=re.IGNORECASE)
    base = re.sub(r'\.(fa|fna|fasta)(\.gz)?$', '', base, flags=re.IGNORECASE)
    if '.' in base:
        base = base.split('.', 1)[0]
    return base

def _make_table(rows: List[Tuple[str, int, int, int]]) -> pa.Table:
    # rows: (sample_id, ksize, hash64, bucket)
    return pa.table({
        "sample_id": pa.array([r[0] for r in rows], type=pa.string()),
        "ksize":     pa.array([r[1] for r in rows], type=pa.int16()),
        "hash":      pa.array([r[2] for r in rows], type=pa.uint64()),
        "bucket":    pa.array([r[3] for r in rows], type=pa.int32()),
    })

def _write_parquet_chunk(out_dir: Path, worker_tag: str, bucket_num: int,
                         rows: List[Tuple[str, int, int, int]],
                         compression: str = "zstd") -> None:
    if not rows:
        return
    pad = _digits_for(max(bucket_num, 0))
    bucket_dir = out_dir / f"bucket={bucket_num:0{pad}d}" / f"worker={worker_tag}"
    _ensure_dir(bucket_dir)
    part = bucket_dir / f"part-{uuid.uuid4().hex}.parquet"
    pq.write_table(_make_table(rows), part, compression=compression, use_dictionary=True)


# -----------------------------
# Signature parsing
# -----------------------------

def _iter_sigfiles_in_zip(zf: zipfile.ZipFile) -> Iterable[Tuple[str, bytes]]:
    """
    Yield (inner_name, uncompressed_bytes) for any signatures/*.sig.gz (or *.sig / *.json) in the zip.
    Ignores SOURMASH-MANIFEST.csv and any non-signature entries.
    """
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = info.filename
        low = name.lower()
        if low.endswith("sourmash-manifest.csv"):
            continue
        # common sourmash archive layout: signatures/<md5>.sig.gz
        if not (low.endswith(".sig.gz") or low.endswith(".sig") or low.endswith(".json") or low.endswith(".json.gz")):
            continue
        with zf.open(info, 'r') as fh:
            data = fh.read()
            if low.endswith(".gz"):
                try:
                    data = gzip.decompress(data)
                except Exception:
                    logging.exception("gzip decompress failed for %s", name)
                    continue
            yield name, data

def _yield_signature_dicts(payload: Any) -> Iterable[Dict[str, Any]]:
    """
    Normalize various sourmash JSON layouts to yield inner signature dicts
    that contain "mins" and (usually) "ksize".
    - payload may be a dict with "signatures": [...]
    - payload may be a list of wrapper dicts, each with "signatures": [...]
    - payload may itself be a signature dict with "mins"
    """
    # Single signature dict?
    if isinstance(payload, dict):
        if "mins" in payload or "hashes" in payload:
            yield payload
            return
        sigs = payload.get("signatures")
        if isinstance(sigs, list):
            for s in sigs:
                if isinstance(s, dict):
                    yield s
            return
        # fallthrough: unknown dict layout -> nothing

    # List payload (common: [ { "class":"sourmash_signature", "signatures":[...] } ])
    if isinstance(payload, list):
        for elem in payload:
            if isinstance(elem, dict):
                if "mins" in elem or "hashes" in elem:
                    yield elem
                else:
                    sigs = elem.get("signatures")
                    if isinstance(sigs, list):
                        for s in sigs:
                            if isinstance(s, dict):
                                yield s

def _iter_hash_rows_from_sigzip(sigzip_path: Path, mod: int, include_ksizes: Optional[set[int]]) -> Iterable[Tuple[str, int, int, int]]:
    """
    Yield rows (sample_id, ksize, hash64, bucket) from a .sig.zip file.
    """
    sample_id = infer_sample_id(sigzip_path)
    try:
        with zipfile.ZipFile(sigzip_path, 'r') as zf:
            for _, data in _iter_sigfiles_in_zip(zf):
                try:
                    payload = json.loads(data.decode("utf-8"))
                except Exception:
                    logging.exception("JSON parse failed in %s", sigzip_path)
                    continue

                for sig in _yield_signature_dicts(payload):
                    # ksize may be int or str in some legacy dumps
                    try:
                        ksize = int(sig.get("ksize", 0))
                    except Exception:
                        ksize = 0
                    if include_ksizes and ksize not in include_ksizes:
                        continue

                    hv = sig.get("mins")
                    if hv is None:
                        hv = sig.get("hashes")
                    if not isinstance(hv, list):
                        continue

                    for h in hv:
                        # Ensure unsigned 64-bit
                        h64 = int(h) & ((1 << 64) - 1)
                        bucket = int(h64 % mod)
                        yield (sample_id, ksize, h64, bucket)
    except zipfile.BadZipFile:
        logging.error("Bad zip: %s", sigzip_path)
    except Exception as e:
        logging.exception("Error parsing %s: %s", sigzip_path, e)


# -----------------------------
# Worker
# -----------------------------

def _worker_export(args: Tuple[int, List[Path], Path, int, int, int, str, int, Optional[set[int]]]) -> Tuple[int, int]:
    """
    Process a chunk of .sig.zip paths and write Parquet parts.

    Returns: (files_processed, rows_emitted)
    """
    (wid, paths, out_root, mod, rows_per_flush, flush_every_n_files,
     compression, progress_log_every, include_ksizes) = args

    worker_tag = f"{wid:04d}"
    out_root = out_root.resolve()
    total_rows = 0
    file_count = 0
    # Per-bucket buffer
    buffer: DefaultDict[int, List[Tuple[str, int, int, int]]] = defaultdict(list)
    buffered_rows = 0

    def flush() -> None:
        nonlocal total_rows, buffered_rows
        if buffered_rows == 0:
            return
        for bkt, rows in list(buffer.items()):
            if not rows:
                continue
            _write_parquet_chunk(out_root, worker_tag, bkt, rows, compression=compression)
            total_rows += len(rows)
            buffered_rows -= len(rows)
            buffer[bkt].clear()

    for p in paths:
        for row in _iter_hash_rows_from_sigzip(p, mod, include_ksizes):
            bkt = row[3]
            buffer[bkt].append(row)
            buffered_rows += 1
            if buffered_rows >= rows_per_flush:
                flush()
        file_count += 1
        if (file_count % flush_every_n_files) == 0:
            flush()
        if (file_count % progress_log_every) == 0:
            logging.info("[worker %s] processed %d files; rows_written=%d (pending_buffer=%d)",
                         worker_tag, file_count, total_rows, buffered_rows)

    flush()
    logging.info("[worker %s] done. files=%d rows=%d", worker_tag, file_count, total_rows)
    return file_count, total_rows


# -----------------------------
# Main
# -----------------------------

def _chunk_evenly(lst: List[Path], n: int) -> List[List[Path]]:
    n = max(1, n)
    L = len(lst)
    q, r = divmod(L, n)
    chunks = []
    start = 0
    for i in range(n):
        end = start + q + (1 if i < r else 0)
        chunks.append(lst[start:end])
        start = end
    return [c for c in chunks if c] or [lst]

def scan_sigzips(input_root: Path, glob: str) -> List[Path]:
    return [p for p in sorted(input_root.rglob(glob)) if p.is_file() and p.name.endswith(".sig.zip")]

def main() -> None:
    ap = argparse.ArgumentParser(description="Export (sample_id, ksize, hash, bucket) from sourmash .sig.zip to Parquet.")
    ap.add_argument("--input-root", required=True, type=Path, help="Root directory containing bucket=XXXX subdirs")
    ap.add_argument("--out-root", required=True, type=Path, help="Output root for non-dedup Parquet dataset")
    ap.add_argument("--glob", default="**/*.sig.zip", help="Glob pattern to find .sig.zip (default: **/*.sig.zip)")
    ap.add_argument("--mod", type=int, default=521, help="Bucket modulus (prime recommended; default: 521)")
    ap.add_argument("--workers", type=int, default=max(1, os.cpu_count() or 1), help="Worker processes")
    ap.add_argument("--rows-per-flush", type=int, default=2_000_000, help="Flush buffered rows at this many rows")
    ap.add_argument("--flush-every-n-files", type=int, default=200, help="Also flush every N files")
    ap.add_argument("--compression", default="zstd", choices=["zstd","snappy","gzip","brotli","lz4","none"], help="Parquet compression")
    ap.add_argument("--progress-log-every", type=int, default=2000, help="Log progress every N files per worker")
    ap.add_argument("--max-files", type=int, default=0, help="If >0, cap number of input files (smoke tests)")
    ap.add_argument("--include-ksizes", type=str, default="", help="Optional comma-separated ksizes to include, e.g. 31,33")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    in_root = args.input_root
    out_root = args.out_root
    _ensure_dir(out_root)

    logging.info("Scanning for .sig.zip files under %s ...", in_root)
    sigzips = scan_sigzips(in_root, args.glob)
    if args.max_files and args.max_files > 0:
        sigzips = sigzips[:args.max_files]
    logging.info("Found %d .sig.zip files.", len(sigzips))
    if not sigzips:
        logging.warning("No input files found. Exiting.")
        return

    include_ksizes: Optional[set[int]] = None
    if args.include_ksizes.strip():
        try:
            include_ksizes = {int(x) for x in args.include_ksizes.split(",") if x.strip()}
            logging.info("Filtering to ksizes: %s", sorted(include_ksizes))
        except Exception:
            logging.exception("Failed to parse --include-ksizes; ignoring.")
            include_ksizes = None

    w = max(1, args.workers)
    chunks = _chunk_evenly(sigzips, w)
    pad = _digits_for(args.mod - 1 if args.mod > 1 else 1)
    logging.info("Using modulus=%d (bucket directory width ~ %d digits).", args.mod, pad)

    job_args = []
    for wid, ch in enumerate(chunks, 1):
        job_args.append((
            wid, ch, out_root, int(args.mod), int(args.rows_per_flush),
            int(args.flush_every_n_files), args.compression, int(args.progress_log_every),
            include_ksizes
        ))

    files_total = 0
    rows_total = 0
    with futures.ProcessPoolExecutor(max_workers=w) as ex:
        for fcount, rcount in ex.map(_worker_export, job_args):
            files_total += fcount
            rows_total += rcount

    logging.info("EXPORT DONE: files=%d rows=%d out_root=%s", files_total, rows_total, out_root)

if __name__ == "__main__":
    main()

