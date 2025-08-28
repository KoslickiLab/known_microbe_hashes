#!/usr/bin/env python3
"""
Deduplicate hashes per bucket using DuckDB.

Input:  non-dedup Parquet from atb_hash_export.py
Output: per-bucket Parquet with DISTINCT hashes
        (optionally DISTINCT by (ksize, hash) if --by-ksize)

Non-dedup layout:
  root/
    bucket=<n>/worker=XXXX/*.parquet

Dedup layout:
  out_root/
    bucket=<n>/unique.parquet
"""

from __future__ import annotations
import argparse
import logging
import os
from pathlib import Path
import duckdb

def list_buckets(non_dedup_root: Path) -> list[int]:
    buckets = []
    for p in non_dedup_root.glob("bucket=*"):
        if p.is_dir():
            try:
                b = int(p.name.split("=", 1)[1])
                buckets.append(b)
            except Exception:
                pass
    return sorted(set(buckets))

def main() -> None:
    ap = argparse.ArgumentParser(description="Deduplicate hashes per bucket from non-dedup Parquet dataset.")
    ap.add_argument("--non-dedup-root", required=True, type=Path, help="Path to non-dedup Parquet dataset root")
    ap.add_argument("--out-root", required=True, type=Path, help="Output root for deduplicated dataset")
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1), help="DuckDB threads")
    ap.add_argument("--with-counts", action="store_true",
                    help="Also include n_samples (distinct sample_id per hash, or per (ksize,hash) if --by-ksize).")
    ap.add_argument("--by-ksize", action="store_true", help="Deduplicate within each ksize (group by (ksize, hash)).")
    ap.add_argument("--max-buckets", type=int, default=0, help="If >0, limit number of buckets (smoke tests)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    (args.out_root).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(args.threads)};")

    buckets = list_buckets(args.non_dedup_root)
    if args.max_buckets and args.max_buckets > 0:
        buckets = buckets[:args.max_buckets]

    logging.info("Found %d buckets.", len(buckets))

    for b in buckets:
        in_glob = str((args.non_dedup_root / f"bucket={b}" / "**" / "*.parquet").as_posix())
        out_dir = args.out_root / f"bucket={b}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "unique.parquet"

        if args.by_ksize:
            # Dedup per (ksize, hash)
            if args.with_counts:
                sql = f"""
                COPY (
                  SELECT
                    CAST(ksize AS SMALLINT) AS ksize,
                    CAST(hash AS UBIGINT) AS hash,
                    CAST({b} AS INTEGER) AS bucket,
                    COUNT(DISTINCT sample_id) AS n_samples
                  FROM read_parquet('{in_glob}')
                  GROUP BY ksize, hash
                ) TO '{out_file.as_posix()}' (FORMAT PARQUET);
                """
            else:
                sql = f"""
                COPY (
                  SELECT DISTINCT
                    CAST(ksize AS SMALLINT) AS ksize,
                    CAST(hash AS UBIGINT) AS hash,
                    CAST({b} AS INTEGER) AS bucket
                  FROM read_parquet('{in_glob}')
                ) TO '{out_file.as_posix()}' (FORMAT PARQUET);
                """
        else:
            # Dedup by hash only (across all ksizes)
            if args.with_counts:
                sql = f"""
                COPY (
                  SELECT
                    CAST(hash AS UBIGINT) AS hash,
                    CAST({b} AS INTEGER) AS bucket,
                    COUNT(DISTINCT sample_id) AS n_samples
                  FROM read_parquet('{in_glob}')
                  GROUP BY hash
                ) TO '{out_file.as_posix()}' (FORMAT PARQUET);
                """
            else:
                sql = f"""
                COPY (
                  SELECT DISTINCT
                    CAST(hash AS UBIGINT) AS hash,
                    CAST({b} AS INTEGER) AS bucket
                  FROM read_parquet('{in_glob}')
                ) TO '{out_file.as_posix()}' (FORMAT PARQUET);
                """

        logging.info("Bucket %d: writing %s", b, out_file)
        con.execute(sql)

    logging.info("DEDUP DONE: out_root=%s", args.out_root)

if __name__ == "__main__":
    main()

