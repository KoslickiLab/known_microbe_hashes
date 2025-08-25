#!/usr/bin/env python3
"""
Ingest deduplicated ATB hashes (per-bucket Parquet) into a DuckDB database.

The script autodetects whether the input dataset was produced:
  - with --by-ksize  (columns: ksize, hash, bucket [, n_samples])
  - without --by-ksize (columns: hash, bucket [, n_samples])

It creates or appends to a DuckDB table with the matching schema, bulk-imports
all files via read_parquet(), and ONLY AFTER the import:
  - creates an index on (hash)            # helps EXISTS / point lookups on hash
  - (optionally) ANALYZE the table to collect stats

Types:
  ksize: SMALLINT          (if present)
  hash:  UBIGINT           (64-bit unsigned)
  bucket: INTEGER
  n_samples: BIGINT        (if present)

Examples
--------
Replace the table (rebuild):
  python analysis/atb_hash_ingest_duckdb.py \
    --dedup-root /path/parquet_dedup \
    --db /path/atb.duckdb \
    --table unique_hashes \
    --mode replace \
    --threads 96

Append (if you ingested a subset previously):
  python analysis/atb_hash_ingest_duckdb.py \
    --dedup-root /path/parquet_dedup \
    --db /path/atb.duckdb \
    --table unique_hashes \
    --mode append \
    --threads 96

Skip index creation:
  python analysis/atb_hash_ingest_duckdb.py \
    --dedup-root /path/parquet_dedup \
    --db /path/atb.duckdb \
    --table unique_hashes \
    --mode replace \
    --no-index
"""

from __future__ import annotations
import argparse
import glob as globmod
import logging
import os
from pathlib import Path
from typing import List, Tuple

import duckdb


def _glob_dedup_files(root: Path, pattern: str) -> List[str]:
    # DuckDB handles wildcards, but we do a quick sanity check here for UX.
    # Default pattern targets one file per bucket: bucket=*/unique.parquet
    files = globmod.glob(str(root / pattern), recursive=True)
    return sorted(files)


def _detect_schema(con: duckdb.DuckDBPyConnection, glob_expr: str) -> Tuple[bool, bool]:
    """
    Return (has_ksize, has_counts) by inspecting the Parquet schema via DuckDB.
    We pass union_by_name=true so the detection works across files.
    """
    # DESCRIBE returns rows with: column_name, column_type, null, key, default, extra
    q = f"DESCRIBE SELECT * FROM read_parquet('{glob_expr}', union_by_name=true) LIMIT 0;"
    rows = con.execute(q).fetchall()
    cols = {r[0].lower() for r in rows}
    has_ksize = "ksize" in cols
    has_counts = "n_samples" in cols
    return has_ksize, has_counts


def _create_table(con: duckdb.DuckDBPyConnection, table: str, has_ksize: bool, has_counts: bool) -> None:
    cols = []
    if has_ksize:
        cols.append("ksize SMALLINT")
    cols.append("hash UBIGINT NOT NULL")
    cols.append("bucket INTEGER NOT NULL")
    if has_counts:
        cols.append("n_samples BIGINT")
    ddl = f"CREATE TABLE {table} ({', '.join(cols)});"
    con.execute(ddl)


def _ingest(con: duckdb.DuckDBPyConnection, table: str, glob_expr: str, mode: str,
            has_ksize: bool, has_counts: bool) -> int:
    """
    Perform the actual import using read_parquet(). Returns number of rows inserted.
    """
    select_cols = []
    if has_ksize:
        select_cols.append("CAST(ksize AS SMALLINT) AS ksize")
    select_cols.append("CAST(hash AS UBIGINT) AS hash")
    select_cols.append("CAST(bucket AS INTEGER) AS bucket")
    if has_counts:
        select_cols.append("CAST(n_samples AS BIGINT) AS n_samples")

    select_clause = ",\n       ".join(select_cols)

    if mode in ("replace", "create"):
        # Build from scratch
        # Use CREATE TABLE AS SELECT for speed and to avoid a huge INSERT transaction.
        create_cols = []
        if has_ksize:
            create_cols.append("ksize SMALLINT")
        create_cols.append("hash UBIGINT")
        create_cols.append("bucket INTEGER")
        if has_counts:
            create_cols.append("n_samples BIGINT")

        # Some DuckDB versions support CREATE OR REPLACE TABLE; to be safe, drop first.
        if mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table};")

        sql = f"""
        CREATE TABLE {table} AS
        SELECT {select_clause}
        FROM read_parquet('{glob_expr}', union_by_name=true);
        """
        con.execute(sql)
    elif mode == "append":
        # Ensure table exists
        exists = con.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}'"
        ).fetchone()
        if not exists:
            _create_table(con, table, has_ksize, has_counts)
        sql = f"""
        INSERT INTO {table}
        SELECT {select_clause}
        FROM read_parquet('{glob_expr}', union_by_name=true);
        """
        con.execute(sql)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    # Row count
    n = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    return int(n)


def _create_indexes(con: duckdb.DuckDBPyConnection, table: str, has_ksize: bool) -> None:
    """
    Create helpful indexes for point lookups *after* import:
      - index on hash
      - (optional) composite (ksize, hash) if available; otherwise, the hash index alone
    """
    # Always helpful for EXISTS(hash=...) or WHERE hash IN (...)
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_hash ON {table}(hash);")

    # Try a composite index if ksize exists (helps WHERE ksize=31 AND hash=...).
    # Not all DuckDB builds support multi-column indexes; try/catch and fall back silently.
    if has_ksize:
        try:
            con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ksize_hash ON {table}(ksize, hash);")
        except Exception:
            # Single-column index on hash already created; fine for most lookups.
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest deduplicated ATB hashes (Parquet) into DuckDB.")
    ap.add_argument("--dedup-root", required=True, type=Path,
                    help="Root of deduplicated dataset (contains bucket=*/unique.parquet)")
    ap.add_argument("--db", required=True, type=Path, help="DuckDB database file path")
    ap.add_argument("--table", default="unique_hashes", help="Destination table name (default: unique_hashes)")
    ap.add_argument("--pattern", default="bucket=*/unique.parquet",
                    help="Glob under --dedup-root to find files (default: bucket=*/unique.parquet)")
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1), help="DuckDB threads")
    ap.add_argument("--mode", choices=["replace", "append", "create"], default="replace",
                    help="replace: rebuild table; append: add rows; create: fail if exists")
    ap.add_argument("--no-index", action="store_true",
                    help="Do not create indexes after import (by default an index on hash is created).")
    ap.add_argument("--analyze", action="store_true",
                    help="Run ANALYZE on the table after import (collects stats).")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Quick sanity check: files exist?
    files = _glob_dedup_files(args.dedup_root, args.pattern)
    if not files:
        logging.error("No files matched %s under %s", args.pattern, args.dedup_root)
        return
    logging.info("Found %d Parquet files to ingest.", len(files))

    glob_expr = str((args.dedup_root / args.pattern).as_posix())

    con = duckdb.connect(str(args.db))
    con.execute(f"PRAGMA threads={int(args.threads)};")

    # Detect schema
    has_ksize, has_counts = _detect_schema(con, glob_expr)
    logging.info("Detected schema: has_ksize=%s, has_counts=%s", has_ksize, has_counts)

    # Guard for mode=create when table exists
    if args.mode == "create":
        exists = con.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{args.table}'"
        ).fetchone()
        if exists:
            raise SystemExit(f"Table {args.table!r} already exists (use --mode replace or --mode append).")

    # Ingest
    logging.info("Starting ingest (mode=%s) into %s table %s ...", args.mode, args.db, args.table)
    total_rows = _ingest(con, args.table, glob_expr, args.mode, has_ksize, has_counts)
    logging.info("Ingest complete. Rows now in table: %s", f"{total_rows:,}")

    # Indexing (only if helpful for point lookups on hash)
    if not args.no_index:
        logging.info("Creating index(es) for hash lookups ...")
        _create_indexes(con, args.table, has_ksize)

    if args.analyze:
        logging.info("Running ANALYZE on %s ...", args.table)
        con.execute(f"ANALYZE {args.table};")

    logging.info("Done.")

if __name__ == "__main__":
    main()

