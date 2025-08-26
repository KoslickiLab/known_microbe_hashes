#!/usr/bin/env python3
"""
Ingest WGS hash Parquet shards (ksize=.../part=*/part.parquet) into a DuckDB table.

Assumptions
-----------
- Each Parquet file contains a single column: `hash` (64-bit unsigned integer).
- Directories follow Hive-style partitions:
    <root>/ksize=31/part=000/part.parquet
  (The 'part' level is optional for the final schema; include with --with-part.)

What the script does
--------------------
- Globs all Parquet files under --parquet-root and --pattern (default: "part=*/part.parquet").
- Uses DuckDB's read_parquet() with hive_partitioning=true to discover partition columns.
- Ensures the output table has:
    ksize  SMALLINT NOT NULL
    hash   UBIGINT  NOT NULL
    [part  VARCHAR]          # included only if --with-part is provided and available
- Supports modes:
    replace (default): drop & recreate table from the Parquet scan
    append:             insert rows into an existing or newly created table
    create:             fail if the table already exists
- Indexing (unless --no-index):
    CREATE INDEX idx_<table>_hash ON <table>(hash);
    CREATE INDEX idx_<table>_ksize_hash ON <table>(ksize, hash);  # if helpful and supported
- Optional ANALYZE after ingest.

Examples
--------
Replace (rebuild) from a ksize=31 directory:
  python wgs_hash_ingest_duckdb.py \
    --parquet-root /scratch/dmk333_new/known_microbe_hashes/GenBankWGS/data/wgs_sketches_dedup/ksize=31 \
    --db /scratch/dmk333_new/known_microbe_hashes/wgs.duckdb \
    --table wgs_hashes \
    --mode replace \
    --threads 96

Append more parts later (skip indexing if you’ll rebuild again soon):
  python wgs_hash_ingest_duckdb.py \
    --parquet-root /scratch/dmk333_new/.../ksize=31 \
    --db /scratch/dmk333_new/known_microbe_hashes/wgs.duckdb \
    --table wgs_hashes \
    --mode append \
    --no-index

If your path DOESN'T contain 'ksize=...' (e.g., you point at the parent dir):
  python wgs_hash_ingest_duckdb.py \
    --parquet-root /scratch/dmk333_new/.../wgs_sketches_dedup/ksize=31 \
    --pattern "part=*/part.parquet" \
    --ksize 31 \
    --db /scratch/dmk333_new/known_microbe_hashes/wgs.duckdb \
    --table wgs_hashes
"""

from __future__ import annotations

import argparse
import glob as globmod
import logging
import os
from pathlib import Path
from typing import List, Tuple

import duckdb


def _glob_files(root: Path, pattern: str) -> List[str]:
    files = globmod.glob(str(root / pattern), recursive=True)
    return sorted(files)


def _detect_partitions(
    con: duckdb.DuckDBPyConnection, glob_expr: str
) -> Tuple[bool, bool]:
    """
    Detect whether the dataset exposes Hive partition columns 'ksize' and 'part'.
    We only care that they are present; types will be normalized via CAST in SELECT.

    Returns
    -------
    (has_hive_ksize, has_hive_part)
    """
    q = (
        "DESCRIBE SELECT * "
        f"FROM read_parquet('{glob_expr}', union_by_name=true, hive_partitioning=1) "
        "LIMIT 0;"
    )
    rows = con.execute(q).fetchall()
    cols = {r[0].lower() for r in rows}
    return ("ksize" in cols, "part" in cols)


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?;",
        [table],
    ).fetchone()
    return bool(row)


def _create_table(
    con: duckdb.DuckDBPyConnection, table: str, include_part: bool
) -> None:
    cols = [
        "ksize SMALLINT NOT NULL",
        "hash UBIGINT NOT NULL",
    ]
    if include_part:
        cols.append("part VARCHAR")
    ddl = f"CREATE TABLE {table} ({', '.join(cols)});"
    con.execute(ddl)


def _ingest(
    con: duckdb.DuckDBPyConnection,
    table: str,
    glob_expr: str,
    mode: str,
    include_part: bool,
    has_hive_ksize: bool,
    has_hive_part: bool,
    cli_ksize: int | None,
) -> int:
    """
    Ingest into <table> using read_parquet(). Returns row count after ingest.
    """
    # Build SELECT list with robust CASTs.
    select_cols = []

    if has_hive_ksize:
        select_cols.append("CAST(ksize AS SMALLINT) AS ksize")
    else:
        if cli_ksize is None:
            raise SystemExit(
                "No 'ksize' partition detected in the path and --ksize was not provided."
            )
        select_cols.append(f"CAST({int(cli_ksize)} AS SMALLINT) AS ksize")

    select_cols.append("CAST(hash AS UBIGINT) AS hash")

    if include_part and has_hive_part:
        # Keep as text; some datasets use hex-like labels (e.g., '35b'), which are not castable to INTEGER.
        select_cols.append("CAST(part AS VARCHAR) AS part")

    select_clause = ",\n       ".join(select_cols)

    # Source scan
    from_clause = (
        f"FROM read_parquet('{glob_expr}', union_by_name=true, hive_partitioning=1)"
    )

    if mode in ("replace", "create"):
        if mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table};")
        # Use CREATE TABLE AS SELECT for speed (no huge INSERT transaction).
        sql = f"""
            CREATE TABLE {table} AS
            SELECT {select_clause}
            {from_clause};
        """
        con.execute(sql)

    elif mode == "append":
        if not _table_exists(con, table):
            _create_table(con, table, include_part and has_hive_part)
        sql = f"""
            INSERT INTO {table}
            SELECT {select_clause}
            {from_clause};
        """
        con.execute(sql)
    else:
        raise ValueError(f"Unknown mode: {mode!r}")

    # Return final row count
    n = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    return int(n)


def _create_indexes(
    con: duckdb.DuckDBPyConnection, table: str
) -> None:
    """
    Helpful indexes for point lookups:
      - (hash)
      - (ksize, hash) if supported (try/catch in case of older DuckDB builds)
    """
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_hash ON {table}(hash);")
    try:
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_ksize_hash ON {table}(ksize, hash);"
        )
    except Exception:
        # Composite index not available in some builds; single-column index still helps.
        pass


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest WGS hash Parquet shards into DuckDB (ksize + hash [+part])."
    )
    ap.add_argument(
        "--parquet-root",
        required=True,
        type=Path,
        help="Directory containing ksize=.../part=*/part.parquet",
    )
    ap.add_argument(
        "--pattern",
        default="part=*/part.parquet",
        help='Glob (relative to --parquet-root). Default: "part=*/part.parquet"',
    )
    ap.add_argument(
        "--db", required=True, type=Path, help="DuckDB database file path (created if absent)"
    )
    ap.add_argument(
        "--table",
        default="wgs_hashes",
        help="Destination table name (default: wgs_hashes)",
    )
    ap.add_argument(
        "--mode",
        choices=["replace", "append", "create"],
        default="replace",
        help="replace: drop & rebuild; append: insert rows; create: fail if exists",
    )
    ap.add_argument(
        "--threads",
        type=int,
        default=max(1, os.cpu_count() or 1),
        help="DuckDB threads (default: CPU count)",
    )
    ap.add_argument(
        "--ksize",
        type=int,
        default=None,
        help="K-mer size to embed if not discoverable from Hive partition path (e.g., 31).",
    )
    ap.add_argument(
        "--with-part",
        action="store_true",
        help="Include the Hive 'part' partition column (VARCHAR) in the table schema.",
    )
    ap.add_argument(
        "--no-index",
        action="store_true",
        help="Skip index creation (by default indexes on hash and (ksize, hash) are created).",
    )
    ap.add_argument(
        "--analyze",
        action="store_true",
        help="Run ANALYZE after ingest (collects stats for the table).",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    # Sanity check: files exist
    files = _glob_files(args.parquet_root, args.pattern)
    if not files:
        logging.error(
            "No Parquet files matched %s under %s",
            args.pattern,
            args.parquet_root,
        )
        raise SystemExit(2)
    logging.info("Found %d Parquet file(s) to ingest.", len(files))

    glob_expr = str((args.parquet_root / args.pattern).as_posix())

    con = duckdb.connect(str(args.db))
    con.execute(f"PRAGMA threads={int(args.threads)};")

    # Guard: create-mode should fail if table exists
    if args.mode == "create" and _table_exists(con, args.table):
        raise SystemExit(
            f"Table {args.table!r} already exists (use --mode replace or --mode append)."
        )

    # Detect available Hive partition columns
    has_hive_ksize, has_hive_part = _detect_partitions(con, glob_expr)
    logging.info(
        "Detected Hive partitions: ksize=%s, part=%s",
        has_hive_ksize,
        has_hive_part,
    )

    # Ingest
    logging.info(
        "Starting ingest (mode=%s) into %s table %s ...",
        args.mode,
        args.db,
        args.table,
    )
    total_rows = _ingest(
        con=con,
        table=args.table,
        glob_expr=glob_expr,
        mode=args.mode,
        include_part=args.with_part,
        has_hive_ksize=has_hive_ksize,
        has_hive_part=has_hive_part,
        cli_ksize=args.ksize,
    )
    logging.info("Ingest complete. Rows now in table: %s", f"{total_rows:,}")

    # Optional indexes & analyze
    if not args.no_index:
        logging.info("Creating index(es) ...")
        _create_indexes(con, args.table)

    if args.analyze:
        logging.info("Running ANALYZE on %s ...", args.table)
        con.execute(f"ANALYZE {args.table};")

    logging.info("Done.")
    con.close()


if __name__ == "__main__":
    main()
