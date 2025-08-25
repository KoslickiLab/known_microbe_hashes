#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingest partitioned FracMinHash Parquet shards into a DuckDB database.

Assumes input layout like:
  dataset_root/
    _MANIFEST.json
    ksize=31/
      part=0000/part.parquet
      part=0001/part.parquet
      ...
    ksize=33/...

Creates either:
  - one big table:   <schema>.<table>(ksize SMALLINT, hash UBIGINT)
  - per-ksize tables <schema>.hashes_<k>(hash UBIGINT)

Also records metadata (format, partition_bits, partition_mode, ksizes) so the
DB is self-describing.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import List, Optional

import duckdb  # pip install duckdb


def _escape_path(p: Path) -> str:
    # escape single quotes for SQL string literal
    return str(p).replace("'", "''")


def _discover_ksizes(root: Path) -> List[int]:
    ks = []
    for d in sorted(root.glob("ksize=*")):
        if d.is_dir():
            try:
                ks.append(int(d.name.split("=", 1)[1]))
            except Exception:
                pass
    return ks


def _read_manifest(root: Path) -> Optional[dict]:
    m = root / "_MANIFEST.json"
    if m.exists():
        try:
            with open(m, "r") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _ensure_meta_tables(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.meta_dataset (
            dataset_root       TEXT,
            format             TEXT,
            partition_bits     SMALLINT,
            partition_mode     TEXT,
            created_at         TIMESTAMP,
            ingested_at        TIMESTAMP DEFAULT NOW(),
            ksizes             TEXT,      -- CSV string for portability
            source_manifest    TEXT       -- raw JSON text for reference
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.meta_ksize (
            dataset_root   TEXT,
            ksize          SMALLINT,
            table_name     TEXT,
            rows_ingested  UBIGINT
        )
    """)


def _insert_meta(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    root: Path,
    manifest: Optional[dict],
    ksizes: List[int],
) -> None:
    fmt = manifest.get("format") if manifest else None
    pbits = manifest.get("partition_bits") if manifest else None
    pmode = manifest.get("partition_mode") if manifest else None
    created_at = manifest.get("created_at") if manifest else None
    ks_csv = ",".join(str(k) for k in ksizes)
    raw_json = json.dumps(manifest) if manifest else None
    con.execute(
        f"INSERT INTO {schema}.meta_dataset(dataset_root, format, partition_bits, partition_mode, created_at, ksizes, source_manifest) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [str(root), fmt, pbits, pmode, created_at, ks_csv, raw_json],
    )


def _ingest_per_ksize(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    root: Path,
    k: int,
    create_index: bool,
    analyze: bool,
    threads: Optional[int],
) -> int:
    pattern = root / f"ksize={k}" / "part=*/part.parquet"
    patt = _escape_path(pattern)
    # read all shards for this ksize; Parquet reader supports globs
    # we rely on projection pushdown to only read the 'hash' column from each file
    # and on DuckDB to parallelize the scan. :contentReference[oaicite:2]{index=2}
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {schema}.hashes_{k} AS
        SELECT CAST(hash AS UBIGINT) AS hash
        FROM read_parquet('{patt}')
        """
    )
    if analyze:
        con.execute(f"ANALYZE {schema}.hashes_{k}")
    if create_index:
        # ART index; see DuckDB indexing docs (can be large to build; ensure enough RAM). :contentReference[oaicite:3]{index=3}
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_hashes_{k}_hash ON {schema}.hashes_{k}(hash)")
    # count rows ingested (fast with stats/zonemaps)
    rows = con.execute(f"SELECT COUNT(*) FROM {schema}.hashes_{k}").fetchone()[0]
    return int(rows)


def _ingest_single_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    root: Path,
    ksizes: List[int],
    create_index: bool,
    analyze: bool,
) -> int:
    # Use Hive partitioning to recover 'ksize' (and 'part' if we wanted it).
    # We filter to the chosen ksizes.
    patt = _escape_path(root / "ksize=*/part=*/part.parquet")
    klist = ",".join(str(k) for k in ksizes)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {schema}.{table} AS
        SELECT CAST(ksize AS SMALLINT) AS ksize,
               CAST(hash  AS UBIGINT)  AS hash
        FROM read_parquet('{patt}', hive_partitioning=true)  -- parse directory names as columns
        WHERE ksize IN ({klist})
        """
    )  # Hive partitioning reference. :contentReference[oaicite:4]{index=4}
    if analyze:
        con.execute(f"ANALYZE {schema}.{table}")
    if create_index:
        # A composite index (ksize, hash) helps when the table stores multiple ksizes
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ksize_hash ON {schema}.{table}(ksize, hash)")
    rows = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    return int(rows)


def main():
    ap = argparse.ArgumentParser(description="Ingest reduced FracMinHash Parquet shards into DuckDB.")
    ap.add_argument("--input", required=True, help="Path to reduced dataset root (e.g., .../unique_parquet).")
    ap.add_argument("--db", required=True, help="Output DuckDB database file (will be created if missing).")
    ap.add_argument("--schema", default="hashes", help="Target schema name (default: hashes).")
    ap.add_argument("--mode", choices=["single", "per-ksize"], default="single",
                    help="single: one table with ksize column; per-ksize: one table per k.")
    ap.add_argument("--table", default="hashes", help="Table name for --mode single (default: hashes).")
    ap.add_argument("--ksizes", type=int, nargs="*", help="Restrict to these ksizes (default: discover all).")
    ap.add_argument("--threads", type=int, default=None, help="PRAGMA threads (default: DuckDB chooses).")
    ap.add_argument("--create-index", action="store_true", help="Create index(es) after load.")
    ap.add_argument("--analyze", action="store_true", help="Run ANALYZE after load (collect stats).")
    args = ap.parse_args()

    root = Path(args.input).resolve()
    if not root.exists():
        raise SystemExit(f"Input not found: {root}")

    # Connect & set threads
    con = duckdb.connect(database=str(Path(args.db).resolve()))
    if args.threads and args.threads > 0:
        con.execute(f"PRAGMA threads={int(args.threads)}")

    _ensure_meta_tables(con, args.schema)

    manifest = _read_manifest(root)
    ksizes = args.ksizes if args.ksizes else _discover_ksizes(root)
    if not ksizes:
        raise SystemExit(f"No ksize=* directories found under {root}")

    # Store dataset-level metadata
    _insert_meta(con, args.schema, root, manifest, ksizes)

    total_rows = 0
    if args.mode == "per-ksize":
        for k in ksizes:
            rows = _ingest_per_ksize(
                con=con,
                schema=args.schema,
                root=root,
                k=k,
                create_index=args.create_index,
                analyze=args.analyze,
                threads=args.threads,
            )
            total_rows += rows
            con.execute(
                f"INSERT INTO {args.schema}.meta_ksize(dataset_root, ksize, table_name, rows_ingested) VALUES (?,?,?,?)",
                [str(root), int(k), f"{args.schema}.hashes_{k}", rows],
            )
    else:
        rows = _ingest_single_table(
            con=con,
            schema=args.schema,
            table=args.table,
            root=root,
            ksizes=ksizes,
            create_index=args.create_index,
            analyze=args.analyze,
        )
        total_rows = rows
        # one meta row per k with same table_name
        for k in ksizes:
            con.execute(
                f"INSERT INTO {args.schema}.meta_ksize(dataset_root, ksize, table_name, rows_ingested) VALUES (?,?,?,?)",
                [str(root), int(k), f"{args.schema}.{args.table}", None],
            )

    # Final sanity output
    print({
        "dataset_root": str(root),
        "mode": args.mode,
        "schema": args.schema,
        "table": args.table if args.mode == "single" else None,
        "ksizes": ksizes,
        "rows_ingested": int(total_rows),
        "indexed": bool(args.create_index),
    })


if __name__ == "__main__":
    main()
