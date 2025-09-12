#!/bin/bash
cmd=(
    /usr/bin/time -v python "/scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_ingest_duckdb.py"
    --dedup-root "/scratch/dmk333_new/known_microbe_hashes/Logan_obelisks/data/deduped"
    --db "/scratch/dmk333_new/known_microbe_hashes/Logan_obelisks/data/Obelisks_unique_hashes.db"
    --table unique_hashes
    --mode create
    --analyze
)
"${cmd[@]}" 2>&1 | tee create_duckdb_from_dedup.log
