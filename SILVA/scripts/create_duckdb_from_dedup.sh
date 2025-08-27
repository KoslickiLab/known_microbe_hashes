#!/bin/bash
ATBScriptsLoc=/scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/scripts
cmd=(
    /usr/bin/time -v python "${ATBScriptsLoc}/atb_hash_ingest_duckdb.py"
    --dedup-root "../data/deduped"
    --db ../data/SILVA_unique_hashes.db
    --table unique_hashes
    --mode create
    --analyze
)
"${cmd[@]}" 2>&1 | tee create_duckdb_from_dedup.log
