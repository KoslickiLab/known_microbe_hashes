#!/bin/bash
cmd=(
    /usr/bin/time -v python "/scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_export.py"
    --input-root "/scratch/dmk333_new/known_microbe_hashes/Logan_obelisks/sketches"
    --out-root "/scratch/dmk333_new/known_microbe_hashes/Logan_obelisks/data/parquet_dumps"
    --glob "*.sig.zip"
    --mod 64
    --workers 64
    --rows-per-flush 2000000
    --compression zstd
    --progress-log-every 1
)
"${cmd[@]}" 2>&1 | tee run_hash_export.log
