#!/bin/bash
cmd=(
    /usr/bin/time -v python "/scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_export.py"
    --input-root "/scratch/dmk333_new/known_microbe_hashes/Serratus_viruses/data/sketches"
    --out-root "/scratch/dmk333_new/known_microbe_hashes/Serratus_viruses/data/parquet_dumps"
    --glob "*.sig.zip"
    --mod 521
    --workers 64
    --rows-per-flush 2000000
    --compression zstd
    --progress-log-every 1
)
"${cmd[@]}" 2>&1 | tee run_hash_export.log
