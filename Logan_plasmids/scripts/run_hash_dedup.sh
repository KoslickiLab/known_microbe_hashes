#!/bin/bash
cmd=(
    /usr/bin/time -v python "/scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_dedup.py"
    --non-dedup-root "/scratch/dmk333_new/known_microbe_hashes/Logan_plasmids/data/parquet_dumps"
    --out-root "/scratch/dmk333_new/known_microbe_hashes/Logan_plasmids/data/deduped"
    --threads 96
    --by-ksize
)
"${cmd[@]}" 2>&1 | tee run_hash_dedup.log
