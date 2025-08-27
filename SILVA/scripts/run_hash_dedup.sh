#!/bin/bash
ATBScriptsLoc=/scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/scripts
cmd=(
    /usr/bin/time -v python "${ATBScriptsLoc}/atb_hash_dedup.py"
    --non-dedup-root "../data/parquet_dumps"
    --out-root "../data/deduped"
    --threads 96
    --by-ksize
)
"${cmd[@]}" > run_hash_dedup.log 2>&1
