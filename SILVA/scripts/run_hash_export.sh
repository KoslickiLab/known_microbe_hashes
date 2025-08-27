#!/bin/bash
ATBScriptsLoc=/scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/scripts
cmd=(
    /usr/bin/time -v python "${ATBScriptsLoc}/atb_hash_export.py"
    --input-root "../data"
    --out-root "../data"
    --glob "../data/*.sig.zip"
    --mod 521
    --workers 4
    --rows-per-flush 2000000
    --compression zstd
    --progress-log-every 1
)
"${cmd[@]}" > run_hash_export.log 2>&1
