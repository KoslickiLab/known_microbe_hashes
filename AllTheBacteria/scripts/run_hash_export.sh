#!/bin/bash
/usr/bin/time -v python atb_hash_export.py --input-root /scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/data/atb_sketches --out-root /scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/data/atb_sketches_parquet --mod 521 --rows-per-flush 2000000 --compression zstd --progress-log-every 1000 --workers 96 > run_hash_export.log 2>&1 
