#!/bin/bash
/usr/bin/time -v python atb_hash_dedup.py --non-dedup-root /scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/data/atb_sketches_parquet --out-root /scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/data/atb_sketches_dedup --threads 96 --with-counts --by-ksize > run_hash_dedup.log 2>&1
