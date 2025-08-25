#!/bin/bash
/usr/bin/time -v python atb_hash_ingest_duckdb.py --dedup-root /scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/data/atb_sketches_dedup --db ../data/atb_unique_hashes.db --threads 96 --mode create --analyze > atb_hash_ingest_duckdb.log 2>&1
