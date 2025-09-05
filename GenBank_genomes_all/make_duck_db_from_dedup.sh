#!/bin/bash
/usr/bin/time -v /scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_ingest_duckdb.py --dedup-root /scratch/genbank_genomes_all/genomes_all_sketches_dedup --db genbank_genomes_all_unique_hashes.db --threads 96 --mode create --analyze > make_duck_db_from_dedup.log 2>&1
