#!/bin/bash
python wgs_hash_ingest_duckdb.py --parquet-root ../data/wgs_sketches_dedup/ksize\=31/ --db ../data/wgs_unique_hashes.db --table unique_hashes --mode create --threads 45 --ksize 31 --analyze
