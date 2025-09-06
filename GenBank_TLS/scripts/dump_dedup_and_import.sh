#!/bin/bash
set -eou pipefail
# export
/usr/bin/time -v python /scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_export.py --input-root /scratch/genbank_tls/tls_sketches --out-root /scratch/genbank_tls/tls_sketches_parquet --mod 512 --workers 96 --compression zstd --progress-log-every 1000 > export_tls.log 2>&1

# dedup
/usr/bin/time -v python /scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_dedup.py --non-dedup-root /scratch/genbank_tls/tls_sketches_parquet --out-root /scratch/genbank_tls/tls_sketches_dedup --threads 96 --by-ksize > dedup_tls.log 2>&1

# import
/usr/bin/time -v /scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_ingest_duckdb.py --dedup-root /scratch/genbank_tls/tls_sketches_dedup --db genbank_tls_unique_hashes.db --threads 96 --mode create --analyze > make_duck_db_from_dedup.log 2>&1
