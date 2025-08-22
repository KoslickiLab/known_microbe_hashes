#!/bin/bash
/usr/bin/time -v python ingest_hashes_to_duckdb.py --input /scratch/dmk333_new/GenBank_WGS_analysis/analysis/reduced_parquet --db wgs_database.db --mode per-ksize --threads 64 --create-index --analyze
