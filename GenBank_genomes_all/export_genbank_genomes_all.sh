#!/bin/bash
/usr/bin/time -v python /scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_export.py --input-root /scratch/genbank_genomes_all/genomes_all_sketches --out-root /scratch/genbank_genomes_all/genomes_all_sketches_parquet --mod 512 --workers 96 --compression zstd --progress-log-every 1000 > export_genbank_genomes_all.log 2>&1 
