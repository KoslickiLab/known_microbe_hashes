#!/bin/bash
/usr/bin/time -v python /scratch/dmk333_new/known_microbe_hashes/shared_scripts/hash_dedup.py ---non-dedup-root /scratch/genbank_genomes_all/genomes_all_sketches_parquet --out-root /scratch/genbank_genomes_all/genomes_all_sketches_dedup --threads 96 --by-ksize > dedup_genbank_genomes_all.log 2>&1
