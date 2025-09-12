#!/bin/bash
cmd=(
	/usr/bin/time -v /scratch/dmk333_new/known_microbe_hashes/shared_scripts/local_sketcher.py
	--input-root /scratch/dmk333_new/known_microbe_hashes/GTDB/data/gtdb_genomes_reps_r226
	--out-root /scratch/dmk333_new/known_microbe_hashes/GTDB/data/sketches
	--tmp-dir /scratch/tmp
	--n-shards 512
	--workers 96
)
"${cmd[@]}" 2>&1 | tee make_sketches.log
