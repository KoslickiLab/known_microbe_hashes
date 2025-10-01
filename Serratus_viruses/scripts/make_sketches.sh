#!/bin/bash
cmd=(
	/usr/bin/time -v /scratch/dmk333_new/known_microbe_hashes/shared_scripts/local_sketcher.py
	--input-root /scratch/dmk333_new/known_microbe_hashes/Serratus_viruses/data/sequences
	--out-root /scratch/dmk333_new/known_microbe_hashes/Serratus_viruses/data/sketches
	--tmp-dir /scratch/tmp
	--n-shards 512
	--workers 96
)
"${cmd[@]}" 2>&1 | tee make_sketches.log
