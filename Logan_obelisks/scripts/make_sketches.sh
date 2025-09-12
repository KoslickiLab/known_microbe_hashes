#!/bin/bash
cmd=(
	/usr/bin/time -v /scratch/dmk333_new/known_microbe_hashes/shared_scripts/local_sketcher.py
	--input-root /scratch/dmk333_new/known_microbe_hashes/Logan_obelisks/data
	--out-root /scratch/dmk333_new/known_microbe_hashes/Logan_obelisks/sketches
	--tmp-dir /scratch/tmp
	--n-shards 64
	--workers 64
)
"${cmd[@]}" 2>&1 | tee make_sketches.log
