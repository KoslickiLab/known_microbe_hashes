#!/bin/bash
# python /scratch/dmk333_new/GenBank_WGS_analysis/analysis/wgs_sketch_union_ChatGPT.py extract --input /mnt/ramdisk/wgs_sketches --out /mnt/ramdisk/extracted_hashes --processes 128 --partition-bits 10 --partition-mode low --max-in-flight 200
# Don't need so many buckets, but the partition-bits=10 is what was used on the GPU server
python /scratch/dmk333_new/GenBank_WGS_analysis/analysis/wgs_sketch_union_ChatGPT.py extract --input /mnt/ramdisk/wgs_sketches --out /mnt/ramdisk/extracted_hashes --processes 128 --partition-bits 8 --partition-mode low --max-in-flight 200
