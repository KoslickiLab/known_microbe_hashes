#!/bin/bash
python wgs_sketch_union_ChatGPT.py reduce --spool /mnt/ramdisk/extracted_hashes/spool --out /scratch/dmk333_new/GenBank_WGS_analysis/analysis/reduced_parquet --format parquet --mem-limit-gb 2000 --partition-mode low --processes 64
