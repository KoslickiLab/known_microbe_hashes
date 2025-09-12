#!/bin/bash
aws s3 cp s3://logan-pub/paper/plasmids ../data --recursive --no-sign-request
zstd -d plasmid_data.tsv.zst
awk -F'\t' 'NR>1 { print ">" $1; print $NF }' plasmid_data.tsv > plasmid_data.fasta
