#!/bin/bash
aws s3 cp s3://logan-pub/paper/Obelisk/ ./data --recursive --no-sign-request --exclude "*" --include "*.fasta"
