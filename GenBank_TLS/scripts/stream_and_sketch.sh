#!/bin/bash
set -euo pipefail
cd /scratch/dmk333_new/known_microbe_hashes/shared_scripts/stream_sketcher
python -m stream_sketcher --config config_genbank_tls.yaml
