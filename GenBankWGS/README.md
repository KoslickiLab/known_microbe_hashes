# Instructions

These data were obtained via the [GenBank_WGS_Analysis](https://github.com/KoslickiLab/GenBank_WGS_analysis) workflow, which essentially consisted of:

1. Streaming and sketching all of GenBank WGS: [run.sh](https://github.com/KoslickiLab/GenBank_WGS_analysis/blob/main/run.sh)
2. Dumping the hashes to a spool file buckets (partitioned by lower hash digits) [wgs_sketch_union_ChatGPT.py extract](https://github.com/KoslickiLab/GenBank_WGS_analysis/blob/main/analysis/wgs_sketch_union_ChatGPT.py)
3. Deduping these hashes [wgs_sketch_union_ChatGPT.py reduce](https://github.com/KoslickiLab/GenBank_WGS_analysis/blob/main/analysis/wgs_sketch_union_ChatGPT.py)

