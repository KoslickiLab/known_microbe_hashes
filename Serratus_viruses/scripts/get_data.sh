cd ../data/sequences
wget https://serratus-public.s3.amazonaws.com/rdrp_contigs/rdrp_contigs.tar.gz
tar -xzvf rdrp_contigs.tar.gz
wget https://serratus-public.s3.amazonaws.com/assemblies/master_table_assemblies.tar.gz
tar -xzvf master_table_assemblies.tar.gz
wget https://lovelywater2.s3.amazonaws.com/assembly/micro/rdrp1.mu.fa
#wget https://github.com/rcedgar/palmdb/raw/refs/heads/main/2021-03-14/serratus.fa.gz
#gunzip serratus.fa.gz
#aws s3 cp s3://lovelywater2 --recursive --no-sign-request --exclude "*" --include "*.fasta"
