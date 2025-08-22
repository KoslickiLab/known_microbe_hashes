#wget https://osf.io/4yv85/download
#mv download file_list.all.latest.tsv.gz
#gunzip file_list.all.latest.tsv.gz
aws s3 ls --no-sign-request s3://allthebacteria-metadata/allthebacteria-assemblies/assemblies-list/data/  | sort | tail -n1
# Then get the name of the file and paste in
aws s3 cp --no-sign-request s3://allthebacteria-metadata/allthebacteria-assemblies/assemblies-list/data/56885c70-92a9-46b4-a026-7d05c813c19d.csv.gz latest.tsv.gz
gunzip latest.tsv
