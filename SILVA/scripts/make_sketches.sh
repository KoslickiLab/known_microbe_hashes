#!/bin/bash
# Create array of files to process (both compressed and uncompressed)
files_to_process=()

for file in ../data/*; do
    # Skip if it doesn't exist
    [[ ! -f "$file" ]] && continue
    
    if [[ "$file" == *.gz ]]; then
        # It's compressed - use uncompressed name for output check
        uncompressed="${file%.gz}"
        [[ -f "${uncompressed}.sig.zip" ]] && continue
        files_to_process+=("$file")
    else
        # It's uncompressed - check if compressed version exists
        compressed="${file}.gz"
        [[ -f "$compressed" ]] && continue  # Skip, we'll process the .gz version
        [[ -f "${file}.sig.zip" ]] && continue  # Output already exists
        files_to_process+=("$file")
    fi
done

# Process the files
for file in "${files_to_process[@]}"; do
    if [[ "$file" == *.gz ]]; then
        gunzip "$file"
        processed_file="${file%.gz}"
    else
        processed_file="$file"
    fi
    
    sed -i 's/U/T/g' "$processed_file"
    sourmash sketch dna -p k=31,scaled=1000,noabund "$processed_file" -o "${processed_file}.sig.zip" &
done

