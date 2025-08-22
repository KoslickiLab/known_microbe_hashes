#!/usr/bin/env python3
"""
Efficiently count total number of uint64 hash values across all .bin files
in a directory structure with part=XXX subdirectories.
"""

import os
import numpy as np
from pathlib import Path
import time
from typing import Iterator, Tuple


def find_bin_files(root_dir: str) -> Iterator[Path]:
    """
    Generator that yields all .bin files in part=XXX subdirectories.
    
    Args:
        root_dir: Root directory containing part=XXX subdirectories
        
    Yields:
        Path objects for each .bin file found
    """
    root_path = Path(root_dir)
    
    # Find all part=XXX directories
    for part_dir in root_path.glob("part=*"):
        if part_dir.is_dir():
            # Find all .bin files in this part directory
            for bin_file in part_dir.glob("*.bin"):
                yield bin_file


def count_hashes_in_file(file_path: Path) -> int:
    """
    Count the number of uint64 values in a single .bin file.
    
    Args:
        file_path: Path to the .bin file
        
    Returns:
        Number of uint64 values in the file
    """
    try:
        # Get file size in bytes
        file_size = file_path.stat().st_size
        
        # Each uint64 is 8 bytes, so divide by 8 to get count
        # This is much faster than loading the entire array
        uint64_count = file_size // 8
        
        return uint64_count
    
    except (OSError, IOError) as e:
        print(f"Error reading {file_path}: {e}")
        return 0


def count_total_hashes(root_dir: str, verbose: bool = True) -> Tuple[int, int]:
    """
    Count total number of uint64 hash values across all .bin files.
    
    Args:
        root_dir: Root directory containing part=XXX subdirectories
        verbose: Whether to print progress information
        
    Returns:
        Tuple of (total_hash_count, total_files_processed)
    """
    total_hashes = 0
    total_files = 0
    
    start_time = time.time()
    
    for bin_file in find_bin_files(root_dir):
        hash_count = count_hashes_in_file(bin_file)
        total_hashes += hash_count
        total_files += 1
        
        if verbose and total_files % 1000 == 0:
            elapsed = time.time() - start_time
            print(f"Processed {total_files:,} files, "
                  f"total hashes so far: {total_hashes:,} "
                  f"(elapsed: {elapsed:.1f}s)")
    
    return total_hashes, total_files


def count_total_hashes_numpy_method(root_dir: str, verbose: bool = True) -> Tuple[int, int]:
    """
    Alternative method that actually loads arrays (slower but more robust).
    Use this if the file size method gives unexpected results.
    
    Args:
        root_dir: Root directory containing part=XXX subdirectories
        verbose: Whether to print progress information
        
    Returns:
        Tuple of (total_hash_count, total_files_processed)
    """
    total_hashes = 0
    total_files = 0
    
    start_time = time.time()
    
    for bin_file in find_bin_files(root_dir):
        try:
            # Load the array and get its length
            arr = np.fromfile(bin_file, dtype=np.uint64)
            hash_count = len(arr)
            total_hashes += hash_count
            total_files += 1
            
            if verbose and total_files % 1000 == 0:
                elapsed = time.time() - start_time
                print(f"Processed {total_files:,} files, "
                      f"total hashes so far: {total_hashes:,} "
                      f"(elapsed: {elapsed:.1f}s)")
                      
        except (OSError, IOError, ValueError) as e:
            print(f"Error reading {bin_file}: {e}")
    
    return total_hashes, total_files


def main():
    """Main function to run the hash counting."""
    
    # Set your directory path here
    root_directory = "/scratch/genbank_wgs/extracted_hashes/spool/ksize=33"
    
    print(f"Counting hashes in: {root_directory}")
    print("=" * 50)
    
    # Check if directory exists
    if not os.path.exists(root_directory):
        print(f"Error: Directory {root_directory} does not exist!")
        return
    
    # Use the fast file-size based method
    start_time = time.time()
    total_hashes, total_files = count_total_hashes(root_directory)
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 50)
    print("RESULTS:")
    print(f"Total files processed: {total_files:,}")
    print(f"Total hash values: {total_hashes:,}")
    print(f"Total processing time: {elapsed_time:.2f} seconds")
    print(f"Average hashes per file: {total_hashes/total_files if total_files > 0 else 0:,.0f}")
    print(f"Processing rate: {total_files/elapsed_time if elapsed_time > 0 else 0:.1f} files/second")


if __name__ == "__main__":
    main()
