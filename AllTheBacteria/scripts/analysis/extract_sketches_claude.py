#!/usr/bin/env python3
"""
Collect and process FracMinHash sketches from sourmash signature files.

This script processes millions of sourmash sketch files, extracts hashes,
and stores them in an efficient format for downstream analysis.
"""

import argparse
import gzip
import json
import logging
import os
import sys
import tempfile
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sketch_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class SketchInfo:
    """Container for sketch metadata and hashes."""
    file_path: str
    sketch_name: str
    ksize: int
    scaled: int
    num_hashes: int
    hashes: np.ndarray  # Using numpy for memory efficiency


class SketchProcessor:
    """Process sourmash sketch files and extract hash information."""

    def __init__(self, base_dir: Path, output_dir: Path, num_workers: int = None):
        """
        Initialize the processor.

        Args:
            base_dir: Base directory containing sketch subdirectories
            output_dir: Directory for output files
            num_workers: Number of parallel workers (None for auto)
        """
        self.base_dir = Path(base_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.num_workers = num_workers or os.cpu_count()

        # Statistics tracking
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'total_hashes': 0,
            'unique_hashes_per_k': {}
        }

    def find_sketch_files(self) -> List[Path]:
        """Find all .sig.zip files in the directory structure."""
        logger.info(f"Scanning for sketch files in {self.base_dir}")
        sketch_files = []

        # Use glob pattern to find all .sig.zip files
        for subdir in self.base_dir.iterdir():
            if subdir.is_dir():
                pattern = "*.sig.zip"
                sketch_files.extend(subdir.glob(pattern))

        logger.info(f"Found {len(sketch_files)} sketch files")
        self.stats['total_files'] = len(sketch_files)
        return sketch_files

    @staticmethod
    def process_single_file(file_path: Path) -> List[SketchInfo]:
        """
        Process a single .sig.zip file and extract sketch information.
        Processes entirely in memory without writing to disk.

        Args:
            file_path: Path to the .sig.zip file

        Returns:
            List of SketchInfo objects
        """
        sketches = []

        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                # Get list of files in the zip
                namelist = zf.namelist()

                # Find signature files (*.sig.gz in signatures/ directory)
                sig_files = [name for name in namelist
                             if name.startswith('signatures/') and name.endswith('.sig.gz')]

                if not sig_files:
                    logger.warning(f"No signature files found in {file_path}")
                    return sketches

                for sig_file_name in sig_files:
                    try:
                        # Read the gzipped file directly from zip into memory
                        with zf.open(sig_file_name) as zipped_sig:
                            # Read the gzipped content
                            gzipped_content = zipped_sig.read()

                            # Decompress in memory and parse JSON
                            decompressed = gzip.decompress(gzipped_content).decode('utf-8')
                            sig_data = json.loads(decompressed)

                        # sig_data is a list of signature objects
                        for sig_obj in sig_data:
                            # Each signature object has a 'signatures' list
                            if 'signatures' in sig_obj:
                                for sketch in sig_obj['signatures']:
                                    sketch_info = SketchProcessor._extract_sketch_info(
                                        sketch, str(file_path), sig_obj.get('filename', '')
                                    )
                                    if sketch_info:
                                        sketches.append(sketch_info)

                    except Exception as e:
                        logger.error(f"Error processing signature {sig_file_name} from {file_path}: {e}")

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

        return sketches

    @staticmethod
    def _extract_sketch_info(sketch_data: Dict, source_file: str,
                             original_filename: str = '') -> Optional[SketchInfo]:
        """
        Extract information from a single sketch.

        Args:
            sketch_data: Dictionary containing sketch data
            source_file: Source file path
            original_filename: Original filename from signature metadata

        Returns:
            SketchInfo object or None if extraction fails
        """
        try:
            # Extract core fields from sourmash signature
            ksize = sketch_data.get('ksize')
            seed = sketch_data.get('seed', 42)
            max_hash = sketch_data.get('max_hash')
            num = sketch_data.get('num', 0)

            # Calculate scaled value if not directly present
            scaled = sketch_data.get('scaled')
            if scaled is None and max_hash:
                # Calculate scaled from max_hash
                # scaled = 2^64 / max_hash
                if max_hash < 18446744073709551616:  # 2^64
                    scaled = int(18446744073709551616 / max_hash)
                else:
                    scaled = 1

            # Extract hashes from 'mins' field
            hashes = sketch_data.get('mins', [])

            if not hashes or ksize is None:
                return None

            # Create a meaningful name from the original filename or source
            name = original_filename if original_filename else source_file

            # Convert to numpy array for efficiency
            hash_array = np.array(hashes, dtype=np.uint64)

            return SketchInfo(
                file_path=source_file,
                sketch_name=name,
                ksize=ksize,
                scaled=scaled if scaled else 0,
                num_hashes=len(hash_array),
                hashes=hash_array
            )

        except Exception as e:
            logger.error(f"Error extracting sketch info: {e}")
            return None

    def process_batch(self, file_batch: List[Path]) -> Tuple[List[SketchInfo], Dict]:
        """
        Process a batch of files.

        Args:
            file_batch: List of file paths to process

        Returns:
            Tuple of (sketches, statistics)
        """
        all_sketches = []
        batch_stats = {
            'processed': 0,
            'failed': 0,
            'total_hashes': 0
        }

        for file_path in file_batch:
            try:
                sketches = self.process_single_file(file_path)
                all_sketches.extend(sketches)
                batch_stats['processed'] += 1
                batch_stats['total_hashes'] += sum(s.num_hashes for s in sketches)
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                batch_stats['failed'] += 1

        return all_sketches, batch_stats

    def save_to_parquet(self, sketches: List[SketchInfo], output_file: Path):
        """
        Save sketches to a Parquet file optimized for hash analysis.
        Uses vectorized operations for better performance.

        Args:
            sketches: List of SketchInfo objects
            output_file: Output file path
        """
        logger.info(f"Saving {len(sketches)} sketches to {output_file}")

        # Group sketches by ksize for better organization
        k_groups = {}
        for sketch in sketches:
            k = sketch.ksize
            if k not in k_groups:
                k_groups[k] = []
            k_groups[k].append(sketch)

        # Create separate tables for each k-value
        for ksize, k_sketches in k_groups.items():
            output_k_file = output_file.parent / f"{output_file.stem}_k{ksize}.parquet"

            logger.info(f"Processing {len(k_sketches)} sketches for k={ksize}")

            # Use vectorized approach for better performance
            all_hashes = []
            all_ksizes = []
            all_scaled = []
            all_source_files = []
            all_sketch_names = []

            for sketch in tqdm(k_sketches, desc=f"Preparing k={ksize}", leave=False):
                num_hashes = len(sketch.hashes)
                all_hashes.append(sketch.hashes)
                all_ksizes.extend([ksize] * num_hashes)
                all_scaled.extend([sketch.scaled] * num_hashes)
                all_source_files.extend([sketch.file_path] * num_hashes)
                all_sketch_names.extend([sketch.sketch_name] * num_hashes)

            # Concatenate all hashes
            all_hashes_array = np.concatenate(all_hashes)

            # Create DataFrame directly from arrays (much faster)
            df = pd.DataFrame({
                'hash': all_hashes_array,
                'ksize': all_ksizes,
                'scaled': all_scaled,
                'source_file': all_source_files,
                'sketch_name': all_sketch_names
            })

            # Use Parquet with compression for efficient storage
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                output_k_file,
                compression='snappy',
                use_dictionary=True,
                compression_level=None
            )

            logger.info(f"Saved {len(df)} hash records for k={ksize} to {output_k_file}")

    def save_unique_hashes(self, sketches: List[SketchInfo], output_file: Path):
        """
        Save unique hashes per k-value in a compact format.

        Args:
            sketches: List of SketchInfo objects
            output_file: Output file path
        """
        logger.info("Extracting unique hashes per k-value")

        k_hashes = {}
        for sketch in sketches:
            k = sketch.ksize
            if k not in k_hashes:
                k_hashes[k] = set()
            k_hashes[k].update(sketch.hashes.tolist())

        # Save unique hashes for each k
        for ksize, unique_hashes in k_hashes.items():
            output_k_file = output_file.parent / f"{output_file.stem}_k{ksize}_unique.parquet"

            # Convert to sorted array for efficient storage
            unique_array = np.array(sorted(unique_hashes), dtype=np.uint64)

            df = pd.DataFrame({
                'hash': unique_array,
                'ksize': ksize
            })

            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                output_k_file,
                compression='snappy'
            )

            logger.info(f"Saved {len(unique_array)} unique hashes for k={ksize}")
            self.stats['unique_hashes_per_k'][ksize] = len(unique_array)

    def process_parallel(self, batch_size: int = 100):
        """
        Process all files in parallel batches.

        Args:
            batch_size: Number of files per batch
        """
        sketch_files = self.find_sketch_files()

        if not sketch_files:
            logger.warning("No sketch files found")
            return

        # Split files into batches
        batches = [sketch_files[i:i + batch_size]
                   for i in range(0, len(sketch_files), batch_size)]

        all_sketches = []

        logger.info(f"Processing {len(sketch_files)} files in {len(batches)} batches "
                    f"using {self.num_workers} workers")

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {executor.submit(self.process_batch, batch): i
                       for i, batch in enumerate(batches)}

            with tqdm(total=len(batches), desc="Processing batches") as pbar:
                for future in as_completed(futures):
                    batch_idx = futures[future]
                    try:
                        sketches, batch_stats = future.result()
                        all_sketches.extend(sketches)

                        # Update statistics
                        self.stats['processed_files'] += batch_stats['processed']
                        self.stats['failed_files'] += batch_stats['failed']
                        self.stats['total_hashes'] += batch_stats['total_hashes']

                        pbar.update(1)

                        # Save intermediate results periodically
                        if len(all_sketches) > 100000:
                            self._save_intermediate(all_sketches, batch_idx)
                            all_sketches = []

                    except Exception as e:
                        logger.error(f"Batch {batch_idx} failed: {e}")
                        pbar.update(1)

        # Save final results if any remain
        if all_sketches:
            output_file = self.output_dir / "genbank_wgs_sketches.parquet"
            self.save_to_parquet(all_sketches, output_file)
            self.save_unique_hashes(all_sketches,
                                    self.output_dir / "genbank_wgs_unique_hashes.parquet")

        # Merge intermediate files if they exist (this now also creates unique files)
        self._merge_intermediate_files()

        # If we only used intermediate files and no unique files exist, generate them
        self._ensure_unique_files_exist()

        # Print final statistics
        self._print_statistics()

    def _ensure_unique_files_exist(self):
        """Ensure unique hash files exist by generating from merged files if needed."""
        for ksize in [15, 31, 33]:
            unique_file = self.output_dir / f"genbank_wgs_unique_hashes_k{ksize}.parquet"
            merged_file = self.output_dir / f"genbank_wgs_sketches_k{ksize}_merged.parquet"

            # If unique file doesn't exist but merged file does, generate it
            if not unique_file.exists() and merged_file.exists():
                logger.info(f"Generating unique hash file for k={ksize}")
                df = pd.read_parquet(merged_file)
                unique_hashes = df['hash'].unique()

                unique_df = pd.DataFrame({
                    'hash': np.sort(unique_hashes),
                    'ksize': ksize
                })

                table = pa.Table.from_pandas(unique_df)
                pq.write_table(
                    table,
                    unique_file,
                    compression='snappy'
                )

                logger.info(f"Generated unique hash file with {len(unique_hashes):,} hashes for k={ksize}")

    def _save_intermediate(self, sketches: List[SketchInfo], batch_idx: int):
        """Save intermediate results to avoid memory issues."""
        intermediate_dir = self.output_dir / "intermediate"
        intermediate_dir.mkdir(exist_ok=True)

        output_file = intermediate_dir / f"batch_{batch_idx:06d}.parquet"
        self.save_to_parquet(sketches, output_file)
        logger.info(f"Saved intermediate batch {batch_idx}")

    def _merge_intermediate_files(self):
        """Merge intermediate files into final output."""
        intermediate_dir = self.output_dir / "intermediate"
        if not intermediate_dir.exists():
            return

        logger.info("Merging intermediate files")

        # Group files by k-value
        k_files = {}
        for file in intermediate_dir.glob("*.parquet"):
            if '_k' in file.name:
                k_val = int(file.stem.split('_k')[1].split('_')[0])
                if k_val not in k_files:
                    k_files[k_val] = []
                k_files[k_val].append(file)

        # Merge files for each k-value
        for ksize, files in k_files.items():
            if files:
                output_file = self.output_dir / f"genbank_wgs_sketches_k{ksize}_merged.parquet"

                # Read and concatenate all files
                dfs = []
                for f in files:
                    dfs.append(pd.read_parquet(f))

                merged_df = pd.concat(dfs, ignore_index=True)

                # Save merged file
                table = pa.Table.from_pandas(merged_df)
                pq.write_table(
                    table,
                    output_file,
                    compression='snappy',
                    use_dictionary=True
                )

                logger.info(f"Merged {len(files)} files for k={ksize}")

                # Create unique hash file from merged data
                unique_output = self.output_dir / f"genbank_wgs_unique_hashes_k{ksize}.parquet"
                unique_hashes = merged_df['hash'].unique()
                unique_df = pd.DataFrame({
                    'hash': np.sort(unique_hashes),
                    'ksize': ksize
                })

                table = pa.Table.from_pandas(unique_df)
                pq.write_table(
                    table,
                    unique_output,
                    compression='snappy'
                )

                logger.info(f"Saved {len(unique_hashes):,} unique hashes for k={ksize} to {unique_output}")
                self.stats['unique_hashes_per_k'][ksize] = len(unique_hashes)

        # Clean up intermediate files
        for file in intermediate_dir.glob("*.parquet"):
            file.unlink()
        intermediate_dir.rmdir()

    def _print_statistics(self):
        """Print processing statistics."""
        logger.info("\n" + "=" * 50)
        logger.info("Processing Complete")
        logger.info("=" * 50)
        logger.info(f"Total files: {self.stats['total_files']}")
        logger.info(f"Processed files: {self.stats['processed_files']}")
        logger.info(f"Failed files: {self.stats['failed_files']}")
        logger.info(f"Total hashes: {self.stats['total_hashes']}")

        if self.stats['unique_hashes_per_k']:
            logger.info("\nUnique hashes per k-value:")
            for k, count in sorted(self.stats['unique_hashes_per_k'].items()):
                logger.info(f"  k={k}: {count:,} unique hashes")


def compare_with_external_hashes(parquet_file: Path, external_hashes: Set[int],
                                 ksize: int) -> Dict:
    """
    Compare GenBank WGS hashes with external database hashes.

    Args:
        parquet_file: Path to the parquet file with GenBank hashes
        external_hashes: Set of hashes from external database
        ksize: K-mer size to compare

    Returns:
        Dictionary with comparison statistics
    """
    logger.info(f"Comparing hashes for k={ksize}")

    # Read the unique hashes file
    unique_file = parquet_file.parent / f"{parquet_file.stem}_k{ksize}_unique.parquet"

    if not unique_file.exists():
        logger.error(f"Unique hash file not found: {unique_file}")
        return {}

    df = pd.read_parquet(unique_file)
    genbank_hashes = set(df['hash'].values)

    # Compute statistics
    stats = {
        'genbank_total': len(genbank_hashes),
        'external_total': len(external_hashes),
        'intersection': len(genbank_hashes & external_hashes),
        'genbank_only': len(genbank_hashes - external_hashes),
        'external_only': len(external_hashes - genbank_hashes)
    }

    stats['jaccard_similarity'] = (
        stats['intersection'] /
        (stats['genbank_total'] + stats['external_total'] - stats['intersection'])
        if (stats['genbank_total'] + stats['external_total'] - stats['intersection']) > 0
        else 0
    )

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Collect and process sourmash FracMinHash sketches"
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Base directory containing sketch subdirectories"
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Output directory for processed files"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: auto)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of files per batch (default: 100)"
    )
    parser.add_argument(
        "--compare-with",
        type=Path,
        help="Path to external hash file for comparison (optional)"
    )
    parser.add_argument(
        "--ksize",
        type=int,
        choices=[15, 31, 33],
        help="K-mer size for comparison (required with --compare-with)"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.input_dir.exists():
        logger.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)

    if args.compare_with and not args.ksize:
        logger.error("--ksize is required when using --compare-with")
        sys.exit(1)

    # Process sketches
    processor = SketchProcessor(
        args.input_dir,
        args.output_dir,
        num_workers=args.workers
    )

    processor.process_parallel(batch_size=args.batch_size)

    # Optional comparison with external hashes
    if args.compare_with and args.compare_with.exists():
        logger.info(f"Loading external hashes from {args.compare_with}")

        # Assuming external file is also in parquet format
        # Adjust this based on actual format
        external_df = pd.read_parquet(args.compare_with)
        external_hashes = set(external_df['hash'].values)

        stats = compare_with_external_hashes(
            args.output_dir / "genbank_wgs_unique_hashes.parquet",
            external_hashes,
            args.ksize
        )

        logger.info("\nComparison Results:")
        logger.info(f"GenBank WGS hashes: {stats['genbank_total']:,}")
        logger.info(f"External DB hashes: {stats['external_total']:,}")
        logger.info(f"Intersection: {stats['intersection']:,}")
        logger.info(f"GenBank-only: {stats['genbank_only']:,}")
        logger.info(f"External-only: {stats['external_only']:,}")
        logger.info(f"Jaccard similarity: {stats['jaccard_similarity']:.4f}")


if __name__ == "__main__":
    main()