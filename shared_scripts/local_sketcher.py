#!/usr/bin/env python3
"""
GTDB/local FASTA sourmash sketcher.

Recursively scan a local directory for FASTA files (*.fa, *.fna, *.fasta, optionally gzipped),
compute sourmash sketches for each file (one zip per input), and place them under
an output root in shard subdirectories:  out_root/bucket=XXXX/<basename>.sig.zip

This layout is compatible with:
  - atb_hash_export.py
  - atb_hash_dedup.py
  - atb_hash_ingest_duckdb.py

Sample IDs downstream are inferred from the zip filename, so we keep the original
FASTA filename for the outer zip name:
    <original-fasta-name>.sig.zip

By default we *skip* re-sketching if the destination .sig.zip already exists.
Use --force to recompute.

Requires:
  - Python 3.8+
  - sourmash >= 4.x available on PATH (or specify --sourmash-bin)

Example (full run):
  python gtdb_local_sketcher.py \\
      --input-root /scratch/GTDB/gtdb_genomes_reps_r226 \\
      --out-root   /scratch/known_microbe_hashes/GTDB/sketches \\
      --workers 24

Example (smoke test on first 10 files):
  python gtdb_local_sketcher.py -i /scratch/GTDB/gtdb_genomes_reps_r226 -o /scratch/tmp/sketches --max-files 10

"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
import errno

# ---------
# Defaults
# ---------
DEFAULT_PARAMS = "k=15,k=31,k=33,scaled=1000,noabund"
DEFAULT_EXT_RE = re.compile(r'\.(fa|fna|fasta)(\.gz)?$', re.IGNORECASE)


@dataclass
class Task:
    src: Path             # input FASTA file
    dst_zip: Path         # output .sig.zip
    params: str           # sourmash param string
    sourmash_bin: str     # "sourmash" binary path
    threads: int          # RAYON_NUM_THREADS for sourmash
    tmp_dir: Path         # temporary directory root
    force: bool           # recompute even if dst exists
    verbose: bool         # print progress


def debug(msg: str, *, verbose: bool=False) -> None:
    if verbose:
        print(msg, file=sys.stderr)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def which_or_exit(bin_name: str) -> str:
    path = shutil.which(bin_name)
    if not path:
        print(f"ERROR: required executable '{bin_name}' not found on PATH.", file=sys.stderr)
        sys.exit(2)
    return path


def shard_subdir_for(name: str, n_shards: int) -> str:
    """
    Deterministic shard subdirectory name `bucket=XXXX` based on md5(name) % n_shards.
    Pads with zeroes to the width of n_shards-1 (e.g., 4096 -> 4 digits).
    """
    h = int.from_bytes(hashlib.md5(name.encode('utf-8')).digest(), 'big')
    bucket = h % int(n_shards)
    pad = max(1, len(str(max(0, n_shards - 1))))
    return f"bucket={bucket:0{pad}d}"


def find_fasta_files(root: Path, *, limit: Optional[int]=None) -> List[Path]:
    files: List[Path] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if DEFAULT_EXT_RE.search(p.name):
            files.append(p)
            if limit is not None and len(files) >= limit:
                break
    return sorted(files)

def move_into_place(src: Path, dst: Path) -> None:
    """Move src -> dst. Try atomic rename; fall back to cross-FS move."""
    ensure_dir(dst.parent)
    try:
        os.replace(src, dst)  # atomic if same filesystem
    except OSError as e:
        if getattr(e, "errno", None) == errno.EXDEV:
            # cross-filesystem; do a copy+rename
            tmp_dst = dst.with_suffix(dst.suffix + ".partial")
            shutil.copy2(src, tmp_dst)
            os.replace(tmp_dst, dst)
            os.unlink(src)
        else:
            raise


def run_sourmash(in_path: Path, out_sig_zip: Path, params: str, *, sourmash_bin: str, threads: int, verbose: bool) -> Tuple[int, str]:
    """
    Execute 'sourmash sketch dna -p <params> -o <out_sig> <in_path>'
    Returns (returncode, combined_stdout_stderr).
    """
    env = os.environ.copy()
    if threads and threads > 0:
        env.setdefault("RAYON_NUM_THREADS", str(int(threads)))
    cmd = [
        sourmash_bin, "sketch", "dna",
        "-p", params,
        "-o", str(out_sig_zip),
        str(in_path),
    ]
    debug(" ".join(cmd), verbose=verbose)
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)
    return proc.returncode, proc.stdout


def process_one(task: Task) -> Tuple[str, bool, Optional[str]]:
    """
    Process a single FASTA file.

    Returns tuple: (dst_zip, skipped, error)
    - skipped=True if existing and not forced.
    - error is None on success, otherwise the error message.
    """
    src, dst_zip = task.src, task.dst_zip
    try:
        if dst_zip.exists() and not task.force:
            return (str(dst_zip), True, None)

        ensure_dir(task.tmp_dir)
        with tempfile.TemporaryDirectory(prefix="sm_", dir=str(task.tmp_dir)) as td:
            tmp_sig_zip = Path(td) / (src.name + ".sig.zip")
            rc, out = run_sourmash(src, tmp_sig_zip, task.params, sourmash_bin=task.sourmash_bin, threads=task.threads, verbose=task.verbose)
            if rc != 0 or not tmp_sig_zip.exists():
                # propagate sourmash error output to caller
                return (str(dst_zip), False, f"sourmash failed (rc={rc}). Output:\n{out}")

            # move into place
            move_into_place(tmp_sig_zip, dst_zip)

        return (str(dst_zip), False, None)
    except Exception as e:
        return (str(dst_zip), False, f"{type(e).__name__}: {e}")


def build_tasks(input_root: Path, out_root: Path, *, n_shards: int, params: str,
                sourmash_bin: str, threads: int, tmp_dir: Path, limit: Optional[int],
                force: bool, verbose: bool) -> List[Task]:
    fasta_files = find_fasta_files(input_root, limit=limit)
    tasks: List[Task] = []
    for src in fasta_files:
        # The zip filename preserves the original FASTA filename (critical for sample_id inference downstream)
        zip_name = src.name + ".sig.zip"
        shard = shard_subdir_for(src.name, n_shards)
        dst_zip = out_root / shard / zip_name
        tasks.append(Task(src=src, dst_zip=dst_zip, params=params,
                          sourmash_bin=sourmash_bin, threads=threads,
                          tmp_dir=tmp_dir, force=force, verbose=verbose))
    return tasks


def main(argv: Optional[Sequence[str]]=None) -> int:
    p = argparse.ArgumentParser(description="Sketch local FASTA files with sourmash into .sig.zip archives (ATB-compatible layout).")
    p.add_argument("-i", "--input-root", type=Path, required=True, help="Root directory to recursively scan for FASTA (*.fa|*.fna|*.fasta[.gz])")
    p.add_argument("-o", "--out-root", type=Path, required=True, help="Output root. Will contain bucket=XXXX/ subdirectories with .sig.zip files.")
    p.add_argument("--params", default=DEFAULT_PARAMS, help=f"sourmash parameter string (default: {DEFAULT_PARAMS})")
    p.add_argument("--sourmash-bin", default="sourmash", help="Path to sourmash executable (default: 'sourmash' on PATH)")
    p.add_argument("--n-shards", type=int, default=4096, help="Number of shard subdirectories to spread output across (default: 4096)")
    p.add_argument("--threads", type=int, default=1, help="RAYON_NUM_THREADS for sourmash (per process). Default: 1")
    p.add_argument("--workers", type=int, default=max(1, os.cpu_count() or 1), help="Parallel worker processes (default: CPU count)")
    p.add_argument("--max-files", type=int, default=None, help="Limit to the first N input files (smoke test)")
    p.add_argument("--tmp-dir", type=Path, default=Path(tempfile.gettempdir()) / "gtdb_local_sketcher", help="Where to create temporary working dirs")
    p.add_argument("--force", action="store_true", help="Recompute and overwrite existing .sig.zip files")
    p.add_argument("--dry-run", action="store_true", help="Only list planned work; do not run sourmash")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = p.parse_args(argv)

    # Early checks & setup
    sourmash_bin = which_or_exit(args.sourmash_bin)
    ensure_dir(args.out_root)
    ensure_dir(args.tmp_dir)

    tasks = build_tasks(args.input_root, args.out_root, n_shards=args.n_shards,
                        params=args.params, sourmash_bin=sourmash_bin, threads=args.threads,
                        tmp_dir=args.tmp_dir, limit=args.max_files, force=args.force, verbose=args.verbose)

    if args.dry_run:
        print(f"Would process {len(tasks)} files:")
        for t in tasks[:20]:
            print(f"  {t.src} -> {t.dst_zip}")
        if len(tasks) > 20:
            print(f"  ... and {len(tasks) - 20} more")
        return 0

    total = len(tasks)
    if total == 0:
        print("No input FASTA files found.", file=sys.stderr)
        return 1

    print(f"Sketching {total} file(s) with params='{args.params}' using {args.workers} workers; output -> {args.out_root}")

    # Run in a process pool (each worker spawns 'sourmash' processes)
    errors: List[Tuple[str, str]] = []
    skipped = 0
    done = 0

    with futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
        for dst_zip, was_skipped, err in pool.map(process_one, tasks, chunksize=1):
            if was_skipped:
                skipped += 1
            elif err:
                errors.append((dst_zip, err))
            done += 1
            if args.verbose and (done % 50 == 0 or done == total):
                print(f"[{done}/{total}] skipped={skipped} errors={len(errors)}", file=sys.stderr)

    if skipped:
        print(f"Skipped {skipped} existing file(s).")

    if errors:
        print(f"Completed with {len(errors)} error(s):", file=sys.stderr)
        for (dst, msg) in errors[:20]:
            print(f"  {dst}: {msg.splitlines()[0]}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... and {len(errors)-20} more", file=sys.stderr)
        return 2

    print("All done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
