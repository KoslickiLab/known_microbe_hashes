#!/usr/bin/env python3
"""
find_ncbi_fasta_ftp.py  — chatty, loop-safe crawler

Crawls ftp.ncbi.nlm.nih.gov and, for each top-level subtree, records ONE example
file whose name looks like a nucleotide FASTA:
  *.fna|*.fa|*.fasta|*.mfa (optionally .gz/.bz2/.xz/.Z)
  *_genomic.fna*
  *.fsa_nt.gz   (WGS/TSA/TLS nucleotide)
  *.ffn*        (nucleotide CDS)
  *.frn*        (rRNA/other RNA)

Now with:
  1) Explicit skip of any path containing '/./', '/../', './', or '../'
  2) Chatty logging about crawling, skips, and matches

Usage (small test):
  python find_ncbi_fasta_ftp.py --out test.tsv --max-dirs 10 --max-files 1000

Examples:
  # Skip SRA too and add a custom pattern
  python find_ncbi_fasta_ftp.py --skip-prefix /sra/ \
    --include '.*_cds_from_genomic\.fna(\.(gz|bz2|xz|Z))?$' \
    --out test.tsv --log-level INFO
"""

import argparse
import ftplib
import logging
import re
import sys
import time
from typing import List, Tuple, Dict
from urllib.parse import quote

HOST = "ftp.ncbi.nlm.nih.gov"
HTTPS_BASE = "https://ftp.ncbi.nlm.nih.gov"

DEFAULT_PATTERNS = [
    r".*\.fna(\.(gz|bz2|xz|Z))?$",
    r".*\.fa(\.(gz|bz2|xz|Z))?$",
    r".*\.fasta(\.(gz|bz2|xz|Z))?$",
    r".*\.mfa(\.(gz|bz2|xz|Z))?$",
    r".*\.fsa_nt\.gz$",
    r".*_genomic\.fna(\.(gz|bz2|xz|Z))?$",
    r".*\.ffn(\.(gz|bz2|xz|Z))?$",
    r".*\.frn(\.(gz|bz2|xz|Z))?$",
]

DEFAULT_SKIP_PREFIXES = [
    "/genomes/",
    "/genbank/wgs/",
    "/genbank/tsa/",
    "/genbank/tls/",
]

SUSPICIOUS_SUBSTRINGS = ("/./", "/../", "./", "../")


def is_suspicious_path(path: str) -> bool:
    """Return True if the path contains any loop-ish relative components."""
    return any(x in path for x in SUSPICIOUS_SUBSTRINGS)


def compile_patterns(pats: List[str]):
    return [(p, re.compile(p, re.IGNORECASE)) for p in pats]


def https_url(path: str) -> str:
    # path like "/dir/sub/file"; quote each component
    parts = [quote(p) for p in path.split("/") if p]
    return f"{HTTPS_BASE}/" + "/".join(parts) + ("/" if path.endswith("/") else "")


def ftp_connect(timeout=60, logger=None) -> ftplib.FTP:
    ftp = ftplib.FTP()
    if logger:
        logger.info(f"Connecting to FTP {HOST} ...")
    ftp.connect(HOST, 21, timeout=timeout)
    ftp.login()  # anonymous
    ftp.set_pasv(True)
    if logger:
        logger.info("Connected and logged in (anonymous).")
    return ftp


def ftp_mlsd_list(ftp: ftplib.FTP, path: str, logger=None) -> List[Tuple[str, str]]:
    """
    Return list of (name, type) where type in {"dir","file","cdir","pdir","slink"} using MLSD.
    """
    out = []
    try:
        if logger:
            logger.debug(f"MLSD listing: {path}")
        for name, facts in ftp.mlsd(path):
            typ = facts.get("type", "")
            if typ.startswith("OS.unix=slink"):
                typ = "slink"
            out.append((name, typ))
        return out
    except ftplib.error_perm:
        # MLSD not supported in this dir (rare). Fallback to LIST parse.
        if logger:
            logger.debug(f"MLSD failed at {path}; falling back to LIST parsing.")
        return ftp_list_fallback(ftp, path, logger=logger)


def ftp_list_fallback(ftp: ftplib.FTP, path: str, logger=None) -> List[Tuple[str, str]]:
    """
    Fallback using LIST parsing (very basic).
    """
    lines: List[str] = []
    try:
        ftp.retrlines(f"LIST {path}", lines.append)
    except Exception as e:
        if logger:
            logger.warning(f"LIST failed at {path}: {e}")
        return []
    out: List[Tuple[str, str]] = []
    for line in lines:
        parts = line.split(maxsplit=8)
        if not parts:
            continue
        mode = parts[0]
        name = parts[-1] if len(parts) >= 9 else None
        if not name or name in (".", ".."):
            continue
        if mode.startswith("d"):
            out.append((name, "dir"))
        elif mode.startswith("l"):
            out.append((name, "slink"))
        else:
            out.append((name, "file"))
    return out


def list_top_level(ftp: ftplib.FTP, logger=None) -> List[str]:
    items = ftp_mlsd_list(ftp, "/", logger=logger)
    # Keep only directories (skip "." and ".." via types)
    dirs = [f"/{name}/" for name, typ in items if typ == "dir"]
    # Filter out suspicious
    dirs = [d for d in dirs if not is_suspicious_path(d)]
    if logger:
        logger.info(f"Top-level subtrees discovered: {', '.join(sorted(dirs))}")
    return dirs


def crawl_subtree(
    ftp: ftplib.FTP,
    root: str,
    compiled_patterns: List[Tuple[str, re.Pattern]],
    skip_prefixes: List[str],
    max_dirs: int,
    max_files: int,
    logger: logging.Logger,
    sleep_s: float = 0.0,
) -> str:
    """
    DFS crawl subtree rooted at 'root' (e.g., '/1000genomes/').
    Return HTTPS URL of the FIRST matching file, or '' if none.
    """
    # safety: normalize trailing slash
    if not root.endswith("/"):
        root += "/"

    if is_suspicious_path(root):
        logger.info(f"Skipping suspicious root path: {root}")
        return ""

    visited_dirs = set()
    dirs_seen = 0
    files_seen = 0

    logger.info(f"=== Crawling subtree: {root} ===")
    stack = [root]
    while stack:
        path = stack.pop()

        if is_suspicious_path(path):
            logger.debug(f"Skip suspicious path: {path}")
            continue

        if any(path.startswith(sp) for sp in skip_prefixes):
            logger.debug(f"Skip (prefix rule) {path}")
            continue

        if path in visited_dirs:
            logger.debug(f"Skip (visited) {path}")
            continue
        visited_dirs.add(path)

        if dirs_seen >= max_dirs:
            logger.warning(f"Max directories reached ({max_dirs}) within {root}; stopping subtree.")
            break
        dirs_seen += 1

        logger.info(f"[{root}] Visiting directory ({dirs_seen}): {path}")
        try:
            entries = ftp_mlsd_list(ftp, path, logger=logger)
        except Exception as e:
            logger.warning(f"Failed to list {path}: {e}")
            continue

        subdirs = []
        for name, typ in entries:
            if name in (".", ".."):
                continue
            child = path + name
            if typ in ("dir", "cdir", "slink"):
                child_dir = child + "/"
                if is_suspicious_path(child_dir):
                    logger.debug(f"Skip suspicious child dir: {child_dir}")
                    continue
                subdirs.append(child_dir)
            else:
                if files_seen >= max_files:
                    logger.warning(f"Max files reached ({max_files}) within {root}; stopping subtree.")
                    break
                files_seen += 1
                # Match patterns
                for label, rgx in compiled_patterns:
                    if rgx.match(name):
                        url = https_url(child)
                        logger.info(f"[MATCH] pattern={label} file={child} url={url}")
                        return url

        if sleep_s:
            time.sleep(sleep_s)
        if subdirs:
            logger.debug(f"Queueing {len(subdirs)} subdir(s) under {path}")
        stack.extend(subdirs)

    logger.info(f"No match found in subtree: {root}")
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="ncbi_fasta_examples.tsv", help="Output TSV path")
    ap.add_argument("--max-dirs", type=int, default=200000, help="Max directories to traverse per subtree")
    ap.add_argument("--max-files", type=int, default=2000000, help="Max files to examine per subtree")
    ap.add_argument("--include", action="append", default=[], help="Extra regex pattern(s) (repeatable)")
    ap.add_argument("--skip-prefix", action="append", default=[], help="Additional path prefixes to skip (e.g., /sra/)")
    ap.add_argument("--no-default-skips", action="store_true", help="Do not skip /genomes and /genbank/{wgs,tsa,tls}")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between directory fetches")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    help="Logging verbosity")
    args = ap.parse_args()

    # logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("ncbi-fasta-crawler")

    patterns = list(DEFAULT_PATTERNS) + list(args.include)
    compiled = compile_patterns(patterns)

    skip_prefixes = list(args.skip_prefix)
    if not args.no_default_skips:
        skip_prefixes = list(DEFAULT_SKIP_PREFIXES) + skip_prefixes
    # ensure suspicious paths are always skipped
    skip_prefixes = sorted(set(skip_prefixes))

    try:
        ftp = ftp_connect(logger=logger)
    except Exception as e:
        logger.error(f"Could not connect to FTP: {e}")
        sys.exit(2)

    try:
        top = list_top_level(ftp, logger=logger)
    except Exception as e:
        logger.error(f"Could not list top-level directories: {e}")
        try:
            ftp.quit()
        except Exception:
            pass
        sys.exit(2)

    results: Dict[str, str] = {}
    for subtree in sorted(top):
        # skip by prefix or suspicious
        if any(subtree.startswith(sp) for sp in skip_prefixes):
            logger.info(f"Skipping subtree by prefix rule: {subtree}")
            continue
        if is_suspicious_path(subtree):
            logger.info(f"Skipping suspicious subtree: {subtree}")
            continue

        try:
            url = crawl_subtree(
                ftp=ftp,
                root=subtree,
                compiled_patterns=compiled,
                skip_prefixes=skip_prefixes,
                max_dirs=args.max_dirs,
                max_files=args.max_files,
                logger=logger,
                sleep_s=args.sleep,
            )
        except Exception as e:
            logger.warning(f"Error while crawling {subtree}: {e}")
            url = ""
        if url:
            results[subtree] = url

    try:
        ftp.quit()
    except Exception:
        pass

    with open(args.out, "w") as fh:
        fh.write("#subtree\turl\n")
        for subtree, url in sorted(results.items()):
            fh.write(f"{subtree}\t{url}\n")

    logger.info(f"Wrote examples to {args.out}")


if __name__ == "__main__":
    main()
