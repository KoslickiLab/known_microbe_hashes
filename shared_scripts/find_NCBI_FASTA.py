#!/usr/bin/env python3
"""
find_ncbi_fasta_ftp.py

Crawl ftp.ncbi.nlm.nih.gov and, for each top-level subtree, record ONE example
file whose name looks like a nucleotide FASTA:
  *.fna|*.fa|*.fasta|*.mfa (optionally .gz/.bz2/.xz/.Z)
  *_genomic.fna*
  *.fsa_nt.gz   (WGS/TSA/TLS nucleotide)
  *.ffn*        (nucleotide CDS)
  *.frn*        (rRNA/other RNA)

Usage (small test):
  python find_ncbi_fasta_ftp.py --out test.tsv --max-dirs 10 --max-files 1000

Notes:
  - Skips by default: /genomes/, /genbank/wgs/, /genbank/tsa/, /genbank/tls/
  - Add more skips with --skip-prefix /path/
"""

import argparse
import ftplib
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

def compile_patterns(pats: List[str]):
    return [(p, re.compile(p, re.IGNORECASE)) for p in pats]

def https_url(path: str) -> str:
    # path like "/dir/sub/file"; quote each component
    parts = [quote(p) for p in path.split("/") if p]
    return f"{HTTPS_BASE}/" + "/".join(parts) + ("/" if path.endswith("/") else "")

def ftp_connect(timeout=60) -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(HOST, 21, timeout=timeout)
    ftp.login()  # anonymous
    ftp.set_pasv(True)
    return ftp

def ftp_mlsd_list(ftp: ftplib.FTP, path: str) -> List[Tuple[str, str]]:
    """
    Return list of (name, type) where type in {"dir","file","cdir","pdir","slink"} using MLSD.
    """
    out = []
    try:
        for name, facts in ftp.mlsd(path):
            typ = facts.get("type", "")
            if typ.startswith("OS.unix=slink"):
                typ = "slink"
            out.append((name, typ))
        return out
    except ftplib.error_perm:
        # MLSD not supported in this dir (rare). Fallback to LIST parse.
        return ftp_list_fallback(ftp, path)

def ftp_list_fallback(ftp: ftplib.FTP, path: str) -> List[Tuple[str, str]]:
    """
    Fallback using LIST parsing (very basic).
    """
    lines: List[str] = []
    ftp.retrlines(f"LIST {path}", lines.append)
    out: List[Tuple[str, str]] = []
    for line in lines:
        # Typical UNIX LIST: drwxr-xr-x  2 ftp ftp     4096 Jan 01 00:00 dirname
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

def list_top_level(ftp: ftplib.FTP) -> List[str]:
    items = ftp_mlsd_list(ftp, "/")
    # Keep only directories (skip "." and ".." via types)
    return [f"/{name}/" for name, typ in items if typ == "dir"]

def crawl_subtree(
    ftp: ftplib.FTP,
    root: str,
    compiled_patterns: List[Tuple[str, re.Pattern]],
    skip_prefixes: List[str],
    max_dirs: int,
    max_files: int,
    sleep_s: float = 0.0,
) -> str:
    """
    DFS crawl subtree rooted at 'root' (e.g., '/1000genomes/').
    Return HTTPS URL of the FIRST matching file, or '' if none.
    """
    # safety: normalize trailing slash
    if not root.endswith("/"):
        root += "/"

    visited_dirs = set()
    dirs_seen = 0
    files_seen = 0

    stack = [root]
    while stack:
        path = stack.pop()
        if any(path.startswith(sp) for sp in skip_prefixes):
            continue
        if path in visited_dirs:
            continue
        visited_dirs.add(path)

        # caps
        if dirs_seen >= max_dirs:
            break
        dirs_seen += 1

        try:
            entries = ftp_mlsd_list(ftp, path)
        except Exception:
            # Permission or transient error—skip
            continue

        subdirs = []
        for name, typ in entries:
            if name in (".", ".."):
                continue
            child = path + name
            if typ in ("dir", "cdir"):
                subdirs.append(child + "/")
            elif typ == "slink":
                # Try to descend once; avoid loops via visited_dirs set
                subdirs.append(child + "/")
            else:
                # file
                if files_seen >= max_files:
                    break
                files_seen += 1
                for label, rgx in compiled_patterns:
                    if rgx.match(name):
                        return https_url(child)
        # politeness
        if sleep_s:
            time.sleep(sleep_s)
        # push subdirs
        stack.extend(subdirs)

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
    args = ap.parse_args()

    patterns = list(DEFAULT_PATTERNS) + list(args.include)
    compiled = compile_patterns(patterns)

    skip_prefixes = list(args.skip_prefix)
    if not args.no_default_skips:
        skip_prefixes = list(DEFAULT_SKIP_PREFIXES) + skip_prefixes

    try:
        ftp = ftp_connect()
    except Exception as e:
        print(f"ERROR: Could not connect to FTP: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        top = list_top_level(ftp)
    except Exception as e:
        print(f"ERROR: Could not list top-level directories: {e}", file=sys.stderr)
        sys.exit(2)

    results: Dict[str, str] = {}
    for subtree in sorted(top):
        # skip skips at top level too
        if any(subtree.startswith(sp) for sp in skip_prefixes):
            continue
        try:
            url = crawl_subtree(
                ftp=ftp,
                root=subtree,
                compiled_patterns=compiled,
                skip_prefixes=skip_prefixes,
                max_dirs=args.max_dirs,
                max_files=args.max_files,
                sleep_s=args.sleep,
            )
        except Exception:
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

    print(f"Wrote examples to {args.out}")

if __name__ == "__main__":
    main()
