#!/usr/bin/env python3
"""
find_ncbi_fasta_ftp.py  — chatty, loop-safe, auto-reconnecting crawler

Crawls ftp.ncbi.nlm.nih.gov and, for each top-level subtree, records ONE example
file whose name looks like a nucleotide FASTA:
  *.fna|*.fa|*.fasta|*.mfa (optionally .gz/.bz2/.xz/.Z)
  *_genomic.fna*
  *.fsa_nt.gz   (WGS/TSA/TLS nucleotide)
  *.ffn*        (nucleotide CDS)
  *.frn*        (rRNA/other RNA)

Features:
  - Skips any path containing '/./', '/../', './', or '../'
  - Retries with backoff and **reconnects** after timeouts / broken pipe / resets
  - Sets **32 MB** socket send/receive buffers for control and data sockets
  - Chatty logging (--log-level DEBUG|INFO|...)

Usage (small test):
  python find_ncbi_fasta_ftp.py --out test.tsv --max-dirs 10 --max-files 1000 --log-level INFO

Examples:
  # skip SRA too and add a custom pattern
  python find_ncbi_fasta_ftp.py --skip-prefix /sra/ \
    --include '.*_cds_from_genomic\.fna(\.(gz|bz2|xz|Z))?$' \
    --out test.tsv --log-level INFO
"""

import argparse
import ftplib
import logging
import re
import socket as _socket
import sys
import time
from typing import List, Tuple, Dict, Optional
from urllib.parse import quote

HOST = "ftp.ncbi.nlm.nih.gov"
HTTPS_BASE = "https://ftp.ncbi.nlm.nih.gov"

# --- Buffer size (32 MB) for control + data sockets ---
BUF_SIZE = 32 * 1024 * 1024
_ORIG_CREATE_CONNECTION = _socket.create_connection

def _create_connection_with_buffers(address, timeout=None, source_address=None):
    s = _ORIG_CREATE_CONNECTION(address, timeout, source_address)
    try:
        s.setsockopt(_socket.SOL_SOCKET, _socket.SO_RCVBUF, BUF_SIZE)
        s.setsockopt(_socket.SOL_SOCKET, _socket.SO_SNDBUF, BUF_SIZE)
    except Exception:
        pass
    return s

# Ensure ftplib uses our buffered sockets for **data** connections too
ftplib.socket.create_connection = _create_connection_with_buffers

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
    return any(x in path for x in SUSPICIOUS_SUBSTRINGS)

def compile_patterns(pats: List[str]):
    return [(p, re.compile(p, re.IGNORECASE)) for p in pats]

def https_url(path: str) -> str:
    parts = [quote(p) for p in path.split("/") if p]
    return f"{HTTPS_BASE}/" + "/".join(parts) + ("/" if path.endswith("/") else "")

class NCBIFTP:
    """Wrapper that lists directories with retries, backoff, and reconnection."""
    def __init__(self, host: str, timeout: int, logger: logging.Logger,
                 retries: int = 5, initial_backoff: float = 3.0, backoff_factor: float = 2.0):
        self.host = host
        self.timeout = timeout
        self.logger = logger
        self.retries = retries
        self.initial_backoff = initial_backoff
        self.backoff_factor = backoff_factor
        self.ftp: Optional[ftplib.FTP] = None

    def connect(self):
        self.logger.info(f"Connecting to FTP {self.host} (timeout={self.timeout}s, buf={BUF_SIZE//(1024*1024)}MB)...")
        ftp = ftplib.FTP()
        ftp.connect(self.host, 21, timeout=self.timeout)
        # set 32MB buffers on control socket too
        try:
            ftp.sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_RCVBUF, BUF_SIZE)
            ftp.sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_SNDBUF, BUF_SIZE)
        except Exception:
            pass
        ftp.login()  # anonymous
        ftp.set_pasv(True)
        self.ftp = ftp
        self.logger.info("Connected and logged in (anonymous).")

    def quit(self):
        try:
            if self.ftp is not None:
                self.ftp.quit()
        except Exception:
            pass
        self.ftp = None

    def reconnect(self):
        self.logger.warning("Reconnecting FTP session...")
        self.quit()
        self.connect()

    def _list_mlsd(self, path: str):
        out = []
        assert self.ftp is not None
        for name, facts in self.ftp.mlsd(path):
            typ = facts.get("type", "")
            if typ.startswith("OS.unix=slink"):
                typ = "slink"
            out.append((name, typ))
        return out

    def _list_fallback(self, path: str):
        lines: List[str] = []
        assert self.ftp is not None
        self.ftp.retrlines(f"LIST {path}", lines.append)
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

    def list_dir(self, path: str) -> List[Tuple[str, str]]:
        """Return list of (name, type) with retries & reconnect on failures."""
        if is_suspicious_path(path):
            self.logger.info(f"Skipping suspicious path: {path}")
            return []
        attempt = 0
        backoff = self.initial_backoff
        while True:
            attempt += 1
            try:
                if self.ftp is None:
                    self.connect()
                self.logger.debug(f"MLSD {path} (attempt {attempt})")
                return self._list_mlsd(path)
            except ftplib.error_perm as e:
                # MLSD not supported here; fallback to LIST (no reconnect)
                self.logger.debug(f"MLSD not supported at {path} ({e}); falling back to LIST (attempt {attempt})")
                try:
                    return self._list_fallback(path)
                except Exception as e2:
                    self.logger.warning(f"LIST failed at {path}: {e2}")
            except (ftplib.all_errors, OSError) as e:
                self.logger.warning(f"List failed at {path}: {e.__class__.__name__}: {e} (attempt {attempt})")
                if attempt >= self.retries:
                    self.logger.error(f"Giving up on {path} after {attempt} attempts.")
                    return []
                # reconnect + backoff
                try:
                    self.reconnect()
                except Exception as e3:
                    self.logger.warning(f"Reconnect failed: {e3}")
                self.logger.info(f"Sleeping {backoff:.1f}s before retry...")
                time.sleep(backoff)
                backoff *= self.backoff_factor

def list_top_level(client: NCBIFTP, logger) -> List[str]:
    items = client.list_dir("/")
    dirs = [f"/{name}/" for name, typ in items if typ == "dir"]
    dirs = [d for d in dirs if not is_suspicious_path(d)]
    logger.info(f"Top-level subtrees discovered: {', '.join(sorted(dirs))}")
    return dirs

def crawl_subtree(
    client: NCBIFTP,
    root: str,
    compiled_patterns: List[Tuple[str, re.Pattern]],
    skip_prefixes: List[str],
    max_dirs: int,
    max_files: int,
    logger: logging.Logger,
    sleep_s: float = 0.0,
) -> str:
    """DFS crawl subtree rooted at 'root'; return HTTPS URL of FIRST match or ''."""
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
        entries = client.list_dir(path)
        if not entries:
            # already logged by client; continue DFS
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
    # retry/backoff tuning
    ap.add_argument("--retry-attempts", type=int, default=5, help="Max retry attempts on list failures")
    ap.add_argument("--retry-initial-sleep", type=float, default=3.0, help="Initial backoff seconds before first retry")
    ap.add_argument("--retry-backoff-factor", type=float, default=2.0, help="Backoff multiplier per retry")
    ap.add_argument("--timeout", type=int, default=60, help="FTP connect timeout (seconds)")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(H)s:%(M)s:%(S)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # Python's logging doesn't support %(H)s etc. Use standard:
    for h in logging.getLogger().handlers:
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%H:%M:%S"))
    logger = logging.getLogger("ncbi-fasta-crawler")

    patterns = list(DEFAULT_PATTERNS) + list(args.include)
    compiled = compile_patterns(patterns)

    skip_prefixes = list(args.skip_prefix)
    if not args.no_default_skips:
        skip_prefixes = list(DEFAULT_SKIP_PREFIXES) + skip_prefixes
    skip_prefixes = sorted(set(skip_prefixes))

    client = NCBIFTP(
        host=HOST,
        timeout=args.timeout,
        logger=logger,
        retries=args.retry_attempts,
        initial_backoff=args.retry_initial_sleep,
        backoff_factor=args.retry_backoff_factor,
    )

    try:
        client.connect()
        top = list_top_level(client, logger=logger)
    except Exception as e:
        logger.error(f"Failed to initialize crawl: {e}")
        client.quit()
        sys.exit(2)

    results: Dict[str, str] = {}
    for subtree in sorted(top):
        if any(subtree.startswith(sp) for sp in skip_prefixes):
            logger.info(f"Skipping subtree by prefix rule: {subtree}")
            continue
        if is_suspicious_path(subtree):
            logger.info(f"Skipping suspicious subtree: {subtree}")
            continue

        try:
            url = crawl_subtree(
                client=client,
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

    client.quit()

    with open(args.out, "w") as fh:
        fh.write("#subtree\turl\n")
        for subtree, url in sorted(results.items()):
            fh.write(f"{subtree}\t{url}\n")

    logger.info(f"Wrote examples to {args.out}")

if __name__ == "__main__":
    main()
