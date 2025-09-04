#!/usr/bin/env python3
"""
Crawl https://ftp.ncbi.nlm.nih.gov/ and find example nucleotide FASTA files.

Default: for each top-level subtree (e.g., /1000genomes, /ReferenceSamples),
return the FIRST match to any nucleotide FASTA-like pattern and write them to a TSV.

Usage:
  python find_ncbi_fastas.py \
      --start https://ftp.ncbi.nlm.nih.gov/ \
      --out ncbi_fasta_examples.tsv

Options:
  --global-one           # instead of "one per subtree", return only ONE example per pattern globally
  --include PATTERN      # add an extra regex (repeatable)
  --skip-prefix PATH     # skip crawling subtrees starting with this path (repeatable)
  --max-dirs N           # safety cap on number of directories to visit
  --max-files N          # safety cap on number of files to examine
"""

import argparse, re, sys, time, urllib.parse
from html.parser import HTMLParser
from urllib.request import Request, urlopen

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
    "/genomes/",         # you've already mirrored this
    "/genbank/wgs/",     # already mirrored
    "/genbank/tsa/",     # already mirrored
    "/genbank/tls/",     # already mirrored
    "/sra/",             # huge, already done in the Logan project
]

UA = "ncbi-fasta-crawler/1.0 (contact: your_email@example.org)"

class IndexParser(HTMLParser):
    """Minimal parser to extract hrefs from Apache 'Index of' pages."""
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if not href:
            return
        # Ignore query sorts and anchors
        if href.startswith("?") or href.startswith("#"):
            return
        # Build absolute
        abs_url = urllib.parse.urljoin(self.base_url, href)
        # explicitely don't add any urls ending in '/./' or '/../'
        if abs_url.path.endswith('/./') or abs_url.path.endswith('/../') or href in ('./', '../'):
            return
        else:
            self.links.append(abs_url)

def fetch(url, timeout=30):
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as resp:
        # Only parse HTML directory listings
        ctype = resp.headers.get("Content-Type", "")
        if "text/html" not in ctype:
            return "", []
        html = resp.read().decode("utf-8", "replace")
        p = IndexParser(url)
        p.feed(html)
        return html, p.links

def is_dir_url(u):
    return u.endswith("/")

def path_part(u):
    # returns the path of the URL
    return urllib.parse.urlparse(u).path

def top_level_child_of(base, url):
    """Return the top-level child under base for url (e.g., '/1000genomes/')."""
    base_path = path_part(base)
    p = path_part(url)
    if not p.startswith(base_path):
        return None
    rest = p[len(base_path):].lstrip("/")
    # first segment
    first = rest.split("/", 1)[0]
    return "/" + first + "/" if first else "/"

def crawl_subtree(root_url, patterns, skip_prefixes,
                  per_subtree_examples, visited_dirs,
                  max_dirs, max_files, counters,
                  stop_when_all_found=False,
                  record_key=None):
    """
    DFS crawl starting at root_url, filling per_subtree_examples dict:
      key = record_key (e.g., top-level subtree string) or a global key
      value = dict of {pattern: first_matching_url}
    """
    stack = [root_url]
    while stack:
        u = stack.pop()
        p = path_part(u)

        # caps
        if counters["dirs"] > max_dirs:
            break

        # skip large known trees or any configured prefix
        if any(p.startswith(sp) for sp in skip_prefixes):
            continue

        # avoid revisits (symlinks can create loops)
        if p in visited_dirs:
            continue
        visited_dirs.add(p)

        try:
            _, links = fetch(u)
        except Exception:
            # silently skip errors
            continue

        counters["dirs"] += 1

        # Partition links
        dirs, files = [], []
        for link in links:
            if link == u:
                continue
            if is_dir_url(link):
                dirs.append(link)
            else:
                files.append(link)

        # Scan files for matches
        for f in files:
            if counters["files"] > max_files:
                break
            counters["files"] += 1
            for pat in patterns:
                if pat not in per_subtree_examples[record_key]:
                    if re.search(pat, f):
                        per_subtree_examples[record_key][pat] = f
            if stop_when_all_found and all(per_subtree_examples[record_key].values()):
                return  # done for this subtree

        # Recurse into subdirs
        # Small politeness delay; adjust if needed
        time.sleep(0.01)
        stack.extend(dirs)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="https://ftp.ncbi.nlm.nih.gov/",
                    help="Base HTTPS URL for NCBI FTP")
    ap.add_argument("--out", default="ncbi_fasta_examples.tsv",
                    help="Output TSV path")
    ap.add_argument("--global-one", action="store_true",
                    help="Return only one example per pattern globally (rather than per top-level subtree)")
    ap.add_argument("--include", action="append", default=[],
                    help="Extra regex pattern(s) to include")
    ap.add_argument("--skip-prefix", action="append", default=[],
                    help="URL path prefix(es) to skip (e.g., /sra/). Can repeat.")
    ap.add_argument("--max-dirs", type=int, default=200000,
                    help="Safety cap on number of directories to visit")
    ap.add_argument("--max-files", type=int, default=2000000,
                    help="Safety cap on number of files to examine")
    args = ap.parse_args()

    base = args.start.rstrip("/") + "/"

    patterns = DEFAULT_PATTERNS + args.include
    # compile patterns once (store as strings still for output labeling)
    comp = {pat: re.compile(pat) for pat in patterns}

    skip_prefixes = set(DEFAULT_SKIP_PREFIXES + args.skip_prefix)

    # Discover top-level children
    try:
        _, links = fetch(base)
    except Exception as e:
        print(f"ERROR: Unable to fetch base {base}: {e}", file=sys.stderr)
        sys.exit(2)

    top_level_dirs = sorted([u for u in links if is_dir_url(u)])
    # Build record keys: either one global bucket or one bucket per top-level child
    example_map = {}
    if args.global_one:
        example_map["GLOBAL"] = {pat: "" for pat in patterns}
        work = [("GLOBAL", base)]
    else:
        work = []
        for d in top_level_dirs:
            key = top_level_child_of(base, d)
            if key is None:
                continue
            example_map[key] = {pat: "" for pat in patterns}
            work.append((key, d))

    visited_dirs = set()
    for record_key, start_url in work:
        counters = {"dirs": 0, "files": 0}
        crawl_subtree(
            root_url=start_url,
            patterns=list(comp.keys()),
            skip_prefixes=skip_prefixes,
            per_subtree_examples=example_map,
            visited_dirs=visited_dirs,
            max_dirs=args.max_dirs,
            max_files=args.max_files,
            counters=counters,
            stop_when_all_found=args.global_one,  # in global mode, stop when all patterns found
            record_key=record_key,
        )

    # Write output
    with open(args.out, "w") as out:
        out.write("#subtree\tpattern\turl\n")
        for subtree, d in sorted(example_map.items()):
            for pat, url in d.items():
                if url:
                    out.write(f"{subtree}\t{pat}\t{url}\n")

    print(f"Wrote examples to {args.out}")

if __name__ == "__main__":
    main()
