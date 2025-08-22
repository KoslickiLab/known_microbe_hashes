import csv
from typing import Iterator, Tuple, Optional

def iter_atb_filelist(path: str, region: str = "eu-west-2",
                      include_suffix: str = ".fa.gz",
                      limit: Optional[int] = None) -> Iterator[Tuple[str, str, str, Optional[int], Optional[str]]]:
    """
    Yield (subdir, filename, url, size, mtime) rows from the ATB filelist TSV (CSV-formatted).
    subdir is left empty; sharding will be assigned downstream.
    """
    count = 0
    with open(path, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            # Expect: bucket, key(filename), size, iso-timestamp, md5, extra
            if len(row) < 2:
                continue
            bucket = row[0].strip().strip('"')
            key = row[1].strip().strip('"')
            if not key.endswith(include_suffix):
                continue
            size = int(row[2]) if len(row) > 2 and row[2].isdigit() else None
            mtime = row[3].strip().strip('"') if len(row) > 3 else None
            url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
            yield ("", key, url, size, mtime)
            count += 1
            if limit is not None and count >= limit:
                break
