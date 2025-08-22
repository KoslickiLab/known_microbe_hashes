# AllTheBacteria (ATB) Sourmash Sketcher

> **Download, sketch, and archive sourmash signatures for AllTheBacteria assemblies**

This is a focused fork of the GenBank WGS sketcher that **removes FTP crawling** and instead
reads the ATB `filelist-latest.tsv` (CSV-formatted) to discover files.
It preserves the same **restartable**, **fault-tolerant**, and **polite** async pipeline and the
same downstream analysis/export tooling (Parquet, DuckDB, etc.).

---

## TL;DR

```bash
python -m atb_sketcher --config config.yaml
```

- Point `filelist_path` in `config.yaml` at your ATB file list.
- For a smoke test, set `seed_limit: 50` in the config and run.
- Outputs: one `.sig.zip` per input `.fa.gz`, partitioned into `bucket=XXXX/` shards.

---

## What’s different from the GenBank version?

- **No FTP crawler.** We parse `filelist-latest.tsv` and construct HTTPS URLs like
  `https://{bucket}.s3.{region}.amazonaws.com/{filename}`.
- **Hash-based sharding** of outputs: `bucket = sha1(filename) % n_shards`.
  This avoids any assumptions about sequential IDs (`SAMD...`). Configure `n_shards`.
- **Same DB & workflow**: a local SQLite DB tracks status (`PENDING, DOWNLOADING, SKETCHING, DONE, ERROR`).
- **Same sketcher**: we call `sourmash sketch dna` with your parameters and write `.sig.zip` archives.

---

## Configure (example tuned to your box)

- Dual 64-core EPYCs, 4 TB RAM, 10G NIC, 8 TB SSD free.
- Suggested starting values (already in `config.yaml`):
  - `max_concurrent_downloads: 48`
  - `max_total_workers: 120`
  - `sourmash_threads: 96` (exported as `RAYON_NUM_THREADS`)
  - `n_shards: 4096`

Adjust up/down based on SSD throughput and network saturation. The pipeline is
IO-bound on downloads and CPU-bound on sourmash; aim to keep both busy.

---

## Smoke test

1. Copy a tiny subset of the ATB file list into `/tmp/mini-filelist.tsv` (e.g., 50 lines).
2. Set in `config.yaml`:
   ```yaml
   filelist_path: "/tmp/mini-filelist.tsv"
   seed_limit: 50
   ```
3. Run `python -m atb_sketcher --config config.yaml` and confirm shards appear under `output_root/`.

---

## Output layout

```
{output_root}/
  bucket=0000/
    SAMD...fa.gz.sig.zip
  bucket=0001/
  ...
```

Temporary downloads land under `{tmp_root}/bucket=XXXX/` and are deleted after successful sketching.

---

## Downstream extraction and Parquet export

Use the **same** scripts under `analysis/` as in the GenBank repo to:
- extract FracMinHash values from `.sig.zip` files,
- reduce and write partitioned Parquet,
- optionally ingest into DuckDB and build indices.

Point the scripts at your `output_root`.

---

## Reliability and restarts

- Safe to stop with `Ctrl+C`; in-flight tasks finish and the DB persists.
- On startup, we **requeue stale** `DOWNLOADING/SKETCHING` rows older than `stale_seconds`.
- Exponential backoff and a cap on total retries per row.

---

## Requirements

```
pip install -r requirements.txt
# plus sourmash in your environment
```

---

## License

MIT
