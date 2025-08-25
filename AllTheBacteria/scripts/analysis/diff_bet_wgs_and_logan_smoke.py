# -*- coding: utf-8 -*-
# file: diff_bet_wgs_and_logan_smoke.py
import duckdb, json, time, pathlib, sys, os, threading

BASE = "/scratch/wgs_logan_diff_smoke"  # separate from your real run
LOG_TXT  = f"{BASE}/minhash_diff_smoke_report.txt"
LOG_JSON = f"{BASE}/minhash_diff_smoke_report.json"

db1_path = "/scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db"
db2_path = "/scratch/dmk333_new/GenBank_WGS_analysis/analysis/wgs_database.db"
out1 = f"{BASE}/parquet/minhash31_db1"
out2 = f"{BASE}/parquet/minhash31_db2"
tmp  = f"{BASE}/duckdb_tmp"

# Same bucket scheme you plan to use:
B = 9
mask = (1 << B) - 1

# Smoke limits (override via env if desired)
SMOKE_LIMIT_DB1 = int(os.getenv("SMOKE_LIMIT_DB1", "2000000000"))
SMOKE_LIMIT_DB2 = int(os.getenv("SMOKE_LIMIT_DB2", "2000000000"))

MONITOR_INTERVAL_SEC = 30

def dir_stats(root):
    total = 0
    files = 0
    buckets = set()
    for p, _, fnames in os.walk(root):
        for f in fnames:
            if f.endswith(".parquet"):
                fp = os.path.join(p, f)
                try:
                    sz = os.path.getsize(fp)
                except FileNotFoundError:
                    continue
                total += sz
                files += 1
                parts = p.split(os.sep)
                for part in parts[::-1]:
                    if part.startswith("bucket="):
                        try:
                            b = int(part.split("=",1)[1])
                            buckets.add(b)
                        except Exception:
                            pass
                        break
    return total, files, len(buckets)

def print_snapshot(label, path):
    if os.path.isdir(path):
        total, files, nb = dir_stats(path)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}: {files} files, {total/1e9:.3f} GB, {nb} buckets", flush=True)
    else:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}: (dir not created yet)", flush=True)

def monitor_outputs(stop_evt, paths):
    while not stop_evt.is_set():
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"[{ts}] smoke progress snapshot"]
        for label, path in paths.items():
            if os.path.isdir(path):
                total, files, nb = dir_stats(path)
                lines.append(f"  {label}: {files} files, {total/1e9:.3f} GB written, {nb} buckets present")
            else:
                lines.append(f"  {label}: (dir not created yet)")
        print("\n".join(lines), flush=True)
        stop_evt.wait(MONITOR_INTERVAL_SEC)

def ensure_any_parquet(path, label):
    # quick sanity check after each COPY
    total, files, nb = dir_stats(path)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label} check: files={files}, size={total/1e9:.3f} GB, buckets={nb}", flush=True)
    if files == 0:
        raise RuntimeError(f"No Parquet files were written to {path}. Please check permissions and DuckDB messages above.")

def main():
    for d in (BASE, out1, out2, tmp):
        os.makedirs(d, exist_ok=True)

    # Background monitor → prints to stdout (captured by nohup)
    stop_evt = threading.Event()
    mon = threading.Thread(
        target=monitor_outputs,
        args=(stop_evt, {"db1_out": out1, "db2_out": out2}),
        daemon=True
    )
    mon.start()

    t0 = time.time()
    con = duckdb.connect()
    # General settings
    con.execute("PRAGMA enable_object_cache")
    con.execute("PRAGMA enable_progress_bar")
    con.execute("SET progress_bar_time=1000")       # show progress after 1s (TTY-dependent)
    con.execute("SET memory_limit='3500GB'")
    con.execute(f"SET temp_directory='{tmp}/'")
    con.execute("SET preserve_insertion_order=false")

    con.execute(f"ATTACH '{db1_path}' AS db1 (READ_ONLY)")
    con.execute(f"ATTACH '{db2_path}' AS db2 (READ_ONLY)")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _params AS SELECT {B}::INTEGER AS B, {mask}::BIGINT AS mask")

    # ---- Phase 1: Exports (distinct per bucket) ----
    # IMPORTANT: with PARTITION_BY in DuckDB 1.3.x, do NOT use:
    #   - ROW_GROUPS_PER_FILE
    #   - FILE_SIZE_BYTES
    #   - PER_THREAD_OUTPUT
    # To produce exactly ONE file per partition in this smoke test, set threads=1.
    con.execute("SET threads=64")
    con.execute("SET partitioned_write_max_open_files=8192")  # raise OS ulimit -n too

    print(f"SMOKE: exporting limited rows to Parquet (db1 limit={SMOKE_LIMIT_DB1}, db2 limit={SMOKE_LIMIT_DB2})", flush=True)

    # ---- db1 smoke export ----
    con.execute(f"""
      COPY (
        WITH src AS (
          SELECT min_hash
          FROM db1.sigs_dna.signature_mins
          WHERE ksize = 31
          LIMIT {SMOKE_LIMIT_DB1}
        )
        SELECT (min_hash & (SELECT mask FROM _params)) AS bucket,
               min_hash AS hash
        FROM src
        GROUP BY bucket, hash
      ) TO '{out1}'
      (FORMAT parquet, COMPRESSION zstd,
       PARTITION_BY (bucket),
       ROW_GROUP_SIZE 250000);
    """)
    print_snapshot("after db1 export", out1)
    ensure_any_parquet(out1, "db1")

    # ---- db2 smoke export ----
    con.execute(f"""
      COPY (
        WITH src AS (
          SELECT hash
          FROM db2.hashes.hashes_31
          LIMIT {SMOKE_LIMIT_DB2}
        )
        SELECT (hash & (SELECT mask FROM _params)) AS bucket,
               hash
        FROM src
        GROUP BY bucket, hash
      ) TO '{out2}'
      (FORMAT parquet, COMPRESSION zstd,
       PARTITION_BY (bucket),
       ROW_GROUP_SIZE 250000);
    """)
    print_snapshot("after db2 export", out2)
    ensure_any_parquet(out2, "db2")

    # Restore higher parallelism for the join/counts
    con.execute("SET threads=64")

    print("SMOKE: counting A\\B and B\\A on smoke outputs ...", flush=True)
    # NOTE: inline the parquet paths to avoid parameter binding issues
    a_not_b, b_not_a = con.execute(f"""
      WITH
      db1u AS (
        SELECT bucket, hash
        FROM read_parquet('{out1}/bucket=*/*.parquet', hive_partitioning=true)
      ),
      db2u AS (
        SELECT bucket, hash
        FROM read_parquet('{out2}/bucket=*/*.parquet', hive_partitioning=true)
      ),
      a_not_b_by_bucket AS (
        SELECT bucket, COUNT(*) AS cnt
        FROM db1u a ANTI JOIN db2u b USING (bucket, hash)
        GROUP BY bucket
      ),
      b_not_a_by_bucket AS (
        SELECT bucket, COUNT(*) AS cnt
        FROM db2u a ANTI JOIN db1u b USING (bucket, hash)
        GROUP BY bucket
      )
      SELECT
        COALESCE((SELECT SUM(cnt) FROM a_not_b_by_bucket), 0) AS a_not_b,
        COALESCE((SELECT SUM(cnt) FROM b_not_a_by_bucket), 0) AS b_not_a
    """).fetchone()

    # Stop monitor
    stop_evt.set()
    mon.join(timeout=5)

    elapsed = time.time() - t0
    # Quick file count summary
    db1_total, db1_files, db1_buckets = dir_stats(out1)
    db2_total, db2_files, db2_buckets = dir_stats(out2)

    report = {
        "mode": "smoke_limit",
        "duckdb_version": con.execute("PRAGMA version").fetchone()[0],
        "bucket_bits": B,
        "mask": mask,
        "limits": {"db1": SMOKE_LIMIT_DB1, "db2": SMOKE_LIMIT_DB2},
        "a_not_b": int(a_not_b),
        "b_not_a": int(b_not_a),
        "paths": {"db1": db1_path, "db2": db2_path, "out1": out1, "out2": out2},
        "tmp": tmp,
        "elapsed_sec": elapsed,
        "outputs": {
            "db1": {"files": db1_files, "gb": round(db1_total/1e9, 3), "buckets": db1_buckets},
            "db2": {"files": db2_files, "gb": round(db2_total/1e9, 3), "buckets": db2_buckets}
        },
        "settings": {
            "threads_after_export": con.execute("SELECT current_setting('threads')").fetchone()[0],
            "memory_limit": con.execute("SELECT current_setting('memory_limit')").fetchone()[0],
            "preserve_insertion_order": con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0],
            "partitioned_write_max_open_files": con.execute("SELECT current_setting('partitioned_write_max_open_files')").fetchone()[0],
        }
    }

    txt = (
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SMOKE TEST\n"
        f"B={B} mask={mask}\n"
        f"LIMITS: db1={SMOKE_LIMIT_DB1}, db2={SMOKE_LIMIT_DB2}\n"
        f"A\\B: {report['a_not_b']}   B\\A: {report['b_not_a']}\n"
        f"Exported files: db1={db1_files} ({report['outputs']['db1']['gb']} GB, buckets={db1_buckets}), "
        f"db2={db2_files} ({report['outputs']['db2']['gb']} GB, buckets={db2_buckets})\n"
        f"Elapsed: {elapsed/60:.2f} min\n"
        f"DuckDB: {report['duckdb_version']}\n"
        f"settings: {report['settings']}\n"
        f"out1={out1}\nout2={out2}\n"
        f"tmp={tmp}\n"
    )
    pathlib.Path(LOG_TXT).write_text(txt)
    pathlib.Path(LOG_JSON).write_text(json.dumps(report, indent=2))
    print(txt, flush=True)

if __name__ == "__main__":
    # Suggest bumping open-files limit before launching
    # e.g., in shell: ulimit -n 200000
    sys.exit(main())
