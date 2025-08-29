
import sqlite3
import threading
import time
import os
from typing import Optional, Tuple, List, Dict, Any

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subdir TEXT NOT NULL,
    filename TEXT NOT NULL,
    url TEXT NOT NULL,
    size INTEGER,
    mtime TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    tries INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    out_path TEXT,
    updated_at REAL,
    created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_status ON files(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_file ON files(subdir, filename);
"""

class DB:
    def __init__(self, path: str):
        self.path = path
        # ensure the directory exists
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=60.0)
        with self.conn:
            for stmt in SCHEMA.strip().split(";"):
                if stmt.strip():
                    self.conn.execute(stmt)

    # --- add to wgs_sketcher/db.py (class DB) ---
    def claim_next(self,
                   error_cooldown_seconds: int = 3600,
                   error_max_total_tries: int = 20) -> Optional[Tuple[int, str, str, str]]:
        """Atomically claim work: PENDING first; else eligible ERROR (aged & below cap)."""
        now = time.time()
        cutoff = now - error_cooldown_seconds
        with self._lock, self.conn:
            # 1) Prefer PENDING
            row = self.conn.execute(
                "SELECT id FROM files WHERE status='PENDING' ORDER BY id LIMIT 1"
            ).fetchone()
            if row:
                fid = row[0]
                cur = self.conn.execute(
                    "UPDATE files SET status='DOWNLOADING', updated_at=? "
                    "WHERE id=? AND status='PENDING'", (now, fid)
                )
                if cur.rowcount == 1:
                    return self.conn.execute(
                        "SELECT id, subdir, filename, url FROM files WHERE id=?", (fid,)
                    ).fetchone()
                # lost a race; fall through to try again next call

            # 2) Otherwise, an ERROR that has cooled down and hasn't exceeded cap
            row = self.conn.execute(
                "SELECT id FROM files "
                "WHERE status='ERROR' AND tries < ? AND (updated_at IS NULL OR updated_at <= ?) "
                "ORDER BY updated_at NULLS FIRST, id LIMIT 1",
                (error_max_total_tries, cutoff)
            ).fetchone()
            if not row:
                return None
            fid = row[0]
            cur = self.conn.execute(
                "UPDATE files SET status='DOWNLOADING', updated_at=? "
                "WHERE id=? AND status='ERROR'", (now, fid)
            )
            if cur.rowcount != 1:
                return None
            return self.conn.execute(
                "SELECT id, subdir, filename, url FROM files WHERE id=?", (fid,)
            ).fetchone()

    def reset_stuck(self, stale_seconds: int = 3600):
        """
        Requeue rows that were 'in-progress' long ago (e.g., crash/kill).
        """
        import time
        threshold = time.time() - stale_seconds
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE files SET status='PENDING', updated_at=? "
                "WHERE status IN ('DOWNLOADING','SKETCHING') AND (updated_at IS NULL OR updated_at < ?)",
                (time.time(), threshold),
            )

    def upsert_file(self, subdir: str, filename: str, url: str, size: Optional[int], mtime: Optional[str]):
        ts = time.time()
        with self._lock, self.conn:
            self.conn.execute(
                """INSERT INTO files (subdir, filename, url, size, mtime, status, updated_at, created_at)
                   VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)
                   ON CONFLICT(subdir, filename) DO UPDATE SET url=excluded.url, size=excluded.size, mtime=excluded.mtime, updated_at=excluded.updated_at""",
                (subdir, filename, url, size, mtime, ts, ts)
            )

    def get_next_batch(self, limit: int=1000) -> List[Tuple[int, str, str, str]]:
        with self._lock:
            cur = self.conn.execute(
                "SELECT id, subdir, filename, url FROM files WHERE status IN ('PENDING','ERROR') ORDER BY id LIMIT ?",
                (limit,)
            )
            return cur.fetchall()

    def mark_status(self, file_id: int, status: str, out_path: Optional[str]=None, error: Optional[str]=None, inc_tries: bool=False):
        ts = time.time()
        with self._lock, self.conn:
            if inc_tries:
                self.conn.execute("UPDATE files SET status=?, out_path=?, last_error=?, tries=tries+1, updated_at=? WHERE id=?",
                                  (status, out_path, error, ts, file_id))
            else:
                self.conn.execute("UPDATE files SET status=?, out_path=?, last_error=?, updated_at=? WHERE id=?",
                                  (status, out_path, error, ts, file_id))

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            cur = self.conn.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
            by_status = {row[0]: row[1] for row in cur.fetchall()}
            cur2 = self.conn.execute("SELECT COUNT(*) FROM files")
            total = cur2.fetchone()[0]
            return {"total": total, "by_status": by_status}

    def list_unfinished(self, limit: int=100) -> List[Tuple[int, str, str, str, str]]:
        with self._lock:
            cur = self.conn.execute(
                "SELECT id, subdir, filename, url, status FROM files WHERE status IN ('PENDING','ERROR','DOWNLOADING','SKETCHING') ORDER BY id LIMIT ?",
                (limit,)
            )
            return cur.fetchall()
