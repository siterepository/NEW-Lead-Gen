"""
JobQueue - Lightweight async SQLite job queue with change-detection support.

Uses aiosqlite for non-blocking I/O and WAL mode for concurrent reads/writes.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL Definitions
# ---------------------------------------------------------------------------

_CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    job_type    TEXT    NOT NULL,
    payload     TEXT    NOT NULL,  -- JSON
    priority    INTEGER NOT NULL DEFAULT 0,
    status      TEXT    NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    error       TEXT,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL
);
"""

_CREATE_JOBS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_status_priority
    ON jobs (status, priority DESC, created_at ASC);
"""

_CREATE_CHANGE_DETECTION_TABLE = """
CREATE TABLE IF NOT EXISTS change_detection (
    agent_name  TEXT NOT NULL,
    url         TEXT NOT NULL,
    last_post_id TEXT NOT NULL,
    checked_at  TEXT NOT NULL,
    PRIMARY KEY (agent_name, url)
);
"""

_CREATE_AGENT_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS agent_runs (
    id          TEXT PRIMARY KEY,
    agent_name  TEXT NOT NULL,
    platform    TEXT,
    status      TEXT NOT NULL,
    items_found INTEGER NOT NULL DEFAULT 0,
    items_new   INTEGER NOT NULL DEFAULT 0,
    items_dup   INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    completed_at TEXT NOT NULL
);
"""


# ---------------------------------------------------------------------------
# JobQueue
# ---------------------------------------------------------------------------

class JobQueue:
    """Async SQLite-backed job queue.

    Usage::

        queue = JobQueue("data/leadgen.db")
        await queue.init_db()

        await queue.enqueue("raw_scrape", {"url": "...", "html": "..."})
        job = await queue.dequeue("raw_scrape")
        if job:
            # ... process ...
            await queue.complete(job["id"])
    """

    def __init__(self, db_path: str = "data/leadgen.db") -> None:
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init_db(self) -> None:
        """Open the database and create tables if they don't exist."""
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        # Enable WAL mode for concurrent read/write performance
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")

        await self._db.execute(_CREATE_JOBS_TABLE)
        await self._db.execute(_CREATE_JOBS_INDEX)
        await self._db.execute(_CREATE_CHANGE_DETECTION_TABLE)
        await self._db.execute(_CREATE_AGENT_RUNS_TABLE)
        await self._db.commit()

        logger.info("JobQueue initialised at %s (WAL mode)", self.db_path)

    async def close(self) -> None:
        """Close the database connection."""
        if self._db:
            await self._db.close()
            self._db = None

    # ------------------------------------------------------------------
    # Job CRUD
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        priority: int = 0,
    ) -> str:
        """Insert a new job and return its ID.

        Higher priority values are dequeued first.
        """
        job_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """
            INSERT INTO jobs (id, job_type, payload, priority, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'pending', ?, ?)
            """,
            (job_id, job_type, json.dumps(payload), priority, now, now),
        )
        await self._db.commit()
        return job_id

    async def dequeue(self, job_type: str) -> Optional[dict[str, Any]]:
        """Atomically fetch the next pending job for *job_type*.

        Returns the full job row as a dict, or None if nothing is queued.
        Jobs are ordered by priority DESC then created_at ASC (FIFO within
        the same priority).
        """
        now = datetime.now(timezone.utc).isoformat()

        # SQLite doesn't have SELECT ... FOR UPDATE, so we use a
        # subquery + UPDATE ... RETURNING pattern in a single statement.
        cursor = await self._db.execute(
            """
            UPDATE jobs
            SET status = 'processing', updated_at = ?
            WHERE id = (
                SELECT id FROM jobs
                WHERE job_type = ? AND status = 'pending'
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            )
            RETURNING *
            """,
            (now, job_type),
        )
        row = await cursor.fetchone()
        await self._db.commit()

        if row is None:
            return None

        result = dict(row)
        # Deserialise the JSON payload
        result["payload"] = json.loads(result["payload"])
        return result

    async def complete(self, job_id: str) -> None:
        """Mark a job as done."""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE jobs SET status = 'done', updated_at = ? WHERE id = ?",
            (now, job_id),
        )
        await self._db.commit()

    async def fail(self, job_id: str, error: str) -> None:
        """Mark a job as failed and record the error."""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE jobs SET status = 'failed', error = ?, updated_at = ? WHERE id = ?",
            (error, now, job_id),
        )
        await self._db.commit()

    async def get_pending_count(self, job_type: Optional[str] = None) -> int:
        """Return the number of pending jobs, optionally filtered by type."""
        if job_type:
            cursor = await self._db.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'pending' AND job_type = ?",
                (job_type,),
            )
        else:
            cursor = await self._db.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'pending'"
            )
        row = await cursor.fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # Change detection
    # ------------------------------------------------------------------

    async def get_last_seen(
        self, agent_name: str, url: str
    ) -> Optional[str]:
        """Return the last seen post ID for a given agent + URL, or None."""
        cursor = await self._db.execute(
            "SELECT last_post_id FROM change_detection WHERE agent_name = ? AND url = ?",
            (agent_name, url),
        )
        row = await cursor.fetchone()
        return row[0] if row else None

    async def set_last_seen(
        self, agent_name: str, url: str, post_id: str
    ) -> None:
        """Upsert the last seen post ID for change detection."""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """
            INSERT INTO change_detection (agent_name, url, last_post_id, checked_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (agent_name, url)
            DO UPDATE SET last_post_id = excluded.last_post_id,
                         checked_at  = excluded.checked_at
            """,
            (agent_name, url, post_id, now),
        )
        await self._db.commit()
