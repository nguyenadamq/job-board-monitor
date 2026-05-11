import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
WATCH_DIR = DATA_DIR / "watch"
WATCH_DIR.mkdir(parents=True, exist_ok=True)

JOB_DB_PATH = WATCH_DIR / os.getenv("JOB_DB", "job_intelligence.db")


NormalizedJob = Dict[str, Any]
Classification = Dict[str, Any]


@dataclass(frozen=True)
class SavedJob:
    id: int
    provider: str
    company: str
    external_job_id: str
    content_hash: str
    is_new: bool
    changed: bool


def init_job_db(db_path: Path | str = JOB_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA wal_autocheckpoint=500")
    conn.execute("PRAGMA journal_size_limit=67108864")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          provider TEXT NOT NULL,
          company TEXT NOT NULL,
          external_job_id TEXT NOT NULL,
          title TEXT NOT NULL,
          location TEXT,
          url TEXT,
          description TEXT,
          content_hash TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          first_seen_ts INTEGER NOT NULL,
          last_seen_ts INTEGER NOT NULL,
          UNIQUE(provider, external_job_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_classifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id INTEGER NOT NULL,
          content_hash TEXT NOT NULL,
          model TEXT NOT NULL,
          role_family TEXT NOT NULL,
          role_type TEXT NOT NULL,
          seniority TEXT NOT NULL,
          location_fit TEXT NOT NULL,
          relevance_score INTEGER NOT NULL,
          confidence REAL NOT NULL,
          reason TEXT NOT NULL,
          raw_response_json TEXT,
          classified_ts INTEGER NOT NULL,
          UNIQUE(job_id, content_hash, model),
          FOREIGN KEY(job_id) REFERENCES jobs(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_provider_company ON jobs(provider, company)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_content_hash ON jobs(content_hash)")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_classifications_score
        ON job_classifications(relevance_score DESC, classified_ts DESC)
        """
    )
    conn.commit()
    return conn


def compute_content_hash(job: NormalizedJob) -> str:
    meaningful_content = {
        "company": _clean(job.get("company")),
        "title": _clean(job.get("title")),
        "location": _clean(job.get("location")),
        "url": _clean(job.get("url")),
        "description": _clean(job.get("description")),
    }
    blob = json.dumps(meaningful_content, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def upsert_job(conn: sqlite3.Connection, job: NormalizedJob) -> SavedJob:
    provider = _required(job, "provider")
    company = _required(job, "company")
    external_job_id = _required(job, "external_job_id")
    title = _required(job, "title")
    location = _clean(job.get("location"))
    url = _clean(job.get("url"))
    description = _clean(job.get("description"))
    raw_json = json.dumps(job.get("raw") or {}, sort_keys=True, default=str)
    content_hash = compute_content_hash(job)
    now_ts = int(time.time())

    existing = conn.execute(
        """
        SELECT id, content_hash, last_seen_ts
        FROM jobs
        WHERE provider = ? AND external_job_id = ?
        """,
        (provider, external_job_id),
    ).fetchone()

    if existing:
        changed = str(existing["content_hash"]) != content_hash
        last_seen_ts = int(existing["last_seen_ts"] or 0)
        last_seen_interval = int(os.getenv("JOB_LAST_SEEN_UPDATE_INTERVAL_SECONDS", "21600"))
        if not changed and now_ts - last_seen_ts < last_seen_interval:
            return SavedJob(
                id=int(existing["id"]),
                provider=provider,
                company=company,
                external_job_id=external_job_id,
                content_hash=content_hash,
                is_new=False,
                changed=False,
            )

        if not changed:
            conn.execute(
                """
                UPDATE jobs
                SET last_seen_ts = ?
                WHERE id = ?
                """,
                (now_ts, int(existing["id"])),
            )
            conn.commit()
            return SavedJob(
                id=int(existing["id"]),
                provider=provider,
                company=company,
                external_job_id=external_job_id,
                content_hash=content_hash,
                is_new=False,
                changed=False,
            )

        conn.execute(
            """
            UPDATE jobs
            SET company = ?,
                title = ?,
                location = ?,
                url = ?,
                description = ?,
                content_hash = ?,
                raw_json = ?,
                last_seen_ts = ?
            WHERE id = ?
            """,
            (
                company,
                title,
                location,
                url,
                description,
                content_hash,
                raw_json,
                now_ts,
                int(existing["id"]),
            ),
        )
        conn.commit()
        return SavedJob(
            id=int(existing["id"]),
            provider=provider,
            company=company,
            external_job_id=external_job_id,
            content_hash=content_hash,
            is_new=False,
            changed=changed,
        )

    cursor = conn.execute(
        """
        INSERT INTO jobs (
            provider, company, external_job_id, title, location, url,
            description, content_hash, raw_json, first_seen_ts, last_seen_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            provider,
            company,
            external_job_id,
            title,
            location,
            url,
            description,
            content_hash,
            raw_json,
            now_ts,
            now_ts,
        ),
    )
    conn.commit()
    return SavedJob(
        id=int(cursor.lastrowid),
        provider=provider,
        company=company,
        external_job_id=external_job_id,
        content_hash=content_hash,
        is_new=True,
        changed=True,
    )


def checkpoint_job_db(conn: sqlite3.Connection, truncate: bool = False) -> None:
    mode = "TRUNCATE" if truncate else "PASSIVE"
    conn.execute(f"PRAGMA wal_checkpoint({mode})")


def get_cached_classification(
    conn: sqlite3.Connection,
    job_id: int,
    content_hash: str,
    model: str,
) -> Optional[Classification]:
    row = conn.execute(
        """
        SELECT role_family, role_type, seniority, location_fit, relevance_score,
               confidence, reason, model, classified_ts, raw_response_json
        FROM job_classifications
        WHERE job_id = ? AND content_hash = ? AND model = ?
        """,
        (job_id, content_hash, model),
    ).fetchone()
    return dict(row) if row else None


def save_classification(
    conn: sqlite3.Connection,
    job_id: int,
    content_hash: str,
    model: str,
    classification: Classification,
    raw_response_json: Optional[str] = None,
) -> Classification:
    classified_ts = int(time.time())
    payload = {
        "role_family": str(classification["role_family"]),
        "role_type": str(classification["role_type"]),
        "seniority": str(classification["seniority"]),
        "location_fit": str(classification["location_fit"]),
        "relevance_score": int(classification["relevance_score"]),
        "confidence": float(classification["confidence"]),
        "reason": str(classification["reason"]),
        "model": model,
        "classified_ts": classified_ts,
        "raw_response_json": raw_response_json,
    }
    conn.execute(
        """
        INSERT INTO job_classifications (
            job_id, content_hash, model, role_family, role_type, seniority,
            location_fit, relevance_score, confidence, reason,
            raw_response_json, classified_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id, content_hash, model) DO UPDATE SET
            role_family = excluded.role_family,
            role_type = excluded.role_type,
            seniority = excluded.seniority,
            location_fit = excluded.location_fit,
            relevance_score = excluded.relevance_score,
            confidence = excluded.confidence,
            reason = excluded.reason,
            raw_response_json = excluded.raw_response_json,
            classified_ts = excluded.classified_ts
        """,
        (
            job_id,
            content_hash,
            model,
            payload["role_family"],
            payload["role_type"],
            payload["seniority"],
            payload["location_fit"],
            payload["relevance_score"],
            payload["confidence"],
            payload["reason"],
            raw_response_json,
            classified_ts,
        ),
    )
    conn.commit()
    return payload


def get_intelligence_snapshot(
    db_path: Path | str = JOB_DB_PATH,
    min_relevance_score: int = 75,
    limit: int = 20,
) -> Dict[str, Any]:
    if not Path(db_path).exists():
        return {
            "total_jobs": 0,
            "classified_jobs": 0,
            "high_relevance_jobs": 0,
            "latest_high_relevance_jobs": [],
            "role_type_distribution": [],
            "seniority_distribution": [],
        }

    conn = init_job_db(db_path)
    total_jobs = conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()["n"]
    classified_jobs = conn.execute(
        """
        SELECT COUNT(DISTINCT job_id) AS n
        FROM job_classifications
        """
    ).fetchone()["n"]
    high_relevance_jobs = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM job_classifications c
        JOIN jobs j ON j.id = c.job_id AND j.content_hash = c.content_hash
        WHERE c.relevance_score >= ?
        """,
        (min_relevance_score,),
    ).fetchone()["n"]
    latest_high_relevance = [
        dict(row)
        for row in conn.execute(
            """
            SELECT j.provider, j.company, j.title, j.location, j.url,
                   c.role_type, c.seniority, c.location_fit,
                   c.relevance_score, c.confidence, c.reason, c.classified_ts
            FROM job_classifications c
            JOIN jobs j ON j.id = c.job_id AND j.content_hash = c.content_hash
            WHERE c.relevance_score >= ?
            ORDER BY c.classified_ts DESC, c.relevance_score DESC
            LIMIT ?
            """,
            (min_relevance_score, limit),
        ).fetchall()
    ]
    role_type_distribution = [
        dict(row)
        for row in conn.execute(
            """
            SELECT c.role_type, COUNT(*) AS count
            FROM job_classifications c
            JOIN jobs j ON j.id = c.job_id AND j.content_hash = c.content_hash
            GROUP BY c.role_type
            ORDER BY count DESC, c.role_type
            """
        ).fetchall()
    ]
    seniority_distribution = [
        dict(row)
        for row in conn.execute(
            """
            SELECT c.seniority, COUNT(*) AS count
            FROM job_classifications c
            JOIN jobs j ON j.id = c.job_id AND j.content_hash = c.content_hash
            GROUP BY c.seniority
            ORDER BY count DESC, c.seniority
            """
        ).fetchall()
    ]
    conn.close()
    return {
        "total_jobs": int(total_jobs),
        "classified_jobs": int(classified_jobs),
        "high_relevance_jobs": int(high_relevance_jobs),
        "latest_high_relevance_jobs": latest_high_relevance,
        "role_type_distribution": role_type_distribution,
        "seniority_distribution": seniority_distribution,
    }


def _required(job: NormalizedJob, key: str) -> str:
    value = _clean(job.get(key))
    if not value:
        raise ValueError(f"Normalized job is missing required field: {key}")
    return value


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
