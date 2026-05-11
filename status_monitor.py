import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List

from job_store import get_intelligence_snapshot


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
WATCH_DIR = DATA_DIR / "watch"
WATCH_DIR.mkdir(parents=True, exist_ok=True)

STATUS_DB_PATH = WATCH_DIR / os.getenv("STATUS_DB", "monitor_status.db")

NON_ERROR_STATUSES = {"ok", "new match", "unchanged (304)"}


def init_status_db() -> sqlite3.Connection:
    try:
        return _init_status_db()
    except sqlite3.DatabaseError as e:
        print(f"[warn] status db appears unhealthy; moving it aside and recreating: {e}", flush=True)
        quarantine_status_db()
        return _init_status_db()


def _init_status_db() -> sqlite3.Connection:
    conn = sqlite3.connect(STATUS_DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_status (
            service TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT,
            is_error INTEGER NOT NULL DEFAULT 0,
            last_checked_ts INTEGER NOT NULL,
            last_ok_ts INTEGER,
            last_error_ts INTEGER,
            consecutive_errors INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (service, source)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT,
            is_error INTEGER NOT NULL DEFAULT 0,
            created_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_cycles (
            service TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            last_cycle_ts INTEGER NOT NULL,
            cycle_duration_ms INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_cycle_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service TEXT NOT NULL,
            status TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            error_count INTEGER NOT NULL DEFAULT 0,
            ok_count INTEGER NOT NULL DEFAULT 0,
            last_cycle_ts INTEGER NOT NULL,
            cycle_duration_ms INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def quarantine_status_db() -> None:
    stamp = int(time.time())
    for suffix in ("", "-wal", "-shm", "-journal"):
        path = Path(f"{STATUS_DB_PATH}{suffix}")
        if not path.exists():
            continue
        target = path.with_name(f"{path.name}.corrupt.{stamp}")
        try:
            path.replace(target)
        except OSError as e:
            print(f"[warn] failed to move unhealthy status db file {path}: {e}", flush=True)


def is_error_status(status: str) -> bool:
    normalized = (status or "").strip().lower()
    return normalized not in NON_ERROR_STATUSES


def record_source_status(
    conn: sqlite3.Connection,
    service: str,
    source: str,
    status: str,
    detail: str = "",
) -> None:
    try:
        _record_source_status(conn, service, source, status, detail)
    except sqlite3.DatabaseError as e:
        print(f"[warn] status db write failed for {service}/{source}: {e}", flush=True)


def _record_source_status(
    conn: sqlite3.Connection,
    service: str,
    source: str,
    status: str,
    detail: str = "",
) -> None:
    now_ts = int(time.time())
    error = 1 if is_error_status(status) else 0

    previous = conn.execute(
        """
        SELECT status, detail, is_error, consecutive_errors
        FROM source_status
        WHERE service = ? AND source = ?
        """,
        (service, source),
    ).fetchone()

    if previous:
        prev_status, prev_detail, prev_is_error, prev_errors = previous
        consecutive_errors = (prev_errors + 1) if error else 0
        last_ok_ts = now_ts if not error else None
        last_error_ts = now_ts if error else None

        conn.execute(
            """
            UPDATE source_status
            SET status = ?,
                detail = ?,
                is_error = ?,
                last_checked_ts = ?,
                last_ok_ts = COALESCE(?, last_ok_ts),
                last_error_ts = COALESCE(?, last_error_ts),
                consecutive_errors = ?
            WHERE service = ? AND source = ?
            """,
            (
                status,
                detail,
                error,
                now_ts,
                last_ok_ts,
                last_error_ts,
                consecutive_errors,
                service,
                source,
            ),
        )

        changed = (
            prev_status != status
            or (prev_detail or "") != (detail or "")
            or prev_is_error != error
        )
    else:
        conn.execute(
            """
            INSERT INTO source_status (
                service, source, status, detail, is_error,
                last_checked_ts, last_ok_ts, last_error_ts, consecutive_errors
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                service,
                source,
                status,
                detail,
                error,
                now_ts,
                now_ts if not error else None,
                now_ts if error else None,
                1 if error else 0,
            ),
        )
        changed = True

    if error or changed:
        conn.execute(
            """
            INSERT INTO source_events (service, source, status, detail, is_error, created_ts)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (service, source, status, detail, error, now_ts),
        )
        conn.execute(
            """
            DELETE FROM source_events
            WHERE id NOT IN (
                SELECT id FROM source_events
                ORDER BY id DESC
                LIMIT 1000
            )
            """
        )

    conn.commit()


def record_cycle(
    conn: sqlite3.Connection,
    service: str,
    summary: Dict[str, int],
    cycle_duration_ms: int,
) -> None:
    try:
        _record_cycle(conn, service, summary, cycle_duration_ms)
    except sqlite3.DatabaseError as e:
        print(f"[warn] status db cycle write failed for {service}: {e}", flush=True)


def _record_cycle(
    conn: sqlite3.Connection,
    service: str,
    summary: Dict[str, int],
    cycle_duration_ms: int,
) -> None:
    now_ts = int(time.time())
    status = "error" if any(is_error_status(key) and count > 0 for key, count in summary.items()) else "ok"
    error_count = sum(count for key, count in summary.items() if is_error_status(key))
    ok_count = sum(count for key, count in summary.items() if not is_error_status(key))
    conn.execute(
        """
        INSERT INTO service_cycles (service, status, summary_json, last_cycle_ts, cycle_duration_ms)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(service) DO UPDATE SET
            status = excluded.status,
            summary_json = excluded.summary_json,
            last_cycle_ts = excluded.last_cycle_ts,
            cycle_duration_ms = excluded.cycle_duration_ms
        """,
        (service, status, json.dumps(summary, sort_keys=True), now_ts, cycle_duration_ms),
    )
    conn.execute(
        """
        INSERT INTO service_cycle_history (
            service, status, summary_json, error_count, ok_count, last_cycle_ts, cycle_duration_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            service,
            status,
            json.dumps(summary, sort_keys=True),
            error_count,
            ok_count,
            now_ts,
            cycle_duration_ms,
        ),
    )
    conn.execute(
        """
        DELETE FROM service_cycle_history
        WHERE id NOT IN (
            SELECT id FROM service_cycle_history
            ORDER BY id DESC
            LIMIT 3000
        )
        """
    )
    conn.commit()


def get_dashboard_snapshot() -> Dict[str, List[Dict[str, object]]]:
    conn = init_status_db()
    conn.row_factory = sqlite3.Row

    services = [dict(row) for row in conn.execute(
        "SELECT service, status, summary_json, last_cycle_ts, cycle_duration_ms FROM service_cycles ORDER BY service"
    ).fetchall()]
    for service in services:
        service["summary"] = json.loads(service.pop("summary_json"))

    active_errors = [dict(row) for row in conn.execute(
        """
        SELECT service, source, status, detail, last_checked_ts, last_error_ts, consecutive_errors
        FROM source_status
        WHERE is_error = 1
        ORDER BY service, last_error_ts DESC, source
        """
    ).fetchall()]

    recent_events = [dict(row) for row in conn.execute(
        """
        SELECT id, service, source, status, detail, is_error, created_ts
        FROM source_events
        ORDER BY id DESC
        LIMIT 100
        """
    ).fetchall()]

    latest_sources = [dict(row) for row in conn.execute(
        """
        SELECT service, source, status, detail, is_error, last_checked_ts, consecutive_errors
        FROM source_status
        ORDER BY service, source
        """
    ).fetchall()]

    history_rows = [dict(row) for row in conn.execute(
        """
        SELECT service, status, error_count, ok_count, last_cycle_ts, cycle_duration_ms
        FROM service_cycle_history
        WHERE id IN (
            SELECT id FROM service_cycle_history
            ORDER BY last_cycle_ts DESC
            LIMIT 360
        )
        ORDER BY service, last_cycle_ts
        """
    ).fetchall()]

    error_groups: Dict[str, List[Dict[str, object]]] = {}
    for row in active_errors:
        error_groups.setdefault(str(row["service"]), []).append(row)

    history_by_service: Dict[str, List[Dict[str, object]]] = {}
    for row in history_rows:
        history_by_service.setdefault(str(row["service"]), []).append(row)

    conn.close()
    try:
        min_relevance = int(os.getenv("MIN_RELEVANCE_SCORE", "75"))
        intelligence = get_intelligence_snapshot(min_relevance_score=min_relevance)
    except Exception as e:
        intelligence = {
            "error": str(e),
            "total_jobs": 0,
            "classified_jobs": 0,
            "high_relevance_jobs": 0,
            "latest_high_relevance_jobs": [],
            "role_type_distribution": [],
            "seniority_distribution": [],
        }
    return {
        "services": services,
        "active_errors": active_errors,
        "error_groups": error_groups,
        "history_by_service": history_by_service,
        "recent_events": recent_events,
        "latest_sources": latest_sources,
        "intelligence": intelligence,
    }
