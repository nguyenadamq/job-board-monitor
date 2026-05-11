import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, Iterable, List

from aiohttp import web
from dotenv import load_dotenv

from status_monitor import STATUS_DB_PATH, init_status_db, is_error_status
from structured_logging import configure_logger, log_event


load_dotenv(".env.local")
LOGGER = configure_logger("metrics")

METRICS_HOST = os.getenv("METRICS_HOST", "0.0.0.0")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9108"))


def metric_line(name: str, value: float | int, labels: Dict[str, object] | None = None) -> str:
    if not labels:
        return f"{name} {value}"
    rendered = ",".join(f'{key}="{escape_label(value)}"' for key, value in sorted(labels.items()))
    return f"{name}{{{rendered}}} {value}"


def escape_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def help_block(name: str, kind: str, description: str) -> List[str]:
    return [f"# HELP {name} {description}", f"# TYPE {name} {kind}"]


def render_metrics() -> str:
    now_ts = int(time.time())
    lines: List[str] = []

    lines.extend(help_block("job_tracker_exporter_up", "gauge", "Whether the job tracker metrics exporter is running."))
    lines.append(metric_line("job_tracker_exporter_up", 1))
    lines.extend(help_block("job_tracker_status_db_last_scrape_timestamp_seconds", "gauge", "Unix timestamp of the exporter scrape."))
    lines.append(metric_line("job_tracker_status_db_last_scrape_timestamp_seconds", now_ts))
    lines.extend(help_block("job_tracker_status_db_readable", "gauge", "Whether the monitor status SQLite database can be read."))

    try:
        conn = open_status_db()
        conn.row_factory = sqlite3.Row
        sources = [dict(row) for row in conn.execute(
            """
            SELECT service, source, status, is_error, last_checked_ts,
                   last_ok_ts, last_error_ts, consecutive_errors
            FROM source_status
            ORDER BY service, source
            """
        ).fetchall()]
        services = [dict(row) for row in conn.execute(
            """
            SELECT service, status, summary_json, last_cycle_ts, cycle_duration_ms
            FROM service_cycles
            ORDER BY service
            """
        ).fetchall()]
        recent_error_events = [dict(row) for row in conn.execute(
            """
            SELECT service, status, COUNT(*) AS count
            FROM source_events
            WHERE is_error = 1
            GROUP BY service, status
            ORDER BY service, status
            """
        ).fetchall()]
        conn.close()
        lines.append(metric_line("job_tracker_status_db_readable", 1))
    except sqlite3.DatabaseError as e:
        lines.append(metric_line("job_tracker_status_db_readable", 0))
        lines.extend(help_block("job_tracker_status_db_read_error", "gauge", "SQLite database read error by exception type."))
        lines.append(metric_line("job_tracker_status_db_read_error", 1, {"error_type": type(e).__name__}))
        return "\n".join(lines) + "\n"

    lines.extend(help_block("job_tracker_sources_total", "gauge", "Tracked source count by provider service."))
    lines.extend(help_block("job_tracker_source_errors", "gauge", "Active error source count by provider service."))
    lines.extend(help_block("job_tracker_source_error_ratio", "gauge", "Active source error ratio by provider service."))
    for service, rows in group_by_service(sources).items():
        total = len(rows)
        errors = sum(1 for row in rows if int(row.get("is_error") or 0) == 1)
        labels = {"service": service}
        lines.append(metric_line("job_tracker_sources_total", total, labels))
        lines.append(metric_line("job_tracker_source_errors", errors, labels))
        lines.append(metric_line("job_tracker_source_error_ratio", errors / total if total else 0, labels))

    lines.extend(help_block("job_tracker_source_error_state", "gauge", "Per-source active error state."))
    lines.extend(help_block("job_tracker_source_consecutive_errors", "gauge", "Per-source consecutive error count."))
    lines.extend(help_block("job_tracker_source_last_checked_timestamp_seconds", "gauge", "Unix timestamp of source last check."))
    lines.extend(help_block("job_tracker_source_last_error_timestamp_seconds", "gauge", "Unix timestamp of source last error."))
    for row in sources:
        labels = {
            "service": row["service"],
            "source": row["source"],
            "status": row["status"],
        }
        lines.append(metric_line("job_tracker_source_error_state", int(row["is_error"]), labels))
        lines.append(metric_line("job_tracker_source_consecutive_errors", int(row["consecutive_errors"] or 0), labels))
        lines.append(metric_line("job_tracker_source_last_checked_timestamp_seconds", int(row["last_checked_ts"] or 0), labels))
        if row.get("last_error_ts"):
            lines.append(metric_line("job_tracker_source_last_error_timestamp_seconds", int(row["last_error_ts"]), labels))

    lines.extend(help_block("job_tracker_service_cycle_duration_seconds", "gauge", "Latest monitor cycle duration by service."))
    lines.extend(help_block("job_tracker_service_last_cycle_timestamp_seconds", "gauge", "Latest monitor cycle Unix timestamp by service."))
    lines.extend(help_block("job_tracker_service_cycle_status", "gauge", "Latest monitor cycle status by service."))
    lines.extend(help_block("job_tracker_service_cycle_sources", "gauge", "Latest monitor cycle source count by status label."))
    lines.extend(help_block("job_tracker_service_cycle_error_sources", "gauge", "Latest monitor cycle source error count by service."))
    lines.extend(help_block("job_tracker_service_cycle_ok_sources", "gauge", "Latest monitor cycle non-error source count by service."))
    for service in services:
        service_name = service["service"]
        labels = {"service": service_name}
        lines.append(metric_line("job_tracker_service_cycle_duration_seconds", int(service["cycle_duration_ms"] or 0) / 1000, labels))
        lines.append(metric_line("job_tracker_service_last_cycle_timestamp_seconds", int(service["last_cycle_ts"] or 0), labels))
        lines.append(metric_line("job_tracker_service_cycle_status", 1, {**labels, "status": service["status"]}))

        summary = json.loads(service["summary_json"] or "{}")
        error_sources = 0
        ok_sources = 0
        for status, count in sorted(summary.items()):
            count = int(count)
            lines.append(metric_line("job_tracker_service_cycle_sources", count, {**labels, "source_status": status}))
            if is_error_status(status):
                error_sources += count
            else:
                ok_sources += count
        lines.append(metric_line("job_tracker_service_cycle_error_sources", error_sources, labels))
        lines.append(metric_line("job_tracker_service_cycle_ok_sources", ok_sources, labels))

    lines.extend(help_block("job_tracker_recent_error_events", "gauge", "Error events currently retained in SQLite by service and status."))
    for row in recent_error_events:
        lines.append(metric_line(
            "job_tracker_recent_error_events",
            int(row["count"] or 0),
            {"service": row["service"], "status": row["status"]},
        ))

    return "\n".join(lines) + "\n"


def open_status_db() -> sqlite3.Connection:
    path = Path(STATUS_DB_PATH)
    if not path.exists():
        return init_status_db()
    conn = sqlite3.connect(f"file:{path}?mode=ro", timeout=10, uri=True)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def group_by_service(rows: Iterable[Dict[str, object]]) -> Dict[str, List[Dict[str, object]]]:
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row["service"]), []).append(row)
    return grouped


async def metrics(_: web.Request) -> web.Response:
    try:
        return web.Response(text=render_metrics(), content_type="text/plain; version=0.0.4")
    except Exception as e:
        log_event(LOGGER, "metrics_render_failed", level="ERROR", error=str(e))
        return web.Response(text=f"# metrics render failed: {e}\n", status=500, content_type="text/plain")


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "status_db": str(STATUS_DB_PATH)})


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/health", health)
    return app


if __name__ == "__main__":
    log_event(LOGGER, "metrics_exporter_started", host=METRICS_HOST, port=METRICS_PORT)
    web.run_app(
        build_app(),
        host=METRICS_HOST,
        port=METRICS_PORT,
        print=lambda message: log_event(LOGGER, "metrics_runtime_message", message=message),
    )
