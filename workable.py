#!/usr/bin/env python3
import asyncio
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

from job_filters import any_us_location, clean_texts, title_matches

load_dotenv(".env.local")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
JITTER_SECONDS = int(os.getenv("JITTER_SECONDS", "15"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "25"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_WORKABLE", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"
COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = Path(os.getenv("WORKABLE_COMPANIES_FILE", str(COMPANIES_DIR / "workable_companies.txt")))
DB_PATH = WATCH_DIR / os.getenv("WORKABLE_DB", "workable_watch.db")


def normalize_account(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty Workable account")
    if "://" not in text and "apply.workable.com" not in text:
        return text.strip("/").split("/", 1)[0]
    if "://" not in text:
        text = "https://" + text
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError(f"Could not parse Workable account from {raw}")
    return parts[0]


def load_accounts_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Companies file not found: {path}")
    accounts: List[str] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = line.split("#", 1)[0].strip()
        if not raw:
            continue
        try:
            accounts.append(normalize_account(raw))
        except Exception as exc:
            print(f"[warn] Skipping invalid line {line_no} in {path}: {raw!r} ({exc})")
    seen = set()
    out: List[str] = []
    for account in accounts:
        if account in seen:
            continue
        seen.add(account)
        out.append(account)
    return out


def api_url(account: str) -> str:
    return f"https://apply.workable.com/api/v3/accounts/{account}/jobs"


def job_url(account: str, job: Dict[str, object]) -> str:
    shortcode = str(job.get("shortcode") or "").strip()
    if shortcode:
        return f"https://apply.workable.com/{account}/j/{shortcode}/"
    return f"https://apply.workable.com/{account}/"


def extract_location_texts(job: Dict[str, object]) -> List[str]:
    texts: List[str] = []
    location = job.get("location")
    if isinstance(location, dict):
        texts.extend(
            clean_texts(
                [
                    location.get("city"),
                    location.get("region"),
                    location.get("country"),
                    location.get("countryCode"),
                ]
            )
        )
    locations = job.get("locations")
    if isinstance(locations, list):
        for item in locations:
            if isinstance(item, dict):
                texts.extend(
                    clean_texts(
                        [
                            item.get("city"),
                            item.get("region"),
                            item.get("country"),
                            item.get("countryCode"),
                        ]
                    )
                )
    workplace = job.get("workplace")
    if workplace:
        texts.append(str(workplace))
    if job.get("remote") is True:
        texts.append("remote")
    return clean_texts(texts)


@dataclass
class StoredState:
    account: str
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_state (
            account TEXT PRIMARY KEY,
            last_seen_ts INTEGER,
            notified_job_ids_json TEXT
        )
        """
    )
    conn.commit()
    return conn


def load_state(conn: sqlite3.Connection, account: str) -> StoredState:
    row = conn.execute(
        "SELECT account, last_seen_ts, notified_job_ids_json FROM account_state WHERE account = ?",
        (account,),
    ).fetchone()
    if not row:
        return StoredState(account, None, None)
    return StoredState(row[0], row[1], row[2])


def save_state(conn: sqlite3.Connection, account: str, last_seen_ts: int, notified_job_ids_json: str) -> None:
    conn.execute(
        """
        INSERT INTO account_state (account, last_seen_ts, notified_job_ids_json)
        VALUES (?, ?, ?)
        ON CONFLICT(account) DO UPDATE SET
            last_seen_ts=excluded.last_seen_ts,
            notified_job_ids_json=excluded.notified_job_ids_json
        """,
        (account, last_seen_ts, notified_job_ids_json),
    )
    conn.commit()


async def post_webhook(session: aiohttp.ClientSession, url: str, payload: Dict[str, str]) -> None:
    if not url:
        return
    try:
        await session.post(url, json=payload, timeout=TIMEOUT_SECONDS)
    except Exception as exc:
        print(f"[warn] webhook post failed: {exc}")


async def notify(session: aiohttp.ClientSession, message: str) -> None:
    print(message)
    if SLACK_WEBHOOK_URL:
        await post_webhook(session, SLACK_WEBHOOK_URL, {"text": message})
    if DISCORD_WEBHOOK_URL:
        lines = message.splitlines()
        chunk = ""
        for line in lines:
            candidate = f"{chunk}\n{line}" if chunk else line
            if len(candidate) > 1900:
                await post_webhook(session, DISCORD_WEBHOOK_URL, {"content": chunk})
                chunk = line
            else:
                chunk = candidate
        if chunk:
            await post_webhook(session, DISCORD_WEBHOOK_URL, {"content": chunk})


def format_message(account: str, jobs: List[Dict[str, object]], limit: int = 15) -> str:
    lines = [f"Board: https://apply.workable.com/{account}/"]
    for job in jobs[:limit]:
        title = str(job.get("title") or "").strip()
        location = " | ".join(extract_location_texts(job))
        url = job_url(account, job)
        lines.append(f"{title} ({location}) | {url}" if location else f"{title} | {url}")
    return "\n".join(lines)


async def fetch_jobs(session: aiohttp.ClientSession, account: str) -> List[Dict[str, object]]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://apply.workable.com",
        "Referer": f"https://apply.workable.com/{account}/",
    }
    async with session.post(api_url(account), json={}, headers=headers) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        data = await resp.json(content_type=None)
    results = data.get("results") or []
    return [item for item in results if isinstance(item, dict)]


async def fetch_account(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    account: str,
) -> Tuple[str, str]:
    prior = load_state(conn, account)
    now_ts = int(time.time())
    try:
        notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
    except Exception:
        notified_ids = set()

    try:
        jobs = await fetch_jobs(session, account)
        new_matching: List[Dict[str, object]] = []
        for job in jobs:
            job_id = str(job.get("id") or "").strip()
            if not job_id or job_id in notified_ids:
                continue
            title = str(job.get("title") or "").strip()
            if not title_matches(title):
                continue
            if not any_us_location(extract_location_texts(job)):
                continue
            new_matching.append(job)
            notified_ids.add(job_id)

        if new_matching:
            await notify(session, format_message(account, new_matching))

        save_state(conn, account, now_ts, json.dumps(sorted(notified_ids)))
        return account, "new match" if new_matching else "ok"
    except asyncio.TimeoutError:
        save_state(conn, account, now_ts, prior.notified_job_ids_json or "[]")
        return account, "timeout"
    except Exception as exc:
        save_state(conn, account, now_ts, prior.notified_job_ids_json or "[]")
        return account, f"exception: {exc}"


async def run_forever() -> None:
    accounts = load_accounts_from_file(COMPANIES_FILE)
    if not accounts:
        raise RuntimeError(f"No valid Workable accounts found in {COMPANIES_FILE}")

    print(f"Watching {len(accounts)} Workable boards (from {COMPANIES_FILE})")

    conn = init_db()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded(account: str) -> Tuple[str, str]:
            async with sem:
                return await fetch_account(session, conn, account)

        while True:
            start = time.time()
            tasks = [asyncio.create_task(bounded(account)) for account in accounts]
            results = await asyncio.gather(*tasks)
            counts: Dict[str, int] = {}
            for _, status in results:
                counts[status] = counts.get(status, 0) + 1
            summary = ", ".join(f"{key}: {value}" for key, value in sorted(counts.items()))
            print(f"Cycle done in {time.time() - start:.1f}s. {summary}")
            await asyncio.sleep(POLL_INTERVAL_SECONDS + random.randint(0, JITTER_SECONDS))


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("Stopped.")
