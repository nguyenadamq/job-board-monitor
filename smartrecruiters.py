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

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_SMARTRECRUITERS", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"
COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = Path(
    os.getenv("SMARTRECRUITERS_COMPANIES_FILE", str(COMPANIES_DIR / "smartrecruiters_companies.txt"))
)
DB_PATH = WATCH_DIR / os.getenv("SMARTRECRUITERS_DB", "smartrecruiters_watch.db")


def normalize_company_identifier(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty company identifier")
    if "://" not in text and "smartrecruiters.com" not in text:
        return text.strip("/").split("/", 1)[0]
    if "://" not in text:
        text = "https://" + text
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError(f"Could not parse SmartRecruiters company from {raw}")
    return parts[0]


def load_companies_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Companies file not found: {path}")
    companies: List[str] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = line.split("#", 1)[0].strip()
        if not raw:
            continue
        try:
            companies.append(normalize_company_identifier(raw))
        except Exception as exc:
            print(f"[warn] Skipping invalid line {line_no} in {path}: {raw!r} ({exc})")
    seen = set()
    out: List[str] = []
    for company in companies:
        key = company.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(company)
    return out


def postings_url(company: str, offset: int = 0, limit: int = 100) -> str:
    return f"https://api.smartrecruiters.com/v1/companies/{company}/postings?offset={offset}&limit={limit}"


def posting_url(company: str, posting: Dict[str, object]) -> str:
    explicit = str(posting.get("postingUrl") or "").strip()
    if explicit:
        return explicit
    posting_id = str(posting.get("id") or "").strip()
    title = str(posting.get("name") or "").strip().lower()
    slug = "".join(ch if ch.isalnum() else "-" for ch in title)
    slug = "-".join(part for part in slug.split("-") if part)
    suffix = f"-{slug}" if slug else ""
    return f"https://jobs.smartrecruiters.com/{company}/{posting_id}{suffix}"


def extract_location_texts(posting: Dict[str, object]) -> List[str]:
    location = posting.get("location")
    if isinstance(location, dict):
        return clean_texts(
            [
                location.get("fullLocation"),
                location.get("city"),
                location.get("region"),
                location.get("country"),
            ]
        )
    return []


@dataclass
class StoredState:
    company: str
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_state (
            company TEXT PRIMARY KEY,
            last_seen_ts INTEGER,
            notified_job_ids_json TEXT
        )
        """
    )
    conn.commit()
    return conn


def load_state(conn: sqlite3.Connection, company: str) -> StoredState:
    row = conn.execute(
        "SELECT company, last_seen_ts, notified_job_ids_json FROM company_state WHERE company = ?",
        (company,),
    ).fetchone()
    if not row:
        return StoredState(company, None, None)
    return StoredState(row[0], row[1], row[2])


def save_state(conn: sqlite3.Connection, company: str, last_seen_ts: int, notified_job_ids_json: str) -> None:
    conn.execute(
        """
        INSERT INTO company_state (company, last_seen_ts, notified_job_ids_json)
        VALUES (?, ?, ?)
        ON CONFLICT(company) DO UPDATE SET
            last_seen_ts=excluded.last_seen_ts,
            notified_job_ids_json=excluded.notified_job_ids_json
        """,
        (company, last_seen_ts, notified_job_ids_json),
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


def format_message(company: str, jobs: List[Dict[str, object]], limit: int = 15) -> str:
    lines = [f"Board: https://jobs.smartrecruiters.com/{company}"]
    for job in jobs[:limit]:
        title = str(job.get("name") or "").strip()
        location = " | ".join(extract_location_texts(job))
        url = posting_url(company, job)
        lines.append(f"{title} ({location}) | {url}" if location else f"{title} | {url}")
    return "\n".join(lines)


async def fetch_postings(session: aiohttp.ClientSession, company: str) -> List[Dict[str, object]]:
    all_postings: List[Dict[str, object]] = []
    offset = 0
    limit = 100
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    while True:
        async with session.get(postings_url(company, offset=offset, limit=limit), headers=headers) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            data = await resp.json(content_type=None)
        page = data.get("content") or []
        if not isinstance(page, list):
            break
        all_postings.extend(item for item in page if isinstance(item, dict))
        total_found = int(data.get("totalFound") or 0)
        offset += len(page)
        if not page or offset >= total_found:
            break
    return all_postings


async def fetch_company(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    company: str,
) -> Tuple[str, str]:
    prior = load_state(conn, company)
    now_ts = int(time.time())
    try:
        notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
    except Exception:
        notified_ids = set()

    try:
        postings = await fetch_postings(session, company)
        new_matching: List[Dict[str, object]] = []
        for posting in postings:
            posting_id = str(posting.get("id") or "").strip()
            if not posting_id or posting_id in notified_ids:
                continue
            title = str(posting.get("name") or "").strip()
            if not title_matches(title):
                continue
            if not any_us_location(extract_location_texts(posting)):
                continue
            new_matching.append(posting)
            notified_ids.add(posting_id)

        if new_matching:
            await notify(session, format_message(company, new_matching))

        save_state(conn, company, now_ts, json.dumps(sorted(notified_ids)))
        return company, "new match" if new_matching else "ok"
    except asyncio.TimeoutError:
        save_state(conn, company, now_ts, prior.notified_job_ids_json or "[]")
        return company, "timeout"
    except Exception as exc:
        save_state(conn, company, now_ts, prior.notified_job_ids_json or "[]")
        return company, f"exception: {exc}"


async def run_forever() -> None:
    companies = load_companies_from_file(COMPANIES_FILE)
    if not companies:
        raise RuntimeError(f"No valid companies found in {COMPANIES_FILE}")

    print(f"Watching {len(companies)} SmartRecruiters boards (from {COMPANIES_FILE})")

    conn = init_db()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded(company: str) -> Tuple[str, str]:
            async with sem:
                return await fetch_company(session, conn, company)

        while True:
            start = time.time()
            tasks = [asyncio.create_task(bounded(company)) for company in companies]
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
