#!/usr/bin/env python3
import asyncio
import json
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from dotenv import load_dotenv

from job_filters import any_us_location, clean_texts, title_matches

load_dotenv(".env.local")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
JITTER_SECONDS = int(os.getenv("JITTER_SECONDS", "15"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "25"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_RIPPLEMATCH", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"
COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = Path(os.getenv("RIPPLEMATCH_COMPANIES_FILE", str(COMPANIES_DIR / "ripplematch_companies.txt")))
DB_PATH = WATCH_DIR / os.getenv("RIPPLEMATCH_DB", "ripplematch_watch.db")

ROLE_CARD_RE = re.compile(r"<article class=\"_roleCard.*?</article></li>", re.S)
TITLE_RE = re.compile(r"<h2 class=\"_cardTitle[^\"]*\">(.*?)</h2>", re.S)
DETAIL_URL_RE = re.compile(r'href=\"(https://app\.ripplematch\.com/v2/public/job/[^\"]+)\"')
LOCATION_BLOCK_RE = re.compile(r'<div class=\"_wrapIconText_[^\"]*\">.*?<span class=\"_truncate_[^\"]*\">(.*?)</span></div>', re.S)
LOCATION_VALUE_RE = re.compile(r"<span>(.*?)</span>", re.S)
COMPANY_RE = re.compile(r'<div class=\"_companyName_[^\"]*\"><span><a [^>]*>(.*?)</a></span></div>', re.S)
TOPLINE_RE = re.compile(r'<div class=\"_topline_[^\"]*\">(.*?)</div>', re.S)
TOPLINE_VALUE_RE = re.compile(r"<span>(.*?)</span>", re.S)


def normalize_source(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty RippleMatch source")
    if "://" not in text:
        text = "https://ripplematch.com/jobs/" + text.strip("/") + "/"
    parsed = urlparse(text)
    if "ripplematch.com" not in parsed.netloc:
        raise ValueError(f"Unsupported RippleMatch URL: {raw}")
    return text.rstrip("/") + "/"


def load_sources_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Companies file not found: {path}")
    sources: List[str] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = line.split("#", 1)[0].strip()
        if not raw:
            continue
        try:
            sources.append(normalize_source(raw))
        except Exception as exc:
            print(f"[warn] Skipping invalid line {line_no} in {path}: {raw!r} ({exc})")
    seen = set()
    out: List[str] = []
    for source in sources:
        if source in seen:
            continue
        seen.add(source)
        out.append(source)
    return out


def strip_tags(text: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", unescape(text or ""))
    values = clean_texts([stripped])
    return values[0] if values else ""


def parse_jobs(page_url: str, html: str) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    for card in ROLE_CARD_RE.findall(html):
        title_match = TITLE_RE.search(card)
        detail_match = DETAIL_URL_RE.search(card)
        company_match = COMPANY_RE.search(card)
        topline_match = TOPLINE_RE.search(card)
        if not title_match or not detail_match:
            continue

        title = strip_tags(title_match.group(1))
        detail_url = urljoin(page_url, detail_match.group(1))
        company = strip_tags(company_match.group(1)) if company_match else ""
        topline_values = TOPLINE_VALUE_RE.findall(topline_match.group(1)) if topline_match else []

        location_blocks = LOCATION_BLOCK_RE.findall(card)
        location_values: List[str] = []
        if location_blocks:
            location_values = [strip_tags(value) for value in LOCATION_VALUE_RE.findall(location_blocks[0])]

        jobs.append(
            {
                "id": detail_url.rsplit("/", 1)[-1],
                "title": title,
                "company": company,
                "url": detail_url,
                "location": " | ".join(clean_texts(location_values)),
                "workplace": " | ".join(clean_texts(strip_tags(v) for v in topline_values[1:])),
            }
        )
    return jobs


@dataclass
class StoredState:
    source_url: str
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_state (
            source_url TEXT PRIMARY KEY,
            last_seen_ts INTEGER,
            notified_job_ids_json TEXT
        )
        """
    )
    conn.commit()
    return conn


def load_state(conn: sqlite3.Connection, source_url: str) -> StoredState:
    row = conn.execute(
        "SELECT source_url, last_seen_ts, notified_job_ids_json FROM source_state WHERE source_url = ?",
        (source_url,),
    ).fetchone()
    if not row:
        return StoredState(source_url, None, None)
    return StoredState(row[0], row[1], row[2])


def save_state(conn: sqlite3.Connection, source_url: str, last_seen_ts: int, notified_job_ids_json: str) -> None:
    conn.execute(
        """
        INSERT INTO source_state (source_url, last_seen_ts, notified_job_ids_json)
        VALUES (?, ?, ?)
        ON CONFLICT(source_url) DO UPDATE SET
            last_seen_ts=excluded.last_seen_ts,
            notified_job_ids_json=excluded.notified_job_ids_json
        """,
        (source_url, last_seen_ts, notified_job_ids_json),
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


def format_message(source_url: str, jobs: List[Dict[str, str]], limit: int = 15) -> str:
    lines = [f"Board: {source_url}"]
    for job in jobs[:limit]:
        label = f"{job['title']} @ {job['company']}" if job.get("company") else job["title"]
        location = job.get("location", "")
        lines.append(f"{label} ({location}) | {job['url']}" if location else f"{label} | {job['url']}")
    return "\n".join(lines)


async def fetch_source(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    source_url: str,
) -> Tuple[str, str]:
    prior = load_state(conn, source_url)
    now_ts = int(time.time())
    try:
        notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
    except Exception:
        notified_ids = set()

    try:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
        async with session.get(source_url, headers=headers) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            html = await resp.text()

        jobs = parse_jobs(source_url, html)
        new_matching: List[Dict[str, str]] = []
        for job in jobs:
            job_id = job.get("id", "")
            if not job_id or job_id in notified_ids:
                continue
            if not title_matches(job.get("title", "")):
                continue
            location_texts = clean_texts([job.get("location", ""), job.get("workplace", "")])
            if not any_us_location(location_texts):
                continue
            new_matching.append(job)
            notified_ids.add(job_id)

        if new_matching:
            await notify(session, format_message(source_url, new_matching))

        save_state(conn, source_url, now_ts, json.dumps(sorted(notified_ids)))
        return source_url, "new match" if new_matching else "ok"
    except asyncio.TimeoutError:
        save_state(conn, source_url, now_ts, prior.notified_job_ids_json or "[]")
        return source_url, "timeout"
    except Exception as exc:
        save_state(conn, source_url, now_ts, prior.notified_job_ids_json or "[]")
        return source_url, f"exception: {exc}"


async def run_forever() -> None:
    sources = load_sources_from_file(COMPANIES_FILE)
    if not sources:
        raise RuntimeError(f"No valid RippleMatch source pages found in {COMPANIES_FILE}")

    print(f"Watching {len(sources)} RippleMatch source pages (from {COMPANIES_FILE})")

    conn = init_db()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded(source_url: str) -> Tuple[str, str]:
            async with sem:
                return await fetch_source(session, conn, source_url)

        while True:
            start = time.time()
            tasks = [asyncio.create_task(bounded(source_url)) for source_url in sources]
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
