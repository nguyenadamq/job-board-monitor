import asyncio
import hashlib
import json
import os
import random
import re
import sqlite3
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Tuple, List
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv
from status_monitor import init_status_db, record_cycle, record_source_status

# Load local env file
load_dotenv(".env.local")

# ----------------------------
# Configuration
# ----------------------------
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
JITTER_SECONDS = int(os.getenv("JITTER_SECONDS", "10"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "20"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "20"))

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_LEVER", "").strip()

#Directory 
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"

COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = COMPANIES_DIR / "lever_companies.txt"

DB_PATH = WATCH_DIR / os.getenv("LEVER_DB", "lever_watch.db")

# ----------------------------
# Lever endpoints/helpers
# ----------------------------
def normalize_lever_company(raw: str) -> str:
    """
    Accepts:
      - company slug (zoox)
      - https://jobs.lever.co/zoox
      - jobs.lever.co/zoox
    Returns:
      - company slug
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty line")

    if "://" not in s and "jobs.lever.co" not in s:
        return s.strip("/")

    if "://" not in s and "jobs.lever.co" in s:
        s = "https://" + s

    parsed = urlparse(s)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        raise ValueError(f"Could not parse company from {raw}")
    return parts[0]


def lever_postings_api(company: str) -> str:
    # Unauthenticated public endpoint
    return f"https://api.lever.co/v0/postings/{company}?mode=json"


def job_absolute_url(company: str, host: str, apply_url: Optional[str], posting_id: Optional[str]) -> str:
    """
    Prefer applyUrl from API. Fallback to jobs.lever.co/{company}/{id}
    """
    if apply_url:
        return apply_url
    if posting_id:
        return f"https://jobs.lever.co/{company}/{posting_id}"
    return f"https://jobs.lever.co/{company}"


def load_companies_from_file(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Companies file not found: {path}. Create it with one company slug or jobs.lever.co URL per line."
        )

    companies: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            raw = raw.split("#", 1)[0].strip()
            if not raw:
                continue

            try:
                company = normalize_lever_company(raw)
                companies.append(company)
            except Exception as e:
                print(f"[warn] Skipping invalid line {line_no} in {path}: {line!r} ({e})")

    seen = set()
    out: List[str] = []
    for c in companies:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def stable_fingerprint(postings_json: list) -> str:
    # Store stable subset, similar to your Greenhouse approach
    compact = [{"id": p.get("id"), "updatedAt": p.get("updatedAt")} for p in (postings_json or [])]
    blob = json.dumps(compact, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def format_new_jobs_message(company: str, new_jobs: list, limit: int = 15) -> str:
    board_url = f"https://jobs.lever.co/{company}"
    lines = [f"Board: {board_url}"]

    def sort_key(p: dict):
        # updatedAt is ms since epoch; id is string
        return (p.get("updatedAt") or 0, p.get("id") or "")

    for p in sorted(new_jobs, key=sort_key)[:limit]:
        title = (p.get("text") or "").strip()
        apply_url = p.get("applyUrl")
        pid = p.get("id")
        url = job_absolute_url(company, board_url, apply_url, pid)
        if title and url:
            lines.append(f"{title} | {url}")

    return "\n".join(lines)


# ----------------------------
# Title matching (broader, robust)
# ----------------------------

# Exclude clearly non-target or senior/management titles.
# Keep this list conservative so you do not miss good SWE roles.
EXCLUDE_TITLE_PATTERNS = [
    r"\bsenior\b",
    r"\bsr\.?\b",
    r"\bstaff\b",
    r"\bprincipal\b",
    r"\blead\b",
    r"\bmanager\b",
    r"\bdirector\b",
    r"\bhead\b",
    r"\bvp\b",
    r"\bvice president\b",
    r"\bchief\b",

    # Non-target specialties (tune if you want these)
    r"\bmachine\s*learning\b",
    r"\bml\b",
    r"\bdata\b",
    r"\banalytics\b",
    r"\bsecurity\b",
    r"\binfrastructure\b",
    r"\bsre\b",
    r"\bsite reliability\b",
    r"\bdevops\b",
    r"\bembedded\b",
    r"\bfirmware\b",
    r"\bnetwork\b",
    r"\btest\b",
    r"\bqa\b",
    r"\bquality\b",
    r"\bautomation\b",
    r"\bmobile\b",
    r"\bios\b",
    r"\bandroid\b",
    r"\bgame\b",
    r"\bgraphics\b",
    r"\brobotics\b",
    r"\breliability\b",
]

# Role family patterns: if any match, we treat it as SWE-related.
ROLE_INCLUDE_PATTERNS = [
    r"\bsoftware\s+engineer\b",
    r"\bsoftware\s+developer\b",
    r"\bbackend\b.*\b(engineer|developer)\b",
    r"\bfront\s*end\b.*\b(engineer|developer)\b",
    r"\bfrontend\b.*\b(engineer|developer)\b",
    r"\bfull\s*stack\b.*\b(engineer|developer)\b",
    r"\bfullstack\b.*\b(engineer|developer)\b",
    r"\bweb\b.*\b(engineer|developer)\b",
    r"\bapplication\b.*\b(engineer|developer)\b",
    r"\bplatform\b.*\b(engineer|developer)\b",
    r"\bapi\b.*\b(engineer|developer)\b",
]

# Optional level hints for early career. Not required unless you uncomment in title_matches().
LEVEL_HINT_PATTERNS = [
    r"\bintern\b",
    r"\bco[-\s]?op\b",
    r"\bnew\s*grad\b",
    r"\bgraduate\b",
    r"\bearly\s*career\b",
    r"\bentry\b",
    r"\bentry[-\s]?level\b",
    r"\bjunior\b",
    r"\bassociate\b",
    r"\b(level|lvl)\s*(1|i)\b",
    r"\bsoftware\s+engineer\s*(1|i)\b",
    r"\bengineer\s*(1|i)\b",
]

def normalize_title(title: str) -> str:
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def title_matches(title: str) -> bool:
    t = normalize_title(title)

    # Hard exclude first
    if any(re.search(p, t) for p in EXCLUDE_TITLE_PATTERNS):
        return False

    # Must look SWE-ish
    if not any(re.search(p, t) for p in ROLE_INCLUDE_PATTERNS):
        return False

    # If you want only early-career postings, uncomment this.
    # if not any(re.search(p, t) for p in LEVEL_HINT_PATTERNS):
    #     return False

    return True

# ----------------------------
# US location matching (same as your Greenhouse program)
# Lever postings may have 'categories' and 'workplaceType'/'location'
# We'll try multiple fields.
# ----------------------------
US_STATES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware",
    "florida","georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana",
    "maine","maryland","massachusetts","michigan","minnesota","mississippi","missouri","montana",
    "nebraska","nevada","new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island","south carolina",
    "south dakota","tennessee","texas","utah","vermont","virginia","washington","west virginia",
    "wisconsin","wyoming",
    "district of columbia","washington dc","d c","d.c."
}

US_STATE_ABBR = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la",
    "me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
    "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc"
}

US_COUNTRY_PATTERNS = [
    r"\busa\b",
    r"\bu\.s\.a\.?\b",
    r"\bu\.s\.?\b",
    r"\bunited states\b",
    r"\bunited states of america\b",
]

REMOTE_US_PATTERNS = [
    r"\bremote\b.*\b(us|usa|u\.s\.?|united states)\b",
    r"\b(us|usa|u\.s\.?|united states)\b.*\bremote\b",
]


def extract_location_texts(posting: dict) -> List[str]:
    texts: List[str] = []

    # Lever field examples:
    # posting["categories"]["location"]
    cats = posting.get("categories")
    if isinstance(cats, dict):
        loc = cats.get("location")
        if loc:
            texts.append(str(loc))

    # Some boards also include a top-level "location" object or "workplaceType"
    loc_obj = posting.get("location")
    if isinstance(loc_obj, dict):
        name = loc_obj.get("name")
        if name:
            texts.append(str(name))
    elif isinstance(loc_obj, str):
        texts.append(loc_obj)

    wt = posting.get("workplaceType")
    if wt:
        texts.append(str(wt))

    # Fallback: sometimes team/department strings include regions
    for k in ("team", "department"):
        v = posting.get(k)
        if v:
            texts.append(str(v))

    return [t.strip() for t in texts if t and str(t).strip()]


def is_us_location_text(text: str) -> bool:
    t = (text or "").lower()

    if any(re.search(p, t) for p in US_COUNTRY_PATTERNS):
        return True

    if any(re.search(p, t) for p in REMOTE_US_PATTERNS):
        return True

    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", t):
            return True

    for abbr in US_STATE_ABBR:
        if re.search(rf"(?<![a-z]){abbr}(?![a-z])", t):
            return True

    return False


def posting_is_us(posting: dict) -> bool:
    return any(is_us_location_text(t) for t in extract_location_texts(posting))


# ----------------------------
# Storage (sqlite)
# ----------------------------
@dataclass
class StoredState:
    etag: Optional[str]
    last_modified: Optional[str]
    fingerprint: Optional[str]
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


def init_db() -> sqlite3.Connection:
    try:
        return _init_db()
    except sqlite3.DatabaseError as e:
        print(f"[warn] Lever state db appears unhealthy; moving it aside and recreating: {e}", flush=True)
        quarantine_db(DB_PATH)
        return _init_db()


def _init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_state (
            company TEXT PRIMARY KEY,
            etag TEXT,
            last_modified TEXT,
            fingerprint TEXT,
            last_seen_ts INTEGER,
            notified_job_ids_json TEXT
        )
        """
    )
    conn.commit()
    return conn


def quarantine_db(db_path: Path) -> None:
    stamp = int(time.time())
    for suffix in ("", "-wal", "-shm", "-journal"):
        path = Path(f"{db_path}{suffix}")
        if path.exists():
            try:
                path.replace(path.with_name(f"{path.name}.corrupt.{stamp}"))
            except OSError as e:
                print(f"[warn] failed to move unhealthy db file {path}: {e}", flush=True)


def load_state(conn: sqlite3.Connection, company: str) -> StoredState:
    row = conn.execute(
        "SELECT etag, last_modified, fingerprint, last_seen_ts, notified_job_ids_json FROM company_state WHERE company = ?",
        (company,),
    ).fetchone()
    if not row:
        return StoredState(None, None, None, None, None)
    return StoredState(row[0], row[1], row[2], row[3], row[4])


def save_state(
    conn: sqlite3.Connection,
    company: str,
    etag: Optional[str],
    last_modified: Optional[str],
    fingerprint: Optional[str],
    last_seen_ts: int,
    notified_job_ids_json: Optional[str],
) -> None:
    try:
        conn.execute(
            """
            INSERT INTO company_state (company, etag, last_modified, fingerprint, last_seen_ts, notified_job_ids_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(company) DO UPDATE SET
                etag=excluded.etag,
                last_modified=excluded.last_modified,
                fingerprint=excluded.fingerprint,
                last_seen_ts=excluded.last_seen_ts,
                notified_job_ids_json=excluded.notified_job_ids_json
            """,
            (company, etag, last_modified, fingerprint, last_seen_ts, notified_job_ids_json),
        )
        conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"[warn] Lever state db write failed for {company}: {e}", flush=True)


# ----------------------------
# Notifications
# ----------------------------
async def post_webhook(session: aiohttp.ClientSession, url: str, text: str) -> None:
    if not url:
        return
    try:
        await session.post(url, json={"content": text}, timeout=TIMEOUT_SECONDS)
    except Exception as e:
        print(f"[warn] webhook post failed: {e}")


async def post_discord_long(session: aiohttp.ClientSession, text: str, max_len: int = 1900) -> None:
    if not DISCORD_WEBHOOK_URL:
        return

    lines = text.split("\n")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > max_len:
            await post_webhook(session, DISCORD_WEBHOOK_URL, chunk)
            chunk = line
        else:
            chunk = f"{chunk}\n{line}" if chunk else line

    if chunk:
        await post_webhook(session, DISCORD_WEBHOOK_URL, chunk)


async def notify(session: aiohttp.ClientSession, message: str) -> None:
    print(message)

    if SLACK_WEBHOOK_URL:
        try:
            await session.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=TIMEOUT_SECONDS)
        except Exception as e:
            print(f"[warn] Slack notify failed: {e}")

    if DISCORD_WEBHOOK_URL:
        await post_discord_long(session, message)


# ----------------------------
# Polling
# ----------------------------
async def fetch_company(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    status_conn: sqlite3.Connection,
    company: str,
) -> Tuple[str, str]:
    api_url = lever_postings_api(company)
    prior = load_state(conn, company)

    try:
        notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
    except Exception:
        notified_ids = set()

    headers = {}
    if prior.etag:
        headers["If-None-Match"] = prior.etag
    if prior.last_modified:
        headers["If-Modified-Since"] = prior.last_modified

    now_ts = int(time.time())

    try:
        async with session.get(api_url, headers=headers, timeout=TIMEOUT_SECONDS) as resp:
            if resp.status == 304:
                save_state(conn, company, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
                record_source_status(status_conn, "lever", company, "unchanged (304)", "not modified")
                return company, "unchanged (304)"

            if resp.status != 200:
                save_state(conn, company, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
                record_source_status(status_conn, "lever", company, f"error HTTP {resp.status}", f"api={api_url}")
                return company, f"error HTTP {resp.status}"

            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")

            postings = await resp.json(content_type=None)
            if not isinstance(postings, list):
                postings = []

            fp = stable_fingerprint(postings)

            by_id = {p.get("id"): p for p in postings if p.get("id")}
            current_ids = set(by_id.keys())
            new_ids = [pid for pid in current_ids if pid not in notified_ids]

            new_matching: List[dict] = []
            for pid in new_ids:
                p = by_id.get(pid)
                if not p:
                    continue
                title = (p.get("text") or "").strip()
                if title_matches(title) and posting_is_us(p):
                    new_matching.append(p)

            if new_matching:
                for p in new_matching:
                    notified_ids.add(p["id"])

                msg = format_new_jobs_message(company, new_matching, limit=15)
                await notify(session, msg)

            save_state(
                conn,
                company,
                etag,
                last_modified,
                fp,
                now_ts,
                json.dumps(sorted(list(notified_ids))),
            )
            record_source_status(
                status_conn,
                "lever",
                company,
                "new match" if new_matching else "ok",
                f"jobs={len(postings)} new_matches={len(new_matching)}",
            )

            return company, "new match" if new_matching else "ok"

    except asyncio.TimeoutError:
        save_state(conn, company, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
        record_source_status(status_conn, "lever", company, "timeout", f"api={api_url}")
        return company, "timeout"
    except Exception as e:
        save_state(conn, company, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
        record_source_status(status_conn, "lever", company, f"exception: {e}", f"api={api_url}")
        return company, f"exception: {e}"


async def run_forever() -> None:
    companies = load_companies_from_file(COMPANIES_FILE)
    if not companies:
        raise RuntimeError(f"No valid companies found in {COMPANIES_FILE}. Add one per line.")

    print(f"Watching {len(companies)} Lever boards (from {COMPANIES_FILE})")

    conn = init_db()
    status_conn = init_status_db()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded_fetch(c: str):
            async with sem:
                return await fetch_company(session, conn, status_conn, c)

        while True:
            start = time.time()
            try:
                tasks = [asyncio.create_task(bounded_fetch(company)) for company in companies]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                counts = {}
                for result in results:
                    if isinstance(result, Exception):
                        status = f"task_exception:{type(result).__name__}"
                        counts[status] = counts.get(status, 0) + 1
                        print(f"[warn] Lever task failed without source result: {result}", flush=True)
                        continue
                    _, status = result
                    counts[status] = counts.get(status, 0) + 1
                summary = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
                elapsed = time.time() - start
                print(f"Cycle done in {elapsed:.1f}s. {summary}", flush=True)
                record_cycle(status_conn, "lever", counts, int(elapsed * 1000))
            except Exception as e:
                elapsed = time.time() - start
                print(f"[error] Lever cycle failed after {elapsed:.1f}s: {e}", flush=True)
                record_cycle(status_conn, "lever", {f"cycle_exception:{type(e).__name__}": 1}, int(elapsed * 1000))

            sleep_for = POLL_INTERVAL_SECONDS + random.randint(0, JITTER_SECONDS)
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("Stopped.")
