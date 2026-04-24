#!/usr/bin/env python3
import asyncio
import json
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, quote

import aiohttp
from dotenv import load_dotenv

load_dotenv(".env.local")

# ----------------------------
# Configuration (env vars)
# ----------------------------
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
JITTER_SECONDS = int(os.getenv("JITTER_SECONDS", "15"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "25"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_WORKDAY", "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"
COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = Path(os.getenv("WORKDAY_COMPANIES_FILE", str(COMPANIES_DIR / "workday_companies.txt")))
DB_PATH = WATCH_DIR / os.getenv("WORKDAY_DB", "workday_watch.db")

# ----------------------------
# Title matching
# ----------------------------
ROLE_PATTERNS = [
    r"\bsoftware\s+engineer\b",
    r"\bsoftware\s+developer\b",
    r"\bbackend\b.*\b(engineer|developer)\b",
    r"\bfrontend\b.*\b(engineer|developer)\b",
    r"\bfront\s*end\b.*\b(engineer|developer)\b",
    r"\bfull\s*stack\b.*\b(engineer|developer)\b",
    r"\bfullstack\b.*\b(engineer|developer)\b",
    r"\bplatform\b.*\b(engineer|developer)\b",
]
ROLE_RE = re.compile("|".join(ROLE_PATTERNS), re.IGNORECASE)

EXCLUDE_TITLE_RE = re.compile(r"\b(senior|advanced|staff|manager|lead|principal|sr)\b", re.IGNORECASE)

def normalize_title(title: str) -> str:
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def title_matches(title: str) -> bool:
    t = normalize_title(title)
    if not t:
        return False
    if EXCLUDE_TITLE_RE.search(t):
        return False
    return bool(ROLE_RE.search(t))

# ----------------------------
# US location matching (Workday)
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

def extract_location_texts(job: Dict[str, Any]) -> List[str]:
    texts: List[str] = []

    for k in ("locationsText", "location"):
        v = job.get(k)
        if v:
            texts.append(str(v))

    for k in ("locations", "jobLocations", "primaryLocation"):
        v = job.get(k)
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    for kk in ("displayName", "name", "location", "value"):
                        if item.get(kk):
                            texts.append(str(item.get(kk)))
                elif item:
                    texts.append(str(item))
        elif isinstance(v, dict):
            for kk in ("displayName", "name", "location", "value"):
                if v.get(kk):
                    texts.append(str(v.get(kk)))
        elif v:
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

def job_is_us(job: Dict[str, Any]) -> bool:
    return any(is_us_location_text(t) for t in extract_location_texts(job))

# ----------------------------
# Server-side narrowing for Workday
# ----------------------------
SEARCH_TEXT_QUERIES = [
    "software engineer",
    "software developer",
    "backend engineer",
    "frontend engineer",
    "front end engineer",
    "full stack engineer",
    "platform engineer",
]

def job_dedupe_key(job: Dict[str, Any]) -> str:
    return (
        str(job.get("externalPath") or "")
        or str(job.get("jobPath") or "")
        or str(job.get("path") or "")
        or str(job.get("jobPostingId") or "")
        or str(job.get("id") or "")
        or json.dumps(job, sort_keys=True)
    )

def extract_title(job: Dict[str, Any]) -> str:
    return (job.get("title") or job.get("jobTitle") or "").strip()

def extract_location(job: Dict[str, Any]) -> str:
    return (job.get("locationsText") or job.get("location") or "").strip()

# ----------------------------
# Workday tenant parsing
# ----------------------------
LOCALE_RE = re.compile(r"^[a-z]{2}(?:-[a-z]{2})?$", re.IGNORECASE)

@dataclass(frozen=True)
class WorkdayTenant:
    name: str
    api_base: str
    board_url: str
    api_url: str
    company_slug: str
    site_slug: str
    locale_segment: str

def _guess_company_from_host(host: str) -> str:
    return host.split(".")[0].strip()

def _split_site_and_locale(parts: List[str]) -> Tuple[str, str]:
    if not parts:
        return "ExternalCareerSite", ""
    if LOCALE_RE.match(parts[0]) and len(parts) >= 2:
        return parts[1], parts[0]
    site = parts[0]
    locale = parts[1] if len(parts) >= 2 and LOCALE_RE.match(parts[1]) else ""
    return site, locale

def parse_workday_line(line: str) -> WorkdayTenant:
    raw = (line or "").strip()
    if not raw or raw.startswith("#"):
        raise ValueError("empty/comment")

    if raw.startswith("http://") or raw.startswith("https://"):
        u = urlparse(raw)
        if not u.netloc:
            raise ValueError(f"Bad URL: {raw}")

        api_base = f"{u.scheme}://{u.netloc}"
        parts = [p for p in (u.path or "").split("/") if p]
        site, locale = _split_site_and_locale(parts)
        company = _guess_company_from_host(u.netloc)

        if locale and parts and LOCALE_RE.match(parts[0]):
            board_url = f"{api_base}/{locale}/{site}"
        else:
            board_url = f"{api_base}/{site}" + (f"/{locale}" if locale else "")

        api_url = f"{api_base}/wday/cxs/{company}/{site}/jobs"
        return WorkdayTenant(raw, api_base, board_url, api_url, company, site, locale)

    tokens = [t for t in re.split(r"[\s,]+", raw) if t]
    host = tokens[0]
    if "/" in host:
        return parse_workday_line("https://" + host)

    site = tokens[1] if len(tokens) >= 2 else "ExternalCareerSite"
    locale = tokens[2] if len(tokens) >= 3 else ""

    api_base = "https://" + host
    company = _guess_company_from_host(host)
    board_url = f"{api_base}/{locale}/{site}" if locale else f"{api_base}/{site}"
    api_url = f"{api_base}/wday/cxs/{company}/{site}/jobs"
    return WorkdayTenant(raw, api_base, board_url, api_url, company, site, locale)

def load_tenants_from_file(path: Path) -> List[WorkdayTenant]:
    if not path.exists():
        raise FileNotFoundError(f"Companies file not found: {path}")

    tenants: List[WorkdayTenant] = []
    for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = s.split("#", 1)[0].strip()
        if not s:
            continue
        try:
            tenants.append(parse_workday_line(s))
        except Exception as e:
            print(f"[warn] Skipping invalid line {i}: {s!r} ({e})")

    seen = set()
    out: List[WorkdayTenant] = []
    for t in tenants:
        key = (t.api_url, t.board_url)
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out

# ----------------------------
# SQLite state
# ----------------------------
@dataclass
class StoredState:
    tenant_key: str
    last_seen_ts: Optional[int]
    notified_job_keys_json: Optional[str]

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tenant_state (
            tenant_key TEXT PRIMARY KEY,
            last_seen_ts INTEGER,
            notified_job_keys_json TEXT
        )
        """
    )
    conn.commit()
    return conn

def load_state(conn: sqlite3.Connection, tenant_key: str) -> StoredState:
    row = conn.execute(
        "SELECT tenant_key, last_seen_ts, notified_job_keys_json FROM tenant_state WHERE tenant_key = ?",
        (tenant_key,),
    ).fetchone()
    if not row:
        return StoredState(tenant_key, None, None)
    return StoredState(row[0], row[1], row[2])

def save_state(conn: sqlite3.Connection, tenant_key: str, last_seen_ts: int, notified_json: str) -> None:
    conn.execute(
        """
        INSERT INTO tenant_state (tenant_key, last_seen_ts, notified_job_keys_json)
        VALUES (?, ?, ?)
        ON CONFLICT(tenant_key) DO UPDATE SET
            last_seen_ts=excluded.last_seen_ts,
            notified_job_keys_json=excluded.notified_job_keys_json
        """,
        (tenant_key, last_seen_ts, notified_json),
    )
    conn.commit()

# ----------------------------
# Discord posting
# ----------------------------
async def post_webhook(session: aiohttp.ClientSession, url: str, text: str) -> None:
    if not url:
        return
    try:
        await session.post(url, json={"content": text}, timeout=TIMEOUT_SECONDS)
    except Exception as e:
        print(f"[warn] Discord post failed: {e}")

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

# ----------------------------
# Extract requisition/job token for correct Workday details URLs
# ----------------------------

# Collect likely string fields from a job posting (deep-ish scan)
def _strings_in_job(job: Dict[str, Any]) -> List[str]:
    texts: List[str] = []

    def add(v: Any) -> None:
        if v is None:
            return
        if isinstance(v, str):
            s = v.strip()
            if s:
                texts.append(s)
            return
        if isinstance(v, (int, float)):
            texts.append(str(v))
            return
        if isinstance(v, list):
            for it in v:
                add(it)
            return
        if isinstance(v, dict):
            for vv in v.values():
                add(vv)
            return

    # Try common Workday keys first
    for k in (
        "externalPath", "jobPath", "path",
        "jobPostingId", "jobRequisitionId", "requisitionId", "reqId",
        "id", "bulletFields", "additionalFields", "postedOn",
        "locationsText", "location",
    ):
        add(job.get(k))

    # Also scan the whole object shallowly
    add(job)
    return texts

# ----------------------------
# Extract requisition/job token for correct Workday details URLs
# ----------------------------

# Try to extract a requisition token, preferring JR_####-# then JR#### then ####-#
def _extract_req_token(job: Dict[str, Any]) -> Optional[str]:
    blob = json.dumps(job, ensure_ascii=False)

    # 1) JR_044991-1
    m = re.search(r"\bJR_(\d+(?:-\d+)?)\b", blob, re.IGNORECASE)
    if m:
        return f"JR_{m.group(1)}"

    # 2) JR2506076 or JR2506076-1
    m = re.search(r"\bJR(\d+(?:-\d+)?)\b", blob, re.IGNORECASE)
    if m:
        return f"JR{m.group(1)}"

    # 3) Numeric token like 2021467 or 1198888-1
    m = re.search(r"\b(\d{4,}(?:-\d+)?)\b", blob)
    if m:
        return m.group(1)

    return None


# ----------------------------
# Correct Workday job URL builder (prefer API-provided /details/ paths)
# ----------------------------
def workday_details_url(tenant: WorkdayTenant, job: Dict[str, Any]) -> str:
    """
    URL preference:
      1) Use API-provided /details/ URL if present
      2) Else use API-provided /job/ URL if present (direct posting page)
      3) Else reconstruct /details/ from title + token
      4) Else fallback to board search
    """
    # 1) Prefer any API-provided details or job path
    for key in ("externalPath", "jobPath", "path"):
        p = job.get(key)
        if not isinstance(p, str) or not p:
            continue

        # absolute URL
        if p.startswith("http"):
            if "/details/" in p or "/job/" in p:
                return p

        # relative URL
        if p.startswith("/"):
            if "/details/" in p or "/job/" in p:
                return tenant.api_base + p

        # odd relative (no leading slash)
        if "/details/" in p or "/job/" in p:
            return tenant.api_base + ("/" + p.lstrip("/"))

    # 2) Reconstruct a /details/ link when no posting path is provided
    title = extract_title(job) or "job"
    slug_title = re.sub(r"\s+", "-", title.strip())
    slug_title = re.sub(r"[^\w\-]", "-", slug_title)
    slug_title = re.sub(r"-{2,}", "-", slug_title).strip("-") or "job"

    token = _extract_req_token(job)
    base = tenant.board_url.rstrip("/")
    q = quote(title, safe="")  # %20 not +

    if token:
        url = f"{base}/details/{slug_title}_{token}?q={q}"

        # ResMed-style fallback: JR_##### might need "-1"
        if re.fullmatch(r"JR_\d{4,}", token, re.IGNORECASE):
            return f"{base}/details/{slug_title}_{token}-1?q={q}"

        return url

    # 3) Final fallback: board search
    return f"{base}?q={q}"


def format_new_jobs_message(tenant: WorkdayTenant, new_jobs: List[Dict[str, Any]], limit: int = 15) -> str:
    lines = [f"Board: {tenant.board_url}"]
    for j in new_jobs[:limit]:
        title = extract_title(j)
        loc = extract_location(j)
        url = workday_details_url(tenant, j)
        if loc:
            lines.append(f"{title} ({loc}) | {url}")
        else:
            lines.append(f"{title} | {url}")
    return "\n".join(lines)

# ----------------------------
# Workday fetch (aiohttp)
# ----------------------------
def build_headers(tenant: WorkdayTenant) -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": tenant.api_base,
        "Referer": tenant.board_url,
        "X-Requested-With": "XMLHttpRequest",
    }

async def warmup(session: aiohttp.ClientSession, tenant: WorkdayTenant) -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    async with session.get(tenant.board_url, headers=headers, timeout=TIMEOUT_SECONDS) as resp:
        await resp.text()

async def post_jobs(
    session: aiohttp.ClientSession,
    tenant: WorkdayTenant,
    payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    headers = build_headers(tenant)
    async with session.post(tenant.api_url, headers=headers, json=payload, timeout=TIMEOUT_SECONDS) as resp:
        if resp.status != 200:
            return None
        try:
            return await resp.json(content_type=None)
        except Exception:
            return None

async def fetch_all_jobs(
    session: aiohttp.ClientSession,
    tenant: WorkdayTenant,
    base_payload: Dict[str, Any],
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    offset = int(base_payload.get("offset", 0))
    limit = int(base_payload.get("limit", 20))

    for _ in range(max_pages):
        payload = dict(base_payload)
        payload["offset"] = offset
        data = await post_jobs(session, tenant, payload)
        if not data or not isinstance(data.get("jobPostings"), list):
            break

        page = data.get("jobPostings", [])
        if not page:
            break

        for j in page:
            if isinstance(j, dict):
                jobs.append(j)

        if len(page) < limit:
            break
        offset += limit

    return jobs

async def fetch_jobs_narrow_then_fallback(
    session: aiohttp.ClientSession,
    tenant: WorkdayTenant,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    narrowed: List[Dict[str, Any]] = []

    for q in SEARCH_TEXT_QUERIES:
        payload = {"limit": 20, "offset": 0, "searchText": q}
        page_jobs = await fetch_all_jobs(session, tenant, payload, max_pages=max_pages)
        for j in page_jobs:
            k = job_dedupe_key(j)
            if k in seen:
                continue
            seen.add(k)
            narrowed.append(j)

    if not narrowed:
        payload = {"limit": 20, "offset": 0}
        return await fetch_all_jobs(session, tenant, payload, max_pages=max_pages)

    return narrowed

# ----------------------------
# Polling loop
# ----------------------------
async def fetch_tenant_once(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    tenant: WorkdayTenant,
) -> Tuple[str, str]:
    tenant_key = tenant.api_url
    prior = load_state(conn, tenant_key)
    now_ts = int(time.time())

    try:
        notified_keys = set(json.loads(prior.notified_job_keys_json)) if prior.notified_job_keys_json else set()
    except Exception:
        notified_keys = set()

    try:
        await warmup(session, tenant)
        jobs = await fetch_jobs_narrow_then_fallback(session, tenant, max_pages=50)

        matched: List[Dict[str, Any]] = []
        for j in jobs:
            title = extract_title(j)
            if not title_matches(title):
                continue
            if not job_is_us(j):
                continue
            matched.append(j)

        new_matched: List[Dict[str, Any]] = []
        for j in matched:
            k = job_dedupe_key(j)
            if k in notified_keys:
                continue
            notified_keys.add(k)
            new_matched.append(j)

        if new_matched:
            msg = format_new_jobs_message(tenant, new_matched, limit=15)
            await post_discord_long(session, msg)

        save_state(conn, tenant_key, now_ts, json.dumps(sorted(list(notified_keys))))
        return tenant.company_slug, ("new match" if new_matched else "ok")

    except asyncio.TimeoutError:
        save_state(conn, tenant_key, now_ts, prior.notified_job_keys_json or "[]")
        return tenant.company_slug, "timeout"
    except Exception as e:
        save_state(conn, tenant_key, now_ts, prior.notified_job_keys_json or "[]")
        return tenant.company_slug, f"exception: {e}"

async def run_forever() -> None:
    if not DISCORD_WEBHOOK_URL:
        print("[warn] DISCORD_WEBHOOK_WORKDAY is not set. No notifications will be sent.")

    tenants = load_tenants_from_file(COMPANIES_FILE)
    if not tenants:
        raise RuntimeError(f"No valid Workday tenants found in {COMPANIES_FILE}")

    print(f"Watching {len(tenants)} Workday boards (from {COMPANIES_FILE})")
    print("Filtering: SWE titles, excluding senior/advanced/staff/manager/lead/principal/sr, and US-only locations")

    conn = init_db()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded(t: WorkdayTenant):
            async with sem:
                return await fetch_tenant_once(session, conn, t)

        while True:
            start = time.time()
            tasks = [asyncio.create_task(bounded(t)) for t in tenants]
            results = await asyncio.gather(*tasks)

            counts: Dict[str, int] = {}
            for _, status in results:
                counts[status] = counts.get(status, 0) + 1
            summary = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
            print(f"Cycle done in {time.time() - start:.1f}s. {summary}")

            sleep_for = POLL_INTERVAL_SECONDS + random.randint(0, JITTER_SECONDS)
            await asyncio.sleep(sleep_for)

if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("Stopped.")
