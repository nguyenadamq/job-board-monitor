import json
import re
import requests
from typing import Any, Dict, List, Optional, Tuple

API_BASE = "https://hp.wd5.myworkdayjobs.com"
BOARD_URL = "https://hp.wd5.myworkdayjobs.com/ExternalCareerSite"
API_URL = "https://hp.wd5.myworkdayjobs.com/wday/cxs/hp/ExternalCareerSite/jobs"
LOCALE = ""

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

def title_matches(title: str) -> bool:
    return bool(ROLE_RE.search((title or "").strip()))

def job_url(job: Dict[str, Any]) -> str:
    p = job.get("externalPath") or job.get("jobPath") or job.get("path") or ""
    if isinstance(p, str) and p.startswith("http"):
        return p
    if isinstance(p, str) and p.startswith("/"):
        return API_BASE + p
    return BOARD_URL

def warmup(session: requests.Session) -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": LOCALE,
    }
    r = session.get(BOARD_URL, headers=headers, timeout=30, allow_redirects=True)
    _ = r.text

def post_jobs(session: requests.Session, payload: Dict[str, Any]) -> Tuple[int, str, Optional[Dict[str, Any]]]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Accept-Language": LOCALE,
        "Origin": API_BASE,
        "Referer": BOARD_URL,
        "X-Requested-With": "XMLHttpRequest",
    }

    r = session.post(API_URL, headers=headers, json=payload, timeout=30)
    txt = r.text[:400]
    if r.status_code != 200:
        return r.status_code, txt, None

    try:
        data = r.json()
    except Exception:
        return r.status_code, txt, None

    return r.status_code, txt, data

def find_working_payload(session: requests.Session) -> Dict[str, Any]:
    # Try a set of payload shapes that Workday tenants commonly accept.
    candidates = [
        {"limit": 20, "offset": 0},
        {"limit": 20, "offset": 0, "searchText": ""},
        {"limit": 20, "offset": 0, "appliedFacets": {}},
        {"limit": 20, "offset": 0, "searchText": "", "appliedFacets": {}},
        {"limit": 20, "offset": 0, "searchText": "", "appliedFacets": {}, "sortBy": "postedOn"},
        {"limit": 20, "offset": 0, "searchText": "", "appliedFacets": {}, "sortBy": "relevance"},
        {"limit": 20, "offset": 0, "searchText": "", "appliedFacets": {}, "filters": {}},
        {"limit": 20, "offset": 0, "searchText": "", "appliedFacets": {}, "facets": {}},
    ]

    for payload in candidates:
        status, snippet, data = post_jobs(session, payload)
        if status == 200 and isinstance(data, dict) and isinstance(data.get("jobPostings"), list):
            print("Working payload:", json.dumps(payload, separators=(",", ":")))
            return payload
        else:
            print(f"Tried {json.dumps(payload, separators=(',', ':'))} -> {status} {snippet}")

    raise RuntimeError("No candidate payload worked. Copy the exact DevTools payload and use it directly.")

def fetch_all_jobs(session: requests.Session, base_payload: Dict[str, Any], max_pages: int = 5) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    offset = int(base_payload.get("offset", 0))
    limit = int(base_payload.get("limit", 20))

    for _ in range(max_pages):
        payload = dict(base_payload)
        payload["offset"] = offset

        status, snippet, data = post_jobs(session, payload)
        if status != 200 or not data:
            print("Fetch failed:", status, snippet)
            break

        page = data.get("jobPostings", [])
        if not isinstance(page, list) or not page:
            break

        jobs.extend([j for j in page if isinstance(j, dict)])
        if len(page) < limit:
            break
        offset += limit

    return jobs

def main() -> None:
    with requests.Session() as session:
        warmup(session)

        base_payload = find_working_payload(session)
        jobs = fetch_all_jobs(session, base_payload, max_pages=20)

    matches = []
    for j in jobs:
        title = (j.get("title") or j.get("jobTitle") or "").strip()
        if title and title_matches(title):
            matches.append(j)

    print(f"\nFetched {len(jobs)} jobs total")
    print(f"Matched {len(matches)} software-engineering-related roles:\n")

    for j in matches[:50]:
        title = (j.get("title") or j.get("jobTitle") or "").strip()
        loc = j.get("locationsText") or j.get("location") or ""
        url = job_url(j)
        if loc:
            print(f"- {title} ({loc}) | {url}")
        else:
            print(f"- {title} | {url}")

if __name__ == "__main__":
    main()
