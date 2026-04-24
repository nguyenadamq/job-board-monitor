import os
import time
import re
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv(".env.local")

SEARCH_URL = "https://serpapi.com/search.json"

# Output file for Workday boards (define here)
WORKDAY_OUT_FILE = "data/companies/workday_companies.txt"

# Put your keys in .env.local like:
# SERPAPI_KEYS=key1,key2,key3,key4
SERPAPI_KEYS = [k.strip() for k in os.getenv("SERPAPI_KEYS", "").split(",") if k.strip()]
if not SERPAPI_KEYS:
    raise SystemExit("Missing SERPAPI_KEYS env var (comma-separated list)")

# Engineering-related keywords (used to filter results and craft queries)
ENGINEERING_KEYWORDS = [
    "software engineer",
    "software engineering",
    "engineer",
    "backend",
    "frontend",
    "full stack",
    "fullstack",
    "developer",
    "platform",
    "site reliability",
    "sre",
    "devops",
    "data engineer",
    "machine learning",
    "ml engineer",
    "security engineer",
]

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def text_matches_engineering(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in ENGINEERING_KEYWORDS)

def extract_slug_exact_host(link: str, expected_host: str) -> str | None:
    try:
        u = urlparse(link)
    except Exception:
        return None

    if u.netloc.lower() != expected_host.lower():
        return None

    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return None

    slug = parts[0].strip()

    # Basic cleanup: remove obvious non-slugs
    if slug.lower() in {"jobs", "job", "search", "api", "assets"}:
        return None

    slug = re.sub(r"[?#].*$", "", slug).strip()
    return slug if slug else None

_LOCALE_RE = re.compile(r"^[a-z]{2}-[A-Z]{2}$")  # en-US, fr-FR, etc.

def extract_workday_board_base(link: str) -> str | None:
    """
    Canonicalize a Workday board base from a result link.

    Typical Workday URLs look like:
      https://company.wd5.myworkdayjobs.com/en-US/Careers/job/...
      https://company.wd5.myworkdayjobs.com/en-US/External/job/...
      https://company.wd5.myworkdayjobs.com/Careers/job/...

    We treat the board as:
      https://{netloc}/{locale}/{site}
    when locale is present, otherwise:
      https://{netloc}/{site}
    """
    try:
        u = urlparse(link)
    except Exception:
        return None

    host = (u.netloc or "").lower()
    if not host.endswith(".myworkdayjobs.com"):
        return None

    parts = [p for p in (u.path or "").split("/") if p]
    if not parts:
        return None

    # Strip obvious file-ish paths
    if parts and parts[0].lower() in {"wday", "cxs"}:
        # Some endpoints are /wday/cxs/... not the human board.
        # We skip these for discovery because they are not stable "board home" URLs.
        return None

    locale = None
    site = None

    # Workday frequently starts with a locale segment (en-US, etc)
    if parts and _LOCALE_RE.match(parts[0]):
        locale = parts[0]
        if len(parts) >= 2:
            site = parts[1]
    else:
        site = parts[0]

    if not site:
        return None

    # Remove common non-site segments
    bad_sites = {"job", "jobs", "search", "career", "careers", "external", "internal"}
    if site.lower() in bad_sites:
        # In rare cases locale is missing and first segment is a generic word
        return None

    if locale:
        return f"https://{u.netloc}/{locale}/{site}"
    return f"https://{u.netloc}/{site}"

class SerpApiClient:
    def __init__(self, keys: list[str]):
        self.keys = keys
        self.idx = 0

    def _rotate_key(self):
        self.idx += 1
        if self.idx >= len(self.keys):
            raise SystemExit("All SerpApi keys exhausted")
        print(f"Switching to key #{self.idx + 1}", flush=True)

    def google(self, query: str, start: int) -> dict:
        while True:
            api_key = self.keys[self.idx]
            params = {
                "engine": "google",
                "q": query,
                "api_key": api_key,
                "num": 10,
                "start": start,
            }

            r = requests.get(SEARCH_URL, params=params, timeout=30)

            # Parse JSON even on errors so we can see quota messages
            try:
                data = r.json()
            except Exception:
                r.raise_for_status()
                return {}

            err = (data.get("error") or "").lower()

            # Rotate on auth/quota/credits signals
            if r.status_code in (401, 403) or any(w in err for w in ["credit", "quota", "limit", "exceeded"]):
                self._rotate_key()
                continue

            r.raise_for_status()
            return data

def append_new_lines(path: str, lines: list[str]) -> None:
    if not lines:
        return
    ensure_parent_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

def load_existing(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {ln.strip() for ln in f if ln.strip()}

def harvest_workday_boards(client: SerpApiClient, out_file: str) -> set[str]:
    """
    Discover Workday boards that likely have engineering roles.
    Saves canonical board base URLs to out_file.
    """
    discovered_urls = load_existing(out_file)

    # Queries designed to:
    # - focus on myworkdayjobs.com hosts
    # - bias toward job detail pages (to capture tenant + locale + site)
    # - include engineering keywords
    queries = [
    # ============================================================
    # 1. Core SWE / Backend / Full Stack roles (highest precision)
    # ============================================================
    '(site:myworkdayjobs.com OR site:wd1.myworkdayjobs.com OR site:wd2.myworkdayjobs.com OR '
    'site:wd3.myworkdayjobs.com OR site:wd4.myworkdayjobs.com OR site:wd5.myworkdayjobs.com OR '
    'site:wd6.myworkdayjobs.com OR site:wd7.myworkdayjobs.com OR site:wd8.myworkdayjobs.com OR '
    'site:wd9.myworkdayjobs.com OR site:wd10.myworkdayjobs.com OR site:wd11.myworkdayjobs.com OR '
    'site:wd12.myworkdayjobs.com) '
    '(inurl:/job/ OR inurl:/jobs/) '
    '("software engineer" OR "backend engineer" OR "full stack engineer" OR "full-stack engineer" '
    'OR "platform engineer" OR "systems engineer" OR "cloud engineer" OR "infrastructure engineer")',

    # ============================================================
    # 2. Developer + Programming language targeting (big expansion)
    # ============================================================
    '(site:myworkdayjobs.com OR site:wd1.myworkdayjobs.com OR site:wd2.myworkdayjobs.com OR '
    'site:wd3.myworkdayjobs.com OR site:wd4.myworkdayjobs.com OR site:wd5.myworkdayjobs.com OR '
    'site:wd6.myworkdayjobs.com OR site:wd7.myworkdayjobs.com OR site:wd8.myworkdayjobs.com OR '
    'site:wd9.myworkdayjobs.com OR site:wd10.myworkdayjobs.com OR site:wd11.myworkdayjobs.com OR '
    'site:wd12.myworkdayjobs.com) '
    '(inurl:/job/ OR inurl:/jobs/) '
    '(developer OR engineering) '
    '(Python OR Java OR Golang OR Rust OR Kubernetes OR React OR Node OR TypeScript)',

    # ============================================================
    # 3. Entry-level SWE / Intern / New Grad (huge hidden pool)
    # ============================================================
    '(site:myworkdayjobs.com OR site:wd1.myworkdayjobs.com OR site:wd2.myworkdayjobs.com OR '
    'site:wd3.myworkdayjobs.com OR site:wd4.myworkdayjobs.com OR site:wd5.myworkdayjobs.com OR '
    'site:wd6.myworkdayjobs.com OR site:wd7.myworkdayjobs.com OR site:wd8.myworkdayjobs.com OR '
    'site:wd9.myworkdayjobs.com OR site:wd10.myworkdayjobs.com OR site:wd11.myworkdayjobs.com OR '
    'site:wd12.myworkdayjobs.com) '
    '(inurl:/job/ OR inurl:/jobs/) '
    '("new grad" OR "graduate" OR internship OR intern OR "entry level") '
    '(software OR engineer OR developer)',

    # ============================================================
    # 4. DevOps / SRE / Infrastructure roles (often separate boards)
    # ============================================================
    '(site:myworkdayjobs.com OR site:wd1.myworkdayjobs.com OR site:wd2.myworkdayjobs.com OR '
    'site:wd3.myworkdayjobs.com OR site:wd4.myworkdayjobs.com OR site:wd5.myworkdayjobs.com OR '
    'site:wd6.myworkdayjobs.com OR site:wd7.myworkdayjobs.com OR site:wd8.myworkdayjobs.com OR '
    'site:wd9.myworkdayjobs.com OR site:wd10.myworkdayjobs.com OR site:wd11.myworkdayjobs.com OR '
    'site:wd12.myworkdayjobs.com) '
    '(inurl:/job/ OR inurl:/jobs/) '
    '("site reliability engineer" OR SRE OR DevOps OR "platform engineer" '
    'OR "infrastructure engineer" OR "cloud engineer")',

    # ============================================================
    # 5. International boards (you were missing most of these)
    # ============================================================
    '(site:myworkdayjobs.com OR site:wd1.myworkdayjobs.com OR site:wd2.myworkdayjobs.com OR '
    'site:wd3.myworkdayjobs.com OR site:wd4.myworkdayjobs.com OR site:wd5.myworkdayjobs.com OR '
    'site:wd6.myworkdayjobs.com OR site:wd7.myworkdayjobs.com OR site:wd8.myworkdayjobs.com OR '
    'site:wd9.myworkdayjobs.com OR site:wd10.myworkdayjobs.com OR site:wd11.myworkdayjobs.com OR '
    'site:wd12.myworkdayjobs.com) '
    '(inurl:/en-US/ OR inurl:/en-GB/ OR inurl:/de-DE/ OR inurl:/fr-FR/ OR '
    'inurl:/nl-NL/ OR inurl:/es-ES/ OR inurl:/sv-SE/) '
    '(inurl:/job/ OR inurl:/jobs/) '
    '(software OR engineer OR developer)',

    # ============================================================
    # 6. Broad recall query (last resort catch-all)
    # ============================================================
    '(site:myworkdayjobs.com OR site:wd1.myworkdayjobs.com OR site:wd2.myworkdayjobs.com OR '
    'site:wd3.myworkdayjobs.com OR site:wd4.myworkdayjobs.com OR site:wd5.myworkdayjobs.com OR '
    'site:wd6.myworkdayjobs.com OR site:wd7.myworkdayjobs.com OR site:wd8.myworkdayjobs.com OR '
    'site:wd9.myworkdayjobs.com OR site:wd10.myworkdayjobs.com OR site:wd11.myworkdayjobs.com OR '
    'site:wd12.myworkdayjobs.com) '
    '("myworkdayjobs.com" AND (software OR engineer OR developer)) '
    '-inurl:/wday/cxs/ -inurl:/login',
    ]


    for q in queries:
        no_new_pages = 0
        start = 0

        while start <= 900:
            print(f"[workday] q='{q[:90]}...' start={start} total={len(discovered_urls)}", flush=True)

            data = client.google(q, start=start)
            results = data.get("organic_results", [])
            if not results:
                print("[workday] no results, stopping this query", flush=True)
                break

            new_urls = []
            for item in results:
                link = item.get("link") or ""
                title = item.get("title") or ""
                snippet = item.get("snippet") or ""

                # Keep this strict to engineering-ish results
                if not (text_matches_engineering(title) or text_matches_engineering(snippet) or text_matches_engineering(link)):
                    continue

                base = extract_workday_board_base(link)
                if not base:
                    continue

                if base not in discovered_urls:
                    discovered_urls.add(base)
                    new_urls.append(base)

            append_new_lines(out_file, new_urls)

            if not new_urls:
                no_new_pages += 1
            else:
                no_new_pages = 0

            print(f"[workday] +{len(new_urls)} new (total {len(discovered_urls)})", flush=True)

            start += 10
            time.sleep(1.0)

    return discovered_urls

def main():
    client = SerpApiClient(SERPAPI_KEYS)

    urls = harvest_workday_boards(client, WORKDAY_OUT_FILE)
    print(f"workday done: {len(urls)} total -> {WORKDAY_OUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
