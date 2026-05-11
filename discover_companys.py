import os
import time
import re
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv
from structured_logging import configure_logger, log_event

load_dotenv(".env.local")
LOGGER = configure_logger("discover")

SEARCH_URL = "https://serpapi.com/search.json"

# Put your keys in .env.local like:
# SERPAPI_KEYS=key1,key2,key3,key4
SERPAPI_KEYS = [k.strip() for k in os.getenv("SERPAPI_KEYS", "").split(",") if k.strip()]
if not SERPAPI_KEYS:
    log_event(LOGGER, "configuration_error", level="ERROR", missing_env_var="SERPAPI_KEYS")
    raise SystemExit(1)

ATS = {
    "ashbyhq": {
        "queries": [
            # 1) Job detail pages (lots of coverage, high unique slugs)
            'site:jobs.ashbyhq.com inurl:/jobs/ ("software" OR engineer OR engineering OR developer OR "data" OR security OR "product") -inurl:/application',

            # 2) Board root and section pages (great for direct slug discovery)
            'site:jobs.ashbyhq.com "jobs.ashbyhq.com/" (careers OR jobs OR openings OR "open roles" OR "open positions" OR "join our team") -inurl:/application',

            # 3) Non-English / international boards (big expansion)
            'site:jobs.ashbyhq.com ("empleo" OR "trabaja con" OR "karriere" OR stellen OR vacatures OR emploi OR "offres d\'emploi") -inurl:/application',

            # 4) Broad catch-all, still excludes obvious noise
            'site:jobs.ashbyhq.com -inurl:/application -inurl:/job -inurl:/embed -inurl:/api',
        ],
        "host": "jobs.ashbyhq.com",
        "base_url": "https://jobs.ashbyhq.com/{slug}",
    },
    "lever": {
        "queries": [
            # 1) Apply pages with hiring keywords (high yield)
            'site:jobs.lever.co inurl:/apply (engineer OR engineering OR developer OR "software" OR "data" OR security OR product)',

            # 2) Company root pages (slug pages)
            'site:jobs.lever.co -inurl:/apply (careers OR jobs OR openings OR "join our team" OR "we\'re hiring")',

            # 3) ATS fingerprint text
            'site:jobs.lever.co ("Powered by Lever" OR "lever.co" "apply")',
        ],
        "host": "jobs.lever.co",
        "base_url": "https://jobs.lever.co/{slug}",
    },
    "greenhouse": {
        "queries": [
            # 1) Job pages with hiring keywords (high yield)
            'site:boards.greenhouse.io inurl:/jobs/ (engineer OR engineering OR developer OR "software" OR "data" OR security OR product)',

            # 2) Board root/company pages (best direct slug discovery)
            'site:boards.greenhouse.io (careers OR jobs OR openings OR "join our team") -inurl:/jobs/',

            # 3) Fingerprint
            'site:boards.greenhouse.io ("Powered by Greenhouse" OR greenhouse.io) -inurl:/embed',
        ],
        "host": "boards.greenhouse.io",
        "base_url": "https://boards.greenhouse.io/{slug}",
    },
}

def extract_slug(link: str, expected_host: str) -> str | None:
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

class SerpApiClient:
    def __init__(self, keys: list[str]):
        self.keys = keys
        self.idx = 0

    def _rotate_key(self):
        self.idx += 1
        if self.idx >= len(self.keys):
            log_event(LOGGER, "serpapi_keys_exhausted", level="ERROR")
            raise SystemExit(1)
        log_event(LOGGER, "serpapi_key_rotated", key_index=self.idx + 1)

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

            # SerpApi often returns { "error": "..."} when out of credits
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
    with open(path, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

def load_existing(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {ln.strip() for ln in f if ln.strip()}

def harvest_platform(client: SerpApiClient, name: str, cfg: dict, out_file: str) -> set[str]:
    # Keep a set in memory, but also resume from existing file
    discovered_urls = load_existing(out_file)

    for q in cfg["queries"]:
        no_new_pages = 0
        start = 0

        while start <= 900 and no_new_pages < 5:
            log_event(
                LOGGER,
                "search_page_started",
                level="DEBUG",
                platform=name,
                query=q,
                start=start,
                discovered_total=len(discovered_urls),
            )

            data = client.google(q, start=start)
            results = data.get("organic_results", [])
            if not results:
                log_event(LOGGER, "search_query_exhausted", platform=name, query=q, start=start)
                break

            new_urls = []
            for item in results:
                link = item.get("link") or ""
                slug = extract_slug(link, cfg["host"])
                if not slug:
                    continue
                base = cfg["base_url"].format(slug=slug)
                if base not in discovered_urls:
                    discovered_urls.add(base)
                    new_urls.append(base)

            append_new_lines(out_file, new_urls)

            if not new_urls:
                no_new_pages += 1
            else:
                no_new_pages = 0

            log_event(
                LOGGER,
                "search_page_completed",
                level="INFO" if new_urls else "DEBUG",
                platform=name,
                query=q,
                start=start,
                new_urls=len(new_urls),
                discovered_total=len(discovered_urls),
            )

            start += 10
            time.sleep(1.0)

    return discovered_urls

def main():
    os.makedirs("data/companies", exist_ok=True)
    client = SerpApiClient(SERPAPI_KEYS)

    for platform, cfg in ATS.items():
        out = f"data/companies/{platform}_companies.txt"
        urls = harvest_platform(client, platform, cfg, out)
        log_event(LOGGER, "platform_completed", platform=platform, discovered_total=len(urls), output_file=out)

if __name__ == "__main__":
    main()
