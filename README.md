# Job Board Tracker

A job monitoring system that discovers company-hosted job boards and continuously checks them for new postings. It filters for target roles and US locations, stores per-company state locally, and sends notifications to Discord.

## What’s Included

### 1) Company Discovery (Build company lists)
**Script:** `discover_companys.py`  
**Tech:** Python, Requests, SerpAPI (Google Search API)  
**Output files:**
- `ashbyhq_companies.txt`
- `greenhouse_companies.txt`
- `lever_companies.txt`

### 2) Job Board Monitors (Watch boards and notify)
Each monitor:
- Reads a company list file (one company per line)
- Polls the board API in a loop
- Stores state in SQLite to prevent duplicate alerts
- Sends notifications only for newly-seen matching jobs

**Monitors and tech**
- Ashby: Python, asyncio, aiohttp, SQLite, GraphQL
- Greenhouse: Python, asyncio, aiohttp, SQLite, REST
- Lever: Python, asyncio, aiohttp, SQLite, REST
- SmartRecruiters: Python, asyncio, aiohttp, SQLite, REST
- Workable: Python, asyncio, aiohttp, SQLite, REST
- RippleMatch: Python, asyncio, aiohttp, SQLite, HTML scrape of public category pages

## How It Works Today

Each monitor follows the same pattern:

1. Read a source-specific text file from `data/companies/`.
2. Normalize each entry into the board identifier or page URL that provider expects.
3. Poll the provider's public endpoint or page in an async loop.
4. Filter jobs down to SWE-style titles and US locations.
5. Store already-notified job ids in a source-specific SQLite database under `data/watch/`.
6. Send webhook notifications only for newly-seen matching jobs.

## New Source Inputs

- `smartrecruiters.py`: one company identifier or `jobs.smartrecruiters.com/<company>` URL per line
- `workable.py`: one account slug or `apply.workable.com/<account>/` URL per line
- `ripplematch.py`: one RippleMatch category/source page per line, such as `computer-science-majors` or the full `https://ripplematch.com/jobs/computer-science-majors/` URL

## Run Multiple Monitors Together

You can now run Ashby, Greenhouse, and Lever together in either of these ways:

- Local: `python run_all.py`
- Docker Compose, separate services: `docker compose up ashby greenhouse lever dashboard`
- Docker Compose, single monitor container: `docker compose up monitors dashboard`

The live monitoring dashboard is available at `http://localhost:8080`.

## Live Error Monitoring

The monitors now write source-level status data to `data/watch/monitor_status.db`.

The dashboard shows:

- Which service is currently healthy vs failing
- Which specific source slug/company returned an error
- The exact last error text for that source
- Recent status/error events across all monitor services
- Latest check results so you can watch changes live while the containers are running

---

## Requirements

- Python 3.10+ recommended
- Packages:
  - `python-dotenv`
  - `requests` (for discovery)
  - `aiohttp` (for monitors)

Install dependencies:
```bash
pip install -r requirements.txt
