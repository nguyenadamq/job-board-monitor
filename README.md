# Job Board Tracker

A self-hosted job-board monitoring system for Ashby, Greenhouse, and Lever career pages. It discovers company-hosted job boards, polls them on a schedule, filters for target US software engineering roles, stores notification state locally, and sends Discord/Slack alerts for newly seen matches.

## What It Runs

- `ashby.py` monitors `jobs.ashbyhq.com` boards through Ashby's public GraphQL endpoint.
- `greenhouse.py` monitors `boards.greenhouse.io` boards through Greenhouse's public API.
- `lever.py` monitors `jobs.lever.co` boards through Lever's public API.
- `dashboard.py` serves a live monitor dashboard at `http://localhost:8080`.
- `status_monitor.py` stores source-level health and cycle history in SQLite.
- `discover_companys.py` uses SerpAPI search results to discover Ashby, Greenhouse, and Lever company boards.

## How It Works

1. Read source-specific company lists from `data/companies/`.
2. Normalize each company entry into the board identifier that provider expects.
3. Poll the provider endpoint asynchronously with concurrency limits and jitter.
4. Filter postings down to SWE-style titles and US locations.
5. Store already-notified job IDs in SQLite under `data/watch/`.
6. Send webhook notifications only for newly seen matching jobs.
7. Record health data for the dashboard after each monitor cycle.

## Required Company Lists

The monitors expect these local files:

- `data/companies/ashbyhq_companies.txt`
- `data/companies/greenhouse_companies.txt`
- `data/companies/lever_companies.txt`

Each file should contain one company board URL or slug per line. Run `python discover_companys.py` to expand or rebuild the lists with SerpAPI.

## Run Locally

```bash
pip install -r requirements.txt
copy .env.example .env.local
python run_all.py
```

Start the dashboard separately:

```bash
python dashboard.py
```

## Run With Docker Compose

Run the three monitors and dashboard as separate services:

```bash
docker compose up ashby greenhouse lever dashboard
```

Or run the three monitors through one process plus the dashboard:

```bash
docker compose up monitors dashboard
```

Run company discovery:

```bash
docker compose run --rm discover
```

## Configuration

Copy `.env.example` to `.env.local`, then fill in the webhooks and SerpAPI keys you want to use. The app writes generated runtime state to `data/watch/`, which is intentionally ignored by git.

## Dashboard

The dashboard shows:

- health status by provider
- source-level errors
- recent status events
- cycle duration and summary counts
- error trend history
