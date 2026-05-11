# Job Board Tracker

An AI-powered real-time job intelligence pipeline for Ashby, Greenhouse, and Lever career pages. It discovers company-hosted job boards, ingests and deduplicates postings, persists normalized raw job records, enriches new or changed postings with a local classifier, and sends relevance-ranked Discord/Slack alerts.

The classifier is an enrichment layer, not the ingestion layer. Raw postings are saved first, then classification runs best-effort with local inference and SQLite caching so provider polling is not blocked by external model services.

## What It Runs

- `ashby.py` monitors `jobs.ashbyhq.com` boards through Ashby's public GraphQL endpoint.
- `greenhouse.py` monitors `boards.greenhouse.io` boards through Greenhouse's public API.
- `lever.py` monitors `jobs.lever.co` boards through Lever's public API.
- `discover_companys.py` uses SerpAPI search results for automated Ashby, Greenhouse, and Lever board discovery.
- `job_store.py` persists normalized jobs and local classifications in SQLite.
- `job_classifier.py` classifies new or changed postings by role family, role type, seniority, location fit, and relevance score.
- `dashboard.py` serves a live monitor and intelligence dashboard at `http://localhost:8080`.
- `metrics_exporter.py` exposes monitor health as Prometheus metrics at `http://localhost:9108/metrics`.
- `status_monitor.py` stores source-level health and cycle history in SQLite.

## Pipeline

1. Discover company ATS boards with SerpAPI.
2. Poll provider APIs asynchronously with concurrency limits, jitter, retries, rate limiting, and exponential backoff.
3. Normalize provider-specific postings into one shared job shape.
4. Save raw normalized postings to SQLite with a stable content hash.
5. Deduplicate by provider and external job ID.
6. Classify new or changed postings with the local classifier when enabled.
7. Cache classifications by `job_id + content_hash + model`.
8. Alert only on high-relevance roles when classification is enabled, or use the existing deterministic title/location filters when it is disabled.
9. Record monitor health, cycle summaries, classifier outputs, and dashboard metrics.

## Local Classification

The classifier scores postings for a US-based early-career software engineering job seeker. It runs locally with keyword-trained scoring for role family, role type, seniority, location fit, and relevance. It prefers new grad, junior, intern, backend, full-stack, platform, infrastructure, data engineering, and AI/ML engineering roles. It penalizes senior, staff, manager, sales, support, non-technical, and non-US roles.

Structured output:

```json
{
  "role_family": "software_engineering",
  "role_type": "backend",
  "seniority": "new_grad",
  "location_fit": "us_remote",
  "relevance_score": 95,
  "confidence": 0.9,
  "reason": "Backend platform role with early-career language and US remote location."
}
```

The monitors save the raw job before attempting classification. If classification fails, the job remains stored and can be retried later because the classification cache is keyed by content hash.

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

Start the Prometheus metrics exporter separately:

```bash
python metrics_exporter.py
```

Run the classifier smoke test:

```bash
python scripts/classifier_smoke_test.py
```

The smoke test runs entirely locally and confirms that classification caching works.

## Run With Docker Compose

Run the three monitors and dashboard as separate services:

```bash
docker compose up ashby greenhouse lever dashboard
```

Run the full monitoring stack with Prometheus and Grafana:

```bash
docker compose up ashby greenhouse lever dashboard metrics prometheus grafana
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

Core local classifier settings:

```env
LOCAL_CLASSIFICATION_ENABLED=true
LOCAL_CLASSIFIER_MODEL=local-keyword-v1
LOCAL_CLASSIFIER_MAX_DESCRIPTION_CHARS=1500
MIN_RELEVANCE_SCORE=75
JOB_LAST_SEEN_UPDATE_INTERVAL_SECONDS=21600
```

Set `LOCAL_CLASSIFICATION_ENABLED=false` to return to the older deterministic title/location alert behavior.

## SQLite State

This project uses SQLite so the tracker stays easy to run on one machine without standing up extra services.

- Provider watch databases track per-company polling state and notified job IDs.
- `monitor_status.db` tracks source health, recent events, and cycle history.
- `job_intelligence.db` stores normalized jobs and classification results.

If files under `data/watch/` are deleted, the monitors can rebuild state from live job boards, though alerts may be resent.

## Dashboard

The dashboard shows:

- health status by provider
- source-level errors
- recent status events
- cycle duration and summary counts
- error trend history
- total normalized jobs stored
- total classified jobs
- high-relevance jobs
- role type and seniority distributions
- latest high-relevance classified jobs

## Grafana

Grafana is available at `http://localhost:3000` when the `grafana` service is running. The default local credentials come from `.env.local`:

```env
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin
```

Prometheus is available at `http://localhost:9090`, and the raw exporter is available at `http://localhost:9108/metrics`.

The provisioned `Job Tracker Error Rates` dashboard includes:

- active source errors
- overall source error ratio
- error ratio by provider
- active errors by provider
- top consecutive source failures
- latest cycle duration by provider

Useful PromQL queries:

```promql
job_tracker_source_error_ratio
sum(job_tracker_source_errors) / clamp_min(sum(job_tracker_sources_total), 1)
topk(25, job_tracker_source_consecutive_errors)
job_tracker_service_cycle_duration_seconds
```

## Logs

The app writes structured JSON logs to stdout. Docker captures those logs, so there are no app-managed log files to clean up inside the repo.

Normal runs use `LOG_LEVEL=INFO`, which keeps logs focused on monitor startup, cycle summaries, new matches, classifier events, and failures. Set `LOG_LEVEL=DEBUG` when you want per-company check details.

Docker Compose enables basic log rotation for each service:

- max log file size: `10m`
- retained files per service: `3`

## Resume Bullets

```latex
\resumeItem{Built an \textbf{AI-powered job intelligence pipeline} that discovers, ingests, deduplicates, classifies, and monitors listings across \textbf{1,000+} Ashby, Greenhouse, and Lever job boards with relevance-ranked alerts.}
\resumeItem{Added a cached local \textbf{AI enrichment layer} that classifies postings by role type, seniority, location fit, and relevance score without external LLM API costs.}
\resumeItem{Engineered high-concurrency \textbf{Python AsyncIO} workers with rate limiting, exponential backoff, retry handling, SQLite checkpointing, structured logging, Dockerized deployment, and a live health dashboard.}
```
