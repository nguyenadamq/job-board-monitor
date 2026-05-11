import asyncio
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from job_classifier import classify_job_if_needed
from job_store import init_job_db, upsert_job


SAMPLE_JOB = {
    "provider": "greenhouse",
    "company": "example-ai",
    "external_job_id": "smoke-123",
    "title": "Software Engineer, New Grad - Backend Platform",
    "location": "Remote - United States",
    "url": "https://boards.greenhouse.io/example/jobs/smoke-123",
    "description": (
        "Build Python APIs and distributed backend services for an AI product. "
        "This role is intended for new graduates and early-career engineers."
    ),
    "raw": {"id": "smoke-123"},
}


async def main() -> None:
    conn = init_job_db(":memory:")
    try:
        saved = upsert_job(conn, SAMPLE_JOB)
        first = await classify_job_if_needed(conn, saved, SAMPLE_JOB)
        second = await classify_job_if_needed(conn, saved, SAMPLE_JOB)
        result = {
            "mode": "local",
            "saved_job": saved.__dict__,
            "first_classification": first,
            "second_classification_cache_hit": bool(second and second.get("cache_hit")),
        }
    finally:
        conn.close()

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
