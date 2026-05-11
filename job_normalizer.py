import re
from typing import Any, Dict, Iterable, List


NormalizedJob = Dict[str, Any]


def normalize_ashby_job(company: str, posting: Dict[str, Any]) -> NormalizedJob:
    job_id = str(posting.get("id") or "").strip()
    title = str(posting.get("title") or "").strip()
    location = str(posting.get("locationName") or "").strip()
    description = _join_text(
        [
            posting.get("descriptionPlain"),
            strip_html(posting.get("descriptionHtml") or posting.get("description")),
            posting.get("employmentType"),
        ]
    )
    return {
        "provider": "ashby",
        "company": company,
        "external_job_id": job_id,
        "title": title,
        "location": location,
        "url": f"https://jobs.ashbyhq.com/{company}/{job_id}" if job_id else f"https://jobs.ashbyhq.com/{company}",
        "description": description,
        "raw": posting,
    }


def normalize_greenhouse_job(company: str, job: Dict[str, Any]) -> NormalizedJob:
    job_id = str(job.get("id") or "").strip()
    title = str(job.get("title") or "").strip()
    location = _greenhouse_location(job)
    url = str(job.get("absolute_url") or "").strip()
    if not url and job_id:
        url = f"https://boards.greenhouse.io/{company}/jobs/{job_id}"
    description = _join_text(
        [
            strip_html(job.get("content")),
            strip_html(job.get("description")),
            _department_text(job),
        ]
    )
    return {
        "provider": "greenhouse",
        "company": company,
        "external_job_id": job_id,
        "title": title,
        "location": location,
        "url": url,
        "description": description,
        "raw": job,
    }


def normalize_lever_job(company: str, posting: Dict[str, Any]) -> NormalizedJob:
    job_id = str(posting.get("id") or "").strip()
    title = str(posting.get("text") or "").strip()
    apply_url = str(posting.get("applyUrl") or "").strip()
    url = apply_url or (f"https://jobs.lever.co/{company}/{job_id}" if job_id else f"https://jobs.lever.co/{company}")
    description = _join_text(
        [
            posting.get("descriptionPlain"),
            strip_html(posting.get("description")),
            posting.get("additionalPlain"),
            strip_html(posting.get("additional")),
            _lever_lists_text(posting.get("lists")),
        ]
    )
    return {
        "provider": "lever",
        "company": company,
        "external_job_id": job_id,
        "title": title,
        "location": _lever_location(posting),
        "url": url,
        "description": description,
        "raw": posting,
    }


def strip_html(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return compact_text(text)


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _greenhouse_location(job: Dict[str, Any]) -> str:
    texts: List[str] = []
    loc = job.get("location")
    if isinstance(loc, dict) and loc.get("name"):
        texts.append(str(loc["name"]))
    elif isinstance(loc, str):
        texts.append(loc)

    for key in ("locations", "additional_locations", "offices"):
        items = job.get(key)
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and item.get("name"):
                    texts.append(str(item["name"]))
                elif isinstance(item, str):
                    texts.append(item)

    return _join_text(texts, sep=", ")


def _lever_location(posting: Dict[str, Any]) -> str:
    texts: List[str] = []
    categories = posting.get("categories")
    if isinstance(categories, dict):
        for key in ("location", "commitment", "team", "department"):
            if categories.get(key):
                texts.append(str(categories[key]))

    loc = posting.get("location")
    if isinstance(loc, dict) and loc.get("name"):
        texts.append(str(loc["name"]))
    elif isinstance(loc, str):
        texts.append(loc)

    if posting.get("workplaceType"):
        texts.append(str(posting["workplaceType"]))

    return _join_text(texts, sep=", ")


def _department_text(job: Dict[str, Any]) -> str:
    departments = job.get("departments")
    if not isinstance(departments, list):
        return ""
    return _join_text(
        [item.get("name") for item in departments if isinstance(item, dict) and item.get("name")],
        sep=", ",
    )


def _lever_lists_text(lists: Any) -> str:
    if not isinstance(lists, list):
        return ""
    chunks: List[str] = []
    for item in lists:
        if not isinstance(item, dict):
            continue
        heading = compact_text(item.get("text"))
        content = strip_html(item.get("content"))
        chunks.extend([heading, content])
    return _join_text(chunks)


def _join_text(values: Iterable[Any], sep: str = " ") -> str:
    seen = set()
    out: List[str] = []
    for value in values:
        text = compact_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return sep.join(out)
