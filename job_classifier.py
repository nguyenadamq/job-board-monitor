import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from job_store import (
    Classification,
    NormalizedJob,
    SavedJob,
    get_cached_classification,
    save_classification,
)


ROLE_FAMILIES = {
    "software_engineering",
    "data",
    "ml_ai",
    "security",
    "product",
    "design",
    "sales",
    "support",
    "other",
}
ROLE_TYPES = {
    "backend",
    "frontend",
    "full_stack",
    "infra",
    "platform",
    "data_engineering",
    "ml_ai",
    "security",
    "product_engineering",
    "mobile",
    "devops",
    "qa",
    "other",
}
SENIORITIES = {"intern", "new_grad", "junior", "mid", "senior", "staff", "manager", "unknown"}
LOCATION_FITS = {"us_remote", "us_onsite", "hybrid_us", "non_us", "unclear"}

DEFAULT_MIN_RELEVANCE_SCORE = 75
DEFAULT_LOCAL_MODEL = "local-keyword-v1"

US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
    "district of columbia", "washington dc",
}
US_STATE_ABBR = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il",
    "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt",
    "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri",
    "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
}
NON_US_HINTS = {
    "canada", "united kingdom", "uk", "england", "london", "germany", "berlin", "france",
    "paris", "india", "bengaluru", "bangalore", "hyderabad", "singapore", "australia",
    "sydney", "netherlands", "amsterdam", "ireland", "dublin", "poland", "spain",
}

ROLE_FAMILY_PATTERNS: Dict[str, List[str]] = {
    "software_engineering": [
        "software engineer", "software developer", "backend", "back end", "frontend", "front end",
        "full stack", "fullstack", "platform engineer", "product engineer", "founding engineer",
        "api engineer", "application engineer", "web engineer", "systems engineer",
    ],
    "data": ["data analyst", "analytics", "business intelligence", "bi engineer"],
    "ml_ai": ["machine learning", " ml ", "ai engineer", "artificial intelligence", "llm", "modeling"],
    "security": ["security engineer", "application security", "appsec", "security analyst"],
    "product": ["product manager", "product owner", "program manager"],
    "design": ["designer", "ux", "ui designer", "visual design"],
    "sales": ["sales", "account executive", "business development", "solutions consultant"],
    "support": ["support", "customer success", "technical account manager", "implementation specialist"],
}

ROLE_TYPE_PATTERNS: Dict[str, List[str]] = {
    "backend": ["backend", "back end", "api", "server", "distributed systems", "python", "go ", "java"],
    "frontend": ["frontend", "front end", "react", "typescript", "javascript", "web ui"],
    "full_stack": ["full stack", "fullstack"],
    "infra": ["infrastructure", "infra", "systems", "compute", "storage", "networking"],
    "platform": ["platform", "developer platform", "internal tools"],
    "data_engineering": ["data engineer", "data engineering", "etl", "pipeline", "spark", "warehouse"],
    "ml_ai": ["machine learning", " ml ", "ai engineer", "llm", "model training", "inference"],
    "security": ["security", "appsec", "detection", "vulnerability"],
    "product_engineering": ["product engineer", "founding engineer", "forward deployed engineer"],
    "mobile": ["ios", "android", "mobile"],
    "devops": ["devops", "sre", "site reliability", "kubernetes", "terraform"],
    "qa": ["qa", "quality assurance", "test automation", "sdet"],
}

SENIORITY_PATTERNS: Dict[str, List[str]] = {
    "manager": ["manager", "director", "head of", "vp ", "vice president", "chief", "lead "],
    "staff": ["staff", "principal", "architect"],
    "senior": ["senior", "sr.", "sr "],
    "intern": ["intern", "internship", "co-op", "coop"],
    "new_grad": ["new grad", "university grad", "graduate", "early career", "entry level"],
    "junior": ["junior", "jr.", "associate", "software engineer i", "engineer i", "level 1"],
    "mid": ["software engineer ii", "engineer ii", "level 2", "mid-level", "mid level"],
}


def classification_enabled() -> bool:
    return os.getenv("LOCAL_CLASSIFICATION_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


def classifier_model() -> str:
    return os.getenv("LOCAL_CLASSIFIER_MODEL", DEFAULT_LOCAL_MODEL).strip() or DEFAULT_LOCAL_MODEL


def classifier_ready() -> bool:
    return classification_enabled()


def min_relevance_score() -> int:
    raw = os.getenv("MIN_RELEVANCE_SCORE", str(DEFAULT_MIN_RELEVANCE_SCORE)).strip()
    try:
        return max(0, min(100, int(raw)))
    except ValueError:
        return DEFAULT_MIN_RELEVANCE_SCORE


async def classify_job_if_needed(
    conn: Any,
    saved_job: SavedJob,
    normalized_job: NormalizedJob,
    logger: Any = None,
) -> Optional[Classification]:
    if not classification_enabled():
        return None

    model = classifier_model()
    cached = get_cached_classification(conn, saved_job.id, saved_job.content_hash, model)
    if cached:
        cached["cache_hit"] = True
        return cached

    try:
        result = validate_classification(classify_local_job(normalized_job))
        saved = save_classification(
            conn,
            saved_job.id,
            saved_job.content_hash,
            model,
            result,
            raw_response_json=json.dumps({"engine": model, "source": "local_classifier"}, sort_keys=True),
        )
        saved["cache_hit"] = False
        _log(
            logger,
            "job_classified_local",
            provider=saved_job.provider,
            company=saved_job.company,
            external_job_id=saved_job.external_job_id,
            relevance_score=saved["relevance_score"],
            role_type=saved["role_type"],
            seniority=saved["seniority"],
        )
        return saved
    except Exception as exc:
        _log(
            logger,
            "job_classification_failed",
            level="WARNING",
            provider=saved_job.provider,
            company=saved_job.company,
            external_job_id=saved_job.external_job_id,
            error=str(exc),
        )
        return None


def classify_local_job(normalized_job: NormalizedJob) -> Classification:
    title = _norm(normalized_job.get("title", ""))
    location = _norm(normalized_job.get("location", ""))
    description = _norm(str(normalized_job.get("description", ""))[: int(os.getenv("LOCAL_CLASSIFIER_MAX_DESCRIPTION_CHARS", "1500"))])
    full_text = f" {title} {location} {description} "

    role_family, family_score = _best_label(full_text, title, ROLE_FAMILY_PATTERNS, default="other")
    role_type, type_score = _best_label(full_text, title, ROLE_TYPE_PATTERNS, default="other")
    seniority, seniority_score = _seniority(title, full_text)
    location_fit = _location_fit(location)
    relevance_score = _relevance_score(role_family, role_type, seniority, location_fit, family_score, type_score)
    confidence = _confidence(family_score, type_score, seniority_score, location_fit)

    return {
        "role_family": role_family,
        "role_type": role_type,
        "seniority": seniority,
        "location_fit": location_fit,
        "relevance_score": relevance_score,
        "confidence": confidence,
        "reason": _reason(role_family, role_type, seniority, location_fit, relevance_score, confidence),
    }


def validate_classification(payload: Dict[str, Any]) -> Classification:
    if not isinstance(payload, dict):
        raise ValueError("classification payload is not an object")

    role_family = str(payload.get("role_family", "")).strip()
    role_type = str(payload.get("role_type", "")).strip()
    seniority = str(payload.get("seniority", "")).strip()
    location_fit = str(payload.get("location_fit", "")).strip()
    reason = str(payload.get("reason", "")).strip()

    if role_family not in ROLE_FAMILIES:
        raise ValueError(f"invalid role_family: {role_family}")
    if role_type not in ROLE_TYPES:
        raise ValueError(f"invalid role_type: {role_type}")
    if seniority not in SENIORITIES:
        raise ValueError(f"invalid seniority: {seniority}")
    if location_fit not in LOCATION_FITS:
        raise ValueError(f"invalid location_fit: {location_fit}")
    if not reason:
        raise ValueError("missing reason")

    relevance_score = int(payload.get("relevance_score"))
    confidence = float(payload.get("confidence"))
    if not 0 <= relevance_score <= 100:
        raise ValueError("relevance_score must be between 0 and 100")
    if not 0 <= confidence <= 1:
        raise ValueError("confidence must be between 0 and 1")

    return {
        "role_family": role_family,
        "role_type": role_type,
        "seniority": seniority,
        "location_fit": location_fit,
        "relevance_score": relevance_score,
        "confidence": confidence,
        "reason": reason[:500],
    }


def is_relevant(classification: Optional[Classification]) -> bool:
    if not classification:
        return False
    try:
        return int(classification.get("relevance_score", 0)) >= min_relevance_score()
    except Exception:
        return False


def format_classified_jobs_message(source: str, entries: List[Dict[str, Any]], board_url: str = "", limit: int = 15) -> str:
    header = f"Board: {board_url}" if board_url else f"[{source}]"
    lines = [header]
    ranked = sorted(
        entries,
        key=lambda entry: int(entry.get("classification", {}).get("relevance_score", 0)),
        reverse=True,
    )
    for entry in ranked[:limit]:
        job = entry["job"]
        classification = entry["classification"]
        score = classification.get("relevance_score", 0)
        role_type = classification.get("role_type", "other")
        seniority = classification.get("seniority", "unknown")
        location_fit = classification.get("location_fit", "unclear")
        title = job.get("title") or "Untitled role"
        company = job.get("company") or source
        location = job.get("location") or "Unknown location"
        reason = classification.get("reason") or "No explanation returned."
        url = job.get("url") or ""
        lines.append(f"{score}/100 | {company} | {title}")
        lines.append(f"{role_type} | {seniority} | {location_fit} | {location}")
        lines.append(f"Why: {reason}")
        if url:
            lines.append(str(url))
    return "\n".join(lines)


def _best_label(text: str, title: str, patterns: Dict[str, List[str]], default: str) -> Tuple[str, int]:
    best_label = default
    best_score = 0
    for label, keywords in patterns.items():
        score = 0
        for keyword in keywords:
            needle = f" {keyword.strip()} "
            if needle in f" {title} ":
                score += 3
            if needle in text:
                score += 1
        if score > best_score:
            best_label = label
            best_score = score
    return best_label, best_score


def _seniority(title: str, text: str) -> Tuple[str, int]:
    for label in ("manager", "staff", "senior", "intern", "new_grad", "junior", "mid"):
        for keyword in SENIORITY_PATTERNS[label]:
            needle = f" {keyword.strip()} "
            if needle in f" {title} ":
                return label, 3
            if needle in text:
                return label, 1
    return "unknown", 0


def _location_fit(location: str) -> str:
    if not location:
        return "unclear"
    if any(hint in location for hint in NON_US_HINTS) and not _has_us_signal(location):
        return "non_us"
    remote = "remote" in location
    hybrid = "hybrid" in location
    if _has_us_signal(location):
        if remote:
            return "us_remote"
        if hybrid:
            return "hybrid_us"
        return "us_onsite"
    if remote and any(token in location for token in ("worldwide", "global", "anywhere")):
        return "unclear"
    return "unclear"


def _has_us_signal(text: str) -> bool:
    padded = f" {text} "
    if any(pattern in padded for pattern in (" united states ", " usa ", " u.s. ", " us ", " u.s.a. ")):
        return True
    if any(f" {state} " in padded for state in US_STATES):
        return True
    return any(re.search(rf"(?<![a-z]){re.escape(abbr)}(?![a-z])", text) for abbr in US_STATE_ABBR)


def _relevance_score(
    role_family: str,
    role_type: str,
    seniority: str,
    location_fit: str,
    family_score: int,
    type_score: int,
) -> int:
    score = 0
    if role_family == "software_engineering":
        score += 45
    elif role_family in {"data", "ml_ai", "security"}:
        score += 25
    elif role_family == "product":
        score += 10

    if role_type in {"backend", "full_stack", "platform", "infra", "data_engineering", "ml_ai", "product_engineering"}:
        score += 25
    elif role_type == "frontend":
        score += 18
    elif role_type in {"devops", "security", "mobile", "qa"}:
        score += 10

    if seniority in {"intern", "new_grad", "junior"}:
        score += 20
    elif seniority == "unknown":
        score += 8
    elif seniority == "mid":
        score += 5
    elif seniority == "senior":
        score -= 20
    elif seniority in {"staff", "manager"}:
        score -= 35

    if location_fit in {"us_remote", "hybrid_us", "us_onsite"}:
        score += 10
    elif location_fit == "unclear":
        score -= 5
    elif location_fit == "non_us":
        score -= 30

    if family_score >= 3 and type_score >= 2:
        score += 5

    return max(0, min(100, score))


def _confidence(family_score: int, type_score: int, seniority_score: int, location_fit: str) -> float:
    raw = 0.25
    raw += min(0.3, family_score * 0.08)
    raw += min(0.25, type_score * 0.06)
    raw += 0.1 if seniority_score else 0
    raw += 0.1 if location_fit != "unclear" else 0
    return round(max(0.05, min(0.98, raw)), 2)


def _reason(role_family: str, role_type: str, seniority: str, location_fit: str, score: int, confidence: float) -> str:
    return (
        f"Local classifier scored this as {role_family}/{role_type}, "
        f"seniority={seniority}, location_fit={location_fit}; "
        f"score={score}, confidence={confidence}."
    )


def _norm(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9.+#-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _log(logger: Any, event: str, level: str = "INFO", **fields: Any) -> None:
    if not logger:
        return
    try:
        from structured_logging import log_event

        log_event(logger, event, level=level, **fields)
    except Exception:
        return
