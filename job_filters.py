import re
from typing import Iterable, List


EXCLUDE_TITLE_PATTERNS = [
    r"\bsenior\b",
    r"\bsr\.?\b",
    r"\bstaff\b",
    r"\bprincipal\b",
    r"\blead\b",
    r"\bmanager\b",
    r"\bdirector\b",
    r"\bhead\b",
    r"\bvp\b",
    r"\bvice president\b",
    r"\bchief\b",
    r"\bmachine\s*learning\b",
    r"\bml\b",
    r"\bdata\b",
    r"\banalytics\b",
    r"\bsecurity\b",
    r"\binfrastructure\b",
    r"\bsre\b",
    r"\bsite reliability\b",
    r"\bdevops\b",
    r"\bembedded\b",
    r"\bfirmware\b",
    r"\bnetwork\b",
    r"\btest\b",
    r"\bqa\b",
    r"\bquality\b",
    r"\bautomation\b",
    r"\bmobile\b",
    r"\bios\b",
    r"\bandroid\b",
    r"\bgame\b",
    r"\bgraphics\b",
    r"\brobotics\b",
    r"\breliability\b",
]

ROLE_INCLUDE_PATTERNS = [
    r"\bsoftware\s+engineer\b",
    r"\bsoftware\s+developer\b",
    r"\bbackend\b.*\b(engineer|developer)\b",
    r"\bfront\s*end\b.*\b(engineer|developer)\b",
    r"\bfrontend\b.*\b(engineer|developer)\b",
    r"\bfull\s*stack\b.*\b(engineer|developer)\b",
    r"\bfullstack\b.*\b(engineer|developer)\b",
    r"\bweb\b.*\b(engineer|developer)\b",
    r"\bapplication\b.*\b(engineer|developer)\b",
    r"\bplatform\b.*\b(engineer|developer)\b",
    r"\bapi\b.*\b(engineer|developer)\b",
]

US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware",
    "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana",
    "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "district of columbia", "washington dc", "d c", "d.c.",
}

US_STATE_ABBR = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
    "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
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


def normalize_title(title: str) -> str:
    text = (title or "").lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def title_matches(title: str) -> bool:
    text = normalize_title(title)
    if not text:
        return False
    if any(re.search(pattern, text) for pattern in EXCLUDE_TITLE_PATTERNS):
        return False
    return any(re.search(pattern, text) for pattern in ROLE_INCLUDE_PATTERNS)


def is_us_location_text(text: str) -> bool:
    normalized = (text or "").lower()
    if any(re.search(pattern, normalized) for pattern in US_COUNTRY_PATTERNS):
        return True
    if any(re.search(pattern, normalized) for pattern in REMOTE_US_PATTERNS):
        return True
    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", normalized):
            return True
    for abbr in US_STATE_ABBR:
        if re.search(rf"(?<![a-z]){abbr}(?![a-z])", normalized):
            return True
    return False


def any_us_location(texts: Iterable[str]) -> bool:
    return any(is_us_location_text(text) for text in texts if text)


def clean_texts(values: Iterable[str]) -> List[str]:
    return [str(value).strip() for value in values if value and str(value).strip()]
