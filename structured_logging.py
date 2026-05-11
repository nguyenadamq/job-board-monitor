import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


_BASE_FIELDS = {"timestamp", "level", "service", "event"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "service": getattr(record, "service", self.service),
            "event": getattr(record, "event", record.getMessage()),
        }

        fields = getattr(record, "fields", {})
        if isinstance(fields, dict):
            for key, value in fields.items():
                if value is None:
                    continue
                if key in _BASE_FIELDS:
                    payload[f"field_{key}"] = value
                else:
                    payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False, separators=(",", ":"))


def configure_logger(service: str) -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(service)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    logger.addHandler(handler)

    return logger


def log_event(logger: logging.Logger, event: str, level: int | str = logging.INFO, **fields: Any) -> None:
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logger.log(level, event, extra={"event": event, "fields": fields})
