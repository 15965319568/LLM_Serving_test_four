from __future__ import annotations

import hashlib
import re
from typing import Any


SECRET_KEYS = re.compile(r"(authorization|token|secret|signature|password|api[_-]?key)", re.I)
URL_QUERY = re.compile(r"([?&](?:token|key|sig|signature|auth|request)=[^&\s]+)", re.I)


def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: "[REDACTED]" if SECRET_KEYS.search(str(key)) else redact(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return URL_QUERY.sub("", value)
    return value
