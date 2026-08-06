from __future__ import annotations

import hashlib
import json

MUTABLE_FIELDS = ("score", "title", "url", "descendants", "dead", "deleted", "text")


def payload_hash(item: dict) -> str:
    subset = {field: item.get(field) for field in MUTABLE_FIELDS}
    encoded = json.dumps(subset, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
