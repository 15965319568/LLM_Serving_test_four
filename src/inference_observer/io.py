from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


UTC = timezone.utc
REQUIRED_FILES = (
    "policies.json", "request_events.ndjson", "batch_events.ndjson",
    "replica_inventory.csv", "runtime_snapshots.csv", "quality_evaluations.csv",
    "tenant_slo.csv", "deployment_policy.json", "incident_events.ndjson",
    "operator_annotations.ndjson",
)


def parse_time(value: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"time must be UTC Z string: {value!r}")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"time must be UTC: {value!r}")
    return parsed.astimezone(UTC)


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path.name}")
    return value


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path.name}:{line_no} is not an object")
        rows.append(value)
    return rows


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_inputs(root: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_FILES if not (root / name).is_file()]
    if missing:
        raise FileNotFoundError("missing input files: " + ", ".join(missing))
    policies = read_json(root / "policies.json")
    deployment = read_json(root / "deployment_policy.json")
    if policies.get("synthetic") is not True or deployment.get("synthetic") is not True:
        raise ValueError("synthetic marker is required")
    data = {
        "policies": policies,
        "deployment": deployment,
        "requests": read_ndjson(root / "request_events.ndjson"),
        "batches": read_ndjson(root / "batch_events.ndjson"),
        "replicas": read_csv(root / "replica_inventory.csv"),
        "snapshots": read_csv(root / "runtime_snapshots.csv"),
        "quality": read_csv(root / "quality_evaluations.csv"),
        "tenants": read_csv(root / "tenant_slo.csv"),
        "incidents": read_ndjson(root / "incident_events.ndjson"),
        "annotations": read_ndjson(root / "operator_annotations.ndjson"),
        "input_files": sorted(path.name for path in root.iterdir() if path.is_file()),
    }
    for request in data["requests"]:
        for key in ("arrival_time", "dispatch_time", "first_token_time", "completed_time"):
            parse_time(request[key])
    for event in data["batches"]:
        parse_time(event["event_time"])
    for snapshot in data["snapshots"]:
        parse_time(snapshot["sample_time"])
    for incident in data["incidents"]:
        parse_time(incident["event_time"])
        if incident.get("end_time"):
            parse_time(incident["end_time"])
    for window in policies.get("windows", []):
        parse_time(window["evaluation_time"])
    return data
