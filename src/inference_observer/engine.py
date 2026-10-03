from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .io import load_inputs, parse_time
from .metrics import observability_contract
from .redaction import prompt_hash
from .timeutil import is_visible, nearest_rank, psi, token_bin


def _ms(start: str, end: str) -> float:
    return (parse_time(end) - parse_time(start)).total_seconds() * 1000.0


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _quality(data: dict[str, Any], window_index: int, window_id: str) -> tuple[list[dict], str, float]:
    policy = data["policies"]["drift"]
    rows = list(data["quality"])
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["slice_id"]].append(row)
    findings: list[dict] = []
    overall_gate = "pass"
    max_psi = 0.0
    for slice_id in sorted(grouped):
        slice_rows = grouped[slice_id]
        current = slice_rows
        weighted_delta = sum(float(row["candidate_score"]) - float(row["baseline_score"]) for row in current)
        effective = sum(float(row["effective_count"]) for row in current)
        baseline_counts = [float(row["baseline_count"]) for row in current]
        candidate_counts = [float(row["candidate_count"]) for row in current]
        drift_psi = psi(baseline_counts, candidate_counts)
        max_psi = max(max_psi, drift_psi)
        gate = "pass"
        if effective < float(policy["min_effective_count"]):
            gate = "insufficient"
        elif weighted_delta <= -float(policy["max_quality_drop"]) or drift_psi >= float(policy["psi_fail"]):
            gate = "fail"
            overall_gate = "fail"
        elif drift_psi >= float(policy["psi_warn"]):
            gate = "warn"
            if overall_gate == "pass":
                overall_gate = "warn"
        findings.append({"window_id": window_id, "slice_id": slice_id, "weighted_quality_delta": f"{weighted_delta:.6f}", "psi": f"{drift_psi:.6f}", "effective_count": f"{effective:.2f}", "gate": gate})
    return findings, overall_gate, max_psi


def _replicas(data: dict[str, Any], eval_time: datetime, window_id: str) -> tuple[list[dict], dict[str, float]]:
    policy = data["policies"]["batching"]
    rows: list[dict] = []
    capacity: dict[str, float] = defaultdict(float)
    for replica in sorted(data["replicas"], key=lambda row: row["replica_id"]):
        if not (parse_time(replica["valid_from"]) <= eval_time < parse_time(replica["valid_to"])):
            continue
        snapshots = [row for row in data["snapshots"] if row["replica_id"] == replica["replica_id"] and parse_time(row["sample_time"]) <= eval_time]
        latest = max(snapshots, key=lambda row: parse_time(row["sample_time"])) if snapshots else None
        included = True
        reason = ""
        if latest is None:
            included, reason = False, "no_authoritative_snapshot"
        elif float(latest["kv_cache_pct"]) > float(policy["max_kv_cache_pct"]):
            included, reason = False, "kv_cache_limit"
        elif float(latest["queue_depth"]) > 12:
            included, reason = False, "queue_limit"
        value = 0.0
        if included and latest:
            value = float(replica["token_capacity_s"]) * max(0.0, 1.0 - float(latest["kv_cache_pct"]) / 100.0) * max(0.5, 1.0 - float(latest["queue_depth"]) / 100.0)
            capacity[replica["model"]] += value
        rows.append({"window_id": window_id, "replica_id": replica["replica_id"], "model": replica["model"], "telemetry_source": "agent" if latest else "none", "authoritative": "true" if latest else "false", "kv_cache_pct": latest["kv_cache_pct"] if latest else "", "queue_depth": latest["queue_depth"] if latest else "", "included": str(included).lower(), "exclude_reason": reason, "capacity_tokens_s": f"{value:.6f}"})
    return rows, capacity


def run(input_dir: str | Path, output_dir: str | Path) -> None:
    root, output = Path(input_dir), Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    data = load_inputs(root)
    policy = data["policies"]
    windows = sorted(policy["windows"], key=lambda row: parse_time(row["evaluation_time"]))
    requests = data["requests"]
    batches = {row["batch_id"]: row for row in data["batches"]}
    summary: list[dict] = []
    replay: list[dict] = []
    replica_rows: list[dict] = []
    drift_rows: list[dict] = []
    tenant_rows: list[dict] = []
    routes: list[dict] = []
    audit: list[dict] = []
    token_boundaries = policy["token_bins"]
    stable = policy["stable_model"]
    candidate = policy["candidate_model"]
    for window_index, window in enumerate(windows):
        window_id = window["window_id"]
        evaluation_time = parse_time(window["evaluation_time"])
        visible = list(requests)
        completed = [row for row in visible if parse_time(row["completed_time"]) <= evaluation_time]
        for row in visible:
            complete = parse_time(row["completed_time"]) <= evaluation_time
            replay.append({"window_id": window_id, "request_id": row["request_id"], "model": row["model"], "batch_id": row["batch_id"], "visibility": "completed" if complete else "in_flight", "queue_ms": f"{_ms(row['arrival_time'], row['dispatch_time']):.3f}" if complete else "", "ttft_ms": f"{_ms(row['arrival_time'], row['first_token_time']):.3f}" if complete else "", "e2e_ms": f"{_ms(row['arrival_time'], row['completed_time']):.3f}" if complete else "", "batch_size": batches.get(row["batch_id"], {}).get("batch_size", ""), "input_token_bin": token_bin(int(row["input_tokens"]), token_boundaries), "prompt_hash": prompt_hash(row.get("prompt", ""))})
        by_model: dict[str, list[dict]] = defaultdict(list)
        for row in completed:
            by_model[row["model"]].append(row)
        for model in sorted({stable, candidate}):
            model_rows = by_model.get(model, [])
            queue = [_ms(row["arrival_time"], row["dispatch_time"]) for row in model_rows]
            ttft = [_ms(row["arrival_time"], row["first_token_time"]) for row in model_rows]
            e2e = [_ms(row["arrival_time"], row["completed_time"]) for row in model_rows]
            errors = sum(row["status"] != "ok" for row in model_rows)
            token_counts = Counter(token_bin(int(row["input_tokens"]), token_boundaries) for row in model_rows)
            observed = [token_counts.get(index, 0) for index in range(len(policy["token_baseline"]))]
            input_psi = psi(policy["token_baseline"], observed)
            summary.append({"window_id": window_id, "model": model, "visible_requests": sum(row["model"] == model for row in visible), "completed_requests": len(model_rows), "error_rate": f"{(errors / len(model_rows)) if model_rows else 0.0:.6f}", "p95_queue_ms": f"{nearest_rank(queue, 0.95):.3f}", "p95_ttft_ms": f"{nearest_rank(ttft, 0.95):.3f}", "p95_e2e_ms": f"{nearest_rank(e2e, 0.95):.3f}", "input_psi": f"{input_psi:.6f}", "slo_pass": str(bool(model_rows) and errors / len(model_rows) <= float(policy["max_error_rate"]) and nearest_rank(queue, 0.95) <= float(policy["latency_slo_ms"]["max_p95_queue"]) and nearest_rank(ttft, 0.95) <= float(policy["latency_slo_ms"]["max_p95_ttft"]) and nearest_rank(e2e, 0.95) <= float(policy["latency_slo_ms"]["max_p95_e2e"])).lower(), "quality_gate": "pending", "capacity_tokens_s": "0.000000"})
        drift, quality_gate, max_quality_psi = _quality(data, window_index, window_id)
        drift_rows.extend(drift)
        replica_detail, capacities = _replicas(data, evaluation_time, window_id)
        replica_rows.extend(replica_detail)
        for row in summary[-2:]:
            row["quality_gate"] = quality_gate
            row["capacity_tokens_s"] = f"{capacities.get(row['model'], 0.0):.6f}"
        for tier in sorted(data["tenants"], key=lambda row: row["tenant_tier"]):
            tier_requests = [row for row in completed if row["tenant_tier"] == tier["tenant_tier"]]
            ttft_values = [_ms(row["arrival_time"], row["first_token_time"]) for row in tier_requests]
            tier_errors = sum(row["status"] != "ok" for row in tier_requests)
            tier_error_rate = tier_errors / len(tier_requests) if tier_requests else 0.0
            tenant_pass = bool(tier_requests) and nearest_rank(ttft_values, 0.95) <= float(tier["max_p95_ttft_ms"]) and tier_error_rate <= float(tier["max_error_rate"])
            tenant_rows.append({"window_id": window_id, "tenant_tier": tier["tenant_tier"], "requests": len(tier_requests), "p95_ttft_ms": f"{nearest_rank(ttft_values, 0.95):.3f}", "error_rate": f"{tier_error_rate:.6f}", "slo_pass": str(tenant_pass).lower()})
        approvals = [row for row in data["deployment"]["approvals"] if row["model"] == candidate and row["scope"] == data["deployment"]["scope"] and parse_time(row["signed_at"]) <= evaluation_time and (not row.get("revoked_at") or parse_time(row["revoked_at"]) > evaluation_time)]
        enabled = parse_time(data["deployment"]["candidate_enabled_at"]) <= evaluation_time
        candidate_summary = next(row for row in summary[-2:] if row["model"] == candidate)
        active_incidents = [row for row in data["incidents"] if row.get("model") in {candidate, "*"} and parse_time(row["event_time"]) <= evaluation_time and (not row.get("end_time") or evaluation_time < parse_time(row["end_time"]))]
        failed_tenant = any(row["slo_pass"] == "false" and row["tenant_tier"] == "gold" for row in tenant_rows[-3:])
        reasons: list[str] = []
        status = "CANARY"
        if not approvals or not enabled:
            status, reasons = "HOLD", ["approval_or_feature_gate"]
        elif any(row["action"] == "rollback" for row in active_incidents):
            status, reasons = "ROLLBACK", ["active_incident"]
        elif quality_gate == "fail":
            status, reasons = "ROLLBACK", ["quality_drift"]
        elif failed_tenant or candidate_summary["slo_pass"] == "false":
            status, reasons = "ROLLBACK", ["candidate_slo"]
        elif any(row["action"] == "hold" for row in active_incidents):
            status, reasons = "HOLD", ["active_incident_hold"]
        elif capacities.get(candidate, 0.0) < float(policy["throughput_target_tokens_s"]):
            status, reasons = "HOLD", ["capacity_below_target"]
        route = next((row for row in data["deployment"]["route_policy"] if row["window_id"] == window_id), {"candidate_percent": 0})
        candidate_percent = min(int(route["candidate_percent"]), int(data["deployment"]["max_canary_percent"])) if status == "CANARY" else 0
        routes.append({"window_id": window_id, "status": status, "stable_percent": 100 - candidate_percent, "candidate_percent": candidate_percent, "reason_codes": ";".join(reasons), "candidate_capacity_tokens_s": f"{capacities.get(candidate, 0.0):.6f}", "stable_capacity_tokens_s": f"{capacities.get(stable, 0.0):.6f}"})
        audit.append({"window_id": window_id, "category": "decision", "status": status, "visible_requests": len(visible), "completed_requests": len(completed), "quality_gate": quality_gate})
    _write_csv(output / "window_summary.csv", summary, ["window_id", "model", "visible_requests", "completed_requests", "error_rate", "p95_queue_ms", "p95_ttft_ms", "p95_e2e_ms", "input_psi", "slo_pass", "quality_gate", "capacity_tokens_s"])
    _write_csv(output / "batch_replay.csv", replay, ["window_id", "request_id", "model", "batch_id", "visibility", "queue_ms", "ttft_ms", "e2e_ms", "batch_size", "input_token_bin", "prompt_hash"])
    _write_csv(output / "replica_health.csv", replica_rows, ["window_id", "replica_id", "model", "telemetry_source", "authoritative", "kv_cache_pct", "queue_depth", "included", "exclude_reason", "capacity_tokens_s"])
    _write_csv(output / "drift_findings.csv", drift_rows, ["window_id", "slice_id", "weighted_quality_delta", "psi", "effective_count", "gate"])
    _write_csv(output / "tenant_slo.csv", tenant_rows, ["window_id", "tenant_tier", "requests", "p95_ttft_ms", "error_rate", "slo_pass"])
    _write_csv(output / "routing_plan.csv", routes, ["window_id", "status", "stable_percent", "candidate_percent", "reason_codes", "candidate_capacity_tokens_s", "stable_capacity_tokens_s"])
    validation = {"synthetic": True, "files": {name: len(data["requests"]) if name == "request_events.ndjson" else len(data["batches"]) if name == "batch_events.ndjson" else len(data["replicas"]) if name == "replica_inventory.csv" else len(data["snapshots"]) if name == "runtime_snapshots.csv" else len(data["quality"]) if name == "quality_evaluations.csv" else len(data["tenants"]) if name == "tenant_slo.csv" else len(data["incidents"]) if name == "incident_events.ndjson" else len(data["annotations"]) if name == "operator_annotations.ndjson" else 1 for name in data["input_files"]}, "window_count": len(windows), "duplicate_request_ids": len(requests) - len({row["request_id"] for row in requests}), "duplicate_batch_ids": len(batches) - len(set(batches))}
    (output / "input_validation.json").write_text(json.dumps(validation, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (output / "observability_contract.json").write_text(json.dumps(observability_contract(policy), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (output / "run_manifest.json").write_text(json.dumps({"schema_version": policy["schema_version"], "input_files": data["input_files"], "engine_version": "1.0.0"}, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (output / "audit.ndjson").write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in audit), encoding="utf-8")
