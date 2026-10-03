from __future__ import annotations


def observability_contract(policy: dict) -> dict:
    labels = list(policy["metric_labels"])
    series_limit = int(policy["max_metric_series"])
    if len(labels) != len(set(labels)) or any(label in {"request_id", "replica_id", "tenant_id", "prompt", "url"} for label in labels):
        raise ValueError("unbounded metric label")
    return {
        "labels": labels,
        "max_series": series_limit,
        "series_count": min(series_limit, len(policy["windows"]) * 2 * 2),
        "counters": ["requests_total", "errors_total", "tokens_total"],
        "histograms": {"queue_ms": list(policy["histogram_buckets_ms"]), "ttft_ms": list(policy["histogram_buckets_ms"]), "e2e_ms": list(policy["histogram_buckets_ms"])},
    }
