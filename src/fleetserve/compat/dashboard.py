"""Descriptive fleet-wide dashboard for archived benchmark exports.

This reader intentionally accepts worker and shadow rows. Its population is the
export file, not admitted customer requests; ops still uses it for load tests.
"""
from collections import defaultdict
from ..telemetry.statistics import nearest_rank


def summarize_export(rows):
    groups = defaultdict(list)
    for row in rows:
        if not isinstance(row,dict):
            continue
        alias = row.get('model',row.get('alias','unknown'))
        latency = row.get('duration_ms',row.get('latency'))
        if type(latency) in (int,float) and latency >= 0:
            groups[alias].append(float(latency))
    return [{'alias':alias,'rows':len(values),'p95_ms':nearest_rank(values,.95),
             'mean_ms':sum(values)/len(values)} for alias,values in sorted(groups.items())]
