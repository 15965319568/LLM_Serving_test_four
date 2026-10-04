import math
from collections import Counter


def nearest_rank(values, quantile):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values)*quantile)-1)]


def summarize(samples):
    qualities = [s['quality'] for s in samples if s['quality'] is not None]
    return {'count': len(samples), 'quality_count': len(qualities),
            'error_rate': sum(s['outcome'] != 'completed' for s in samples)/len(samples) if samples else None,
            'p95_ms': nearest_rank([s['latency_ms'] for s in samples], .95),
            'quality_mean': sum(qualities)/len(qualities) if qualities else None}


def mix_distance(left, right, cohorts):
    a, b = Counter(s['cohort'] for s in left), Counter(s['cohort'] for s in right)
    if not left or not right:
        return None
    return .5 * sum(abs(a[c]/len(left)-b[c]/len(right)) for c in cohorts)
