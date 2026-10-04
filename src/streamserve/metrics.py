"""Bounded Prometheus counters and histograms; request content is never a label."""
from collections import Counter
import math


class Metrics:
    def __init__(self):
        self.counters = Counter()
        self.buckets = (.001, .01, .1, 1.0, 10.0, math.inf)
        self.latency = Counter()
        self.latency_sum = Counter()

    def count(self, name, alias, outcome, value=1):
        self.counters[(name, alias, outcome)] += value

    def observe(self, alias, seconds):
        seconds = max(0, seconds)
        self.latency_sum[alias] += seconds
        for upper in self.buckets:
            if seconds <= upper:
                self.latency[(alias, upper)] += 1

    def render(self, scheduler, cache):
        lines = []
        for (name, alias, outcome), value in sorted(self.counters.items()):
            lines.append(f'streamserve_{name}{{model="{alias}",outcome="{outcome}"}} {value}')
        for (alias, upper), value in sorted(self.latency.items()):
            bound = '+Inf' if math.isinf(upper) else str(upper)
            lines.append(f'streamserve_ttft_seconds_bucket{{model="{alias}",le="{bound}"}} {value}')
        for alias, value in sorted(self.latency_sum.items()):
            lines.append(f'streamserve_ttft_seconds_sum{{model="{alias}"}} {value:.9f}')
            lines.append(f'streamserve_ttft_seconds_count{{model="{alias}"}} {self.latency[(alias, math.inf)]}')
        for name in ('active', 'queued', 'kv_used'):
            lines.append(f'streamserve_{name} {scheduler[name]}')
        for name in ('used', 'hits', 'misses', 'pinned'):
            lines.append(f'streamserve_cache_{name} {cache[name]}')
        return '\n'.join(lines) + '\n'
