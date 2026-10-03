from __future__ import annotations

import math
from collections import Counter
from datetime import datetime
from typing import Iterable


def nearest_rank(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    rank = max(1, int(quantile * len(ordered)))
    return ordered[rank - 1]


def psi(expected: Iterable[float], actual: Iterable[float]) -> float:
    expected_values = [max(float(value), 1e-9) for value in expected]
    actual_values = [max(float(value), 1e-9) for value in actual]
    total_expected = sum(expected_values)
    total_actual = sum(actual_values)
    if total_expected <= 0 or total_actual <= 0 or len(expected_values) != len(actual_values):
        return 0.0
    score = 0.0
    for left, right in zip(expected_values, actual_values):
        ep = left / total_expected
        ap = right / total_actual
        score += (ap - ep) * math.log(ap / ep)
    return score


def token_bin(token_count: int, boundaries: list[int]) -> int:
    for index in range(len(boundaries) - 1):
        if boundaries[index] <= token_count < boundaries[index + 1]:
            return index
    return len(boundaries) - 2


def is_visible(event_time: datetime, evaluation_time: datetime) -> bool:
    return event_time <= evaluation_time
