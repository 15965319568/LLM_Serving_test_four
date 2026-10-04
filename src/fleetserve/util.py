"""Wire values and stable identities shared by the service boundaries."""
import hashlib
import json
import math
from datetime import datetime, timezone
from .errors import FleetError


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def integer(value, field, minimum=0):
    if type(value) is not int or value < minimum:
        raise FleetError('invalid_value', field + ' must be an integer >= ' + str(minimum))
    return value


def number(value, field, minimum=0):
    if type(value) not in (int, float) or not math.isfinite(value) or value < minimum:
        raise FleetError('invalid_value', field + ' must be a finite number >= ' + str(minimum))
    return float(value)


def identifier(value, field):
    if not isinstance(value, str) or not 1 <= len(value) <= 96:
        raise FleetError('invalid_value', field + ' must be a nonempty bounded string')
    if any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._:' for c in value):
        raise FleetError('invalid_value', field + ' has unsupported characters')
    return value


def timestamp(value):
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError('timezone required')
            return parsed.astimezone(timezone.utc).timestamp()
        except ValueError as error:
            raise FleetError('invalid_timestamp', str(error)) from None
    return number(value, 'timestamp')


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


class SystemClock:
    def __call__(self):
        import time
        return time.time()


class ManualClock:
    def __init__(self, now=0):
        self.now = float(now)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += number(seconds, 'advance')
        return self.now
