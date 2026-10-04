"""Transport independent request, event and backend contracts."""
from dataclasses import asdict, dataclass
import hashlib
import json


class ServiceError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.status = status

    def wire(self):
        return {'error': {'code': self.code, 'message': str(self)}}


@dataclass(frozen=True)
class Request:
    tenant: str
    key: str
    model: str
    prompt: str
    max_tokens: int = 8
    stop: tuple = ()

    def validate(self):
        if not all(isinstance(v, str) and v for v in (self.tenant, self.key, self.model)):
            raise ServiceError('invalid_request', 'tenant, key and model must be nonempty strings')
        if len(self.key) > 128 or not isinstance(self.prompt, str):
            raise ServiceError('invalid_request', 'invalid key or prompt')
        if type(self.max_tokens) is not int or not 1 <= self.max_tokens <= 4096:
            raise ServiceError('invalid_request', 'max_tokens must be an integer in [1,4096]')
        if not isinstance(self.stop, (list, tuple)) or len(self.stop) > 8:
            raise ServiceError('invalid_request', 'stop must contain at most eight strings')
        if any(not isinstance(s, str) or not s or len(s) > 128 for s in self.stop):
            raise ServiceError('invalid_request', 'stop strings must be nonempty and bounded')

    def fingerprint(self):
        encoded = json.dumps(asdict(self), sort_keys=True, ensure_ascii=False, separators=(',', ':'))
        return hashlib.sha256(encoded.encode()).hexdigest()

    def wire(self):
        return asdict(self)


@dataclass(frozen=True)
class Ticket:
    request_id: str
    tenant: str
    revision: str


@dataclass(frozen=True)
class Event:
    request_id: str
    seq: int
    kind: str
    data: dict

    def wire(self):
        return asdict(self)


@dataclass(frozen=True)
class Work:
    request_id: str
    revision: str
    attempt: int
    prompt_tokens: tuple
    max_tokens: int


@dataclass(frozen=True)
class WorkerEvent:
    attempt: int
    index: int
    text: str = ''
    finish_reason: str | None = None


class BackendFault(Exception):
    """A backend failure. retryable does not by itself authorize replay."""
    def __init__(self, message='worker unavailable', retryable=True):
        super().__init__(message)
        self.retryable = retryable
