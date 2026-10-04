"""Validated immutable runtime configuration and JSON loading."""
from dataclasses import asdict, dataclass, field
from pathlib import Path
import json
from .protocol import ServiceError


@dataclass(frozen=True)
class ModelSpec:
    revision: str
    tokenizer: str = 'unicode-v1'
    context_limit: int = 1024

    def validate(self):
        if not self.revision or len(self.revision) > 64 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._' for c in self.revision):
            raise ServiceError('invalid_model', 'invalid revision identifier')
        if self.tokenizer not in ('unicode-v1', 'utf8-v1'):
            raise ServiceError('invalid_model', 'unsupported tokenizer')
        if type(self.context_limit) is not int or self.context_limit < 2:
            raise ServiceError('invalid_model', 'context limit must be at least two')

    def wire(self):
        return asdict(self)


@dataclass(frozen=True)
class TenantSpec:
    token: str
    max_active: int = 2
    max_queued: int = 32
    weight: int = 1


@dataclass(frozen=True)
class Settings:
    models: dict = field(default_factory=lambda: {'chat': ModelSpec('stable')})
    tenants: dict = field(default_factory=lambda: {'demo': TenantSpec('demo-token')})
    max_active: int = 4
    kv_tokens: int = 4096
    cache_tokens: int = 4096
    admin_token: str = 'local-admin'
    max_body_bytes: int = 65536

    def validate(self):
        if not self.models or not self.tenants:
            raise ServiceError('invalid_config', 'models and tenants are required')
        for spec in self.models.values():
            spec.validate()
        for name, tenant in self.tenants.items():
            if not name or not tenant.token or min(tenant.max_active, tenant.max_queued, tenant.weight) < 1:
                raise ServiceError('invalid_config', 'invalid tenant configuration')
        if len({t.token for t in self.tenants.values()}) != len(self.tenants):
            raise ServiceError('invalid_config', 'tenant credentials must be unique')
        if min(self.max_active, self.kv_tokens, self.max_body_bytes) < 1 or self.cache_tokens < 0:
            raise ServiceError('invalid_config', 'invalid resource capacity')
        revisions = {}
        for spec in self.models.values():
            if spec.revision in revisions and revisions[spec.revision] != spec:
                raise ServiceError('invalid_config', 'revision definitions conflict')
            revisions[spec.revision] = spec

    @classmethod
    def load(cls, path):
        value = json.loads(Path(path).read_text(encoding='utf-8'))
        value['models'] = {k: ModelSpec(**v) for k, v in value['models'].items()}
        value['tenants'] = {k: TenantSpec(**v) for k, v in value['tenants'].items()}
        result = cls(**value)
        result.validate()
        return result
