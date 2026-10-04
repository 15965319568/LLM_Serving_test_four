"""Credential-to-tenant binding at the transport boundary."""
import hmac
from .protocol import ServiceError


def bearer(headers):
    value = headers.get('authorization', '')
    if not value.startswith('Bearer '):
        raise ServiceError('unauthorized', 'Bearer credential required', 401)
    return value[7:]


def tenant_for(settings, headers):
    token = bearer(headers)
    for tenant, spec in settings.tenants.items():
        if hmac.compare_digest(token, spec.token):
            return tenant
    raise ServiceError('unauthorized', 'invalid credential', 401)


def require_admin(settings, headers):
    if not hmac.compare_digest(bearer(headers), settings.admin_token):
        raise ServiceError('forbidden', 'admin credential required', 403)
