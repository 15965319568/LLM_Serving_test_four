"""Resolve a checked-in profile from relative layers and retain leaf provenance."""
from copy import deepcopy
import json
from pathlib import Path
from .errors import FleetError
from .util import digest, identifier, integer, number


def merge(base, overlay):
    result = deepcopy(base)
    for key, value in overlay.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        elif isinstance(value, list) and isinstance(result.get(key), list):
            result[key] = list(dict.fromkeys(result[key] + value))
        else:
            result[key] = deepcopy(value)
    return result


def leaves(value, prefix=''):
    for key, item in value.items():
        path = prefix + key
        if isinstance(item, dict):
            yield from leaves(item, path+'.')
        else:
            yield path, item


def validate(config):
    if config.get('schema') != 2:
        raise FleetError('invalid_config', 'resolved profile must use schema 2')
    if not config.get('tenants') or not config.get('pools') or not config.get('aliases'):
        raise FleetError('invalid_config', 'tenants, pools and aliases are required')
    for name, tenant in config['tenants'].items():
        identifier(name, 'tenant')
        if not tenant.get('regions') or not tenant.get('token') or not tenant.get('cohort'):
            raise FleetError('invalid_config', 'tenant needs regions, token and cohort')
        if type(tenant.get('max_tokens', 4096)) is not int:
            raise FleetError('invalid_config', 'tenant max_tokens must be integer')
    for name, pool in config['pools'].items():
        identifier(name, 'pool')
        integer(pool['capacity'], 'pool capacity', 1)
        if not pool.get('region'):
            raise FleetError('invalid_config', 'pool region is required')
    tokens = [v['token'] for v in config['tenants'].values()]
    if len(tokens) != len(set(tokens)):
        raise FleetError('invalid_config', 'tenant tokens must be unique')
    if not config.get('admin_token') or config['admin_token'] in tokens:
        raise FleetError('invalid_config', 'admin token must be distinct')
    policy = config['policy']
    integer(policy['min_samples'], 'min_samples', 1)
    for key in ('max_error_rate', 'max_latency_ratio', 'max_quality_drop', 'max_mix_distance'):
        number(policy[key], key)
    if policy['max_error_rate'] > 1 or policy['max_quality_drop'] > 1 or policy['max_mix_distance'] > 1:
        raise FleetError('invalid_config', 'rate thresholds must not exceed one')
    if not policy.get('required_cohorts'):
        raise FleetError('invalid_config', 'required_cohorts is empty')
    from .control.progression import validate_rollout
    validate_rollout(policy)
    return config


def load_profile(manifest_path, profile='production'):
    manifest_path = Path(manifest_path).resolve()
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    profiles = manifest.get('profiles', {})
    if profile not in profiles:
        raise FleetError('unknown_profile', profile, 404)
    merged, provenance = {}, {}
    for relative in profiles[profile]:
        path = (manifest_path.parent / relative).resolve()
        # Configuration may share checked-in files with a sibling directory.
        value = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(value, dict):
            raise FleetError('invalid_config', 'each layer must contain an object')
        merged = merge(merged, value)
        for leaf, item in leaves(value):
            provenance[leaf] = {'file': relative, 'value': item}
    validate(merged)
    active_leaves = dict(leaves(merged))
    provenance = {k: v for k, v in provenance.items() if k in active_leaves}
    return {'config': merged, 'digest': digest(merged), 'provenance': provenance,
            'manifest': str(manifest_path), 'profile': profile}
