"""Route plans and placement share tenant residency and artifact constraints."""
from copy import deepcopy
import hashlib
from .errors import FleetError
from .util import identifier, integer


class Planner:
    def __init__(self, config, artifacts, leases):
        self.config, self.artifacts, self.leases = config, artifacts, leases

    def stable(self, revision, salt='bootstrap'):
        self.artifacts.spec(revision)
        return {'stable': revision, 'candidate': None, 'candidate_bps': 0, 'salt': salt,
                'pools': {tenant: [name for name, pool in self.config['pools'].items()
                                   if pool['region'] in spec['regions']]
                          for tenant, spec in self.config['tenants'].items()}}

    def canary(self, previous, revision, bps, release_id):
        integer(bps, 'candidate_bps', 1)
        if bps >= 10000:
            raise FleetError('invalid_canary', 'canary must retain stable traffic')
        self.artifacts.spec(revision)
        if previous['candidate'] is not None or revision == previous['stable']:
            raise FleetError('release_conflict', 'a distinct stable base is required', 409)
        plan = deepcopy(previous)
        plan.update(candidate=revision, candidate_bps=bps, salt=release_id)
        self.validate_capacity(plan)
        return plan

    def validate_capacity(self, plan):
        for tenant, pools in plan['pools'].items():
            for revision in [plan['stable']] + ([plan['candidate']] if plan['candidate'] else []):
                if not self.leases.available(tenant, revision, pools):
                    raise FleetError('insufficient_capacity', tenant + ':' + revision, 409)

    def revision_for(self, plan, tenant, key):
        seed = '\0'.join([plan['salt'], tenant, key]).encode()
        bucket = int.from_bytes(hashlib.sha256(seed).digest()[:8], 'big') % 10000
        if plan['candidate'] is not None and bucket < plan['candidate_bps']:
            return plan['candidate']
        return plan['stable']

    def place(self, plan, tenant, key):
        revision = self.revision_for(plan, tenant, key)
        candidates = self.leases.available(tenant, revision, plan['pools'].get(tenant, []))
        if not candidates:
            # Silently falling back would contaminate the canary population and
            # change an admitted request's actual deployment identity.
            raise FleetError('no_healthy_capacity', 'no eligible worker for routed revision', 503)
        return revision, candidates[0]

    def settle(self, plan, decision):
        if decision not in ('PROMOTE', 'ROLLBACK'):
            raise FleetError('invalid_decision')
        result = deepcopy(plan)
        result['stable'] = plan['candidate'] if decision == 'PROMOTE' else plan['stable']
        result['candidate'] = None
        result['candidate_bps'] = 0
        return result
