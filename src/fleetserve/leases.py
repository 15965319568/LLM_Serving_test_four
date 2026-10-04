"""Worker registration and heartbeat repository. Generations survive process restarts."""
from .errors import FleetError
from .util import identifier, integer, number


class LeaseRegistry:
    def __init__(self, store, artifacts, config, clock):
        self.store, self.artifacts, self.config, self.clock = store, artifacts, config, clock

    def register(self, worker, pool, revision, capacity, ttl=60):
        identifier(worker, 'worker')
        integer(capacity, 'capacity', 1)
        number(ttl, 'ttl', .001)
        if pool not in self.config['pools']:
            raise FleetError('unknown_pool', pool, 404)
        if capacity > self.config['pools'][pool]['capacity']:
            raise FleetError('invalid_capacity', 'worker exceeds pool capacity')
        spec = self.artifacts.spec(revision)
        with self.store.transaction():
            old = self.store.one('SELECT * FROM leases WHERE worker=?', (worker,))
            generation = old['generation'] + 1 if old else 1
            self.store.execute('INSERT OR REPLACE INTO leases VALUES (?,?,?,?,?,?,?,?,?,?)',
                               (worker, generation, 0, pool, revision, capacity, capacity,
                                self.clock()+ttl, 'ready', spec.tokenizer))
            self.store.audit('worker.register', worker, {'generation': generation, 'pool': pool,
                                                        'revision': revision}, self.clock())
        return self.get(worker)

    def get(self, worker):
        row = self.store.one('SELECT * FROM leases WHERE worker=?', (worker,))
        if row is None:
            raise FleetError('unknown_worker', worker, 404)
        return row

    def heartbeat(self, worker, generation, sequence, free, ttl=60):
        integer(generation, 'generation', 1)
        integer(sequence, 'sequence', 1)
        integer(free, 'free')
        number(ttl, 'ttl', .001)
        with self.store.transaction():
            row = self.get(worker)
            if sequence <= row['sequence']:
                return {'accepted': False, 'generation': row['generation'], 'sequence': row['sequence']}
            if free > row['capacity']:
                raise FleetError('invalid_capacity', 'free exceeds worker capacity')
            self.store.execute('UPDATE leases SET sequence=?,free=?,expires=? WHERE worker=?',
                               (sequence, free, self.clock()+ttl, worker))
        return {'accepted': True, 'generation': generation, 'sequence': sequence}

    def drain(self, worker, generation):
        with self.store.transaction():
            row = self.get(worker)
            if row['generation'] != generation:
                raise FleetError('worker_generation', 'worker was replaced', 409)
            self.store.execute("UPDATE leases SET state='draining' WHERE worker=?", (worker,))
        return self.get(worker)

    def available(self, tenant, revision, pools):
        spec = self.artifacts.spec(revision)
        allowed = set(self.config['tenants'][tenant]['regions'])
        rows = self.store.all("SELECT * FROM leases WHERE revision=? AND state='ready' AND expires>=?",
                              (revision, self.clock()))
        result = []
        for row in rows:
            pool = self.config['pools'][row['pool']]
            if row['pool'] not in pools or pool['region'] not in allowed or row['tokenizer'] != spec.tokenizer:
                continue
            active = self.store.one("SELECT COUNT(*) AS n FROM bindings WHERE worker=? AND generation=? AND state IN ('admitting','queued','running')",
                                    (row['worker'], row['generation']))['n']
            # A heartbeat reports capacity available before this coordinator's
            # reservations; local reservations must still be accounted for.
            active += getattr(self, 'reserved_extra', lambda worker, generation: 0)(row['worker'], row['generation'])
            if min(row['free'], row['capacity']) > active:
                result.append({**row, 'reservations': active})
        return sorted(result, key=lambda r: (r['reservations'], r['worker']))

    def list(self):
        return self.store.all('SELECT * FROM leases ORDER BY worker')
