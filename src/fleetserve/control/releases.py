import json
from ..errors import FleetError
from ..util import canonical, digest, identifier, integer


class Releases:
    def __init__(self, store, planner, outbox, config, clock):
        self.store, self.planner, self.outbox = store, planner, outbox
        self.config, self.clock = config, clock

    def bootstrap(self):
        with self.store.transaction():
            for alias, revision in self.config['aliases'].items():
                if self.store.one('SELECT alias FROM routes WHERE alias=?', (alias,)):
                    continue
                plan = self.planner.stable(revision)
                self.store.execute('INSERT INTO routes VALUES (?,?,?)', (alias, 0, canonical(plan)))
                self.outbox.enqueue('bootstrap:'+alias, alias, 0, plan)

    def route(self, alias):
        row = self.store.one('SELECT * FROM routes WHERE alias=?', (alias,))
        if row is None:
            raise FleetError('unknown_alias', alias, 404)
        return {**row, 'plan': json.loads(row['plan'])}

    def get(self, release_id):
        row = self.store.one('SELECT * FROM releases WHERE id=?', (release_id,))
        if row is None:
            raise FleetError('unknown_release', release_id, 404)
        return {**row, **{key: json.loads(row[key]) for key in ['stable_plan','candidate_plan','policy']}}

    def begin(self, release_id, alias, revision, candidate_bps, expected_epoch, operation_id):
        identifier(release_id, 'release_id'); identifier(operation_id, 'operation_id')
        integer(expected_epoch, 'expected_epoch')
        payload = [release_id, alias, revision, candidate_bps, expected_epoch]
        fingerprint = digest(payload)
        with self.store.transaction():
            previous = self.store.operation(operation_id, 'release.begin', fingerprint)
            if previous:
                return json.loads(previous['result'])
            route = self.route(alias)
            if route['epoch'] != expected_epoch:
                raise FleetError('epoch_conflict', 'route changed', 409)
            if self.store.one('SELECT id FROM releases WHERE id=?', (release_id,)):
                raise FleetError('release_conflict', 'release id already exists', 409)
            plan = self.planner.canary(route['plan'], revision, candidate_bps, release_id)
            epoch = expected_epoch + 1
            policy = canonical(self.config['policy'])
            self.store.execute('INSERT INTO releases VALUES (?,?,?,?,?,?,?,?,?)',
                               (release_id, alias, 'canary_pending', epoch, canonical(route['plan']),
                                canonical(plan), policy, self.clock(), None,))
            self.store.execute('UPDATE routes SET epoch=?,plan=? WHERE alias=?', (epoch, canonical(plan), alias))
            self.outbox.enqueue(operation_id, alias, epoch, plan, release_id, 'canary')
            result = {'release_id': release_id, 'epoch': epoch, 'phase': 'canary_pending'}
            self.store.remember(operation_id, 'release.begin', fingerprint, result)
            self.store.audit('release.begin', release_id, result, self.clock())
        return result

    def apply(self, assessment_id, operation_id):
        identifier(operation_id, 'operation_id')
        fingerprint = digest([assessment_id])
        with self.store.transaction():
            previous = self.store.operation(operation_id, 'release.apply', fingerprint)
            if previous:
                return json.loads(previous['result'])
            assessment = self.store.one('SELECT * FROM assessments WHERE id=?', (assessment_id,))
            if assessment is None:
                raise FleetError('unknown_assessment', assessment_id, 404)
            release = self.get(assessment['release_id'])
            route = self.route(release['alias'])
            version = int(self.store.one("SELECT value FROM meta WHERE key='evidence_version'")['value'])
            if release['phase'] != 'canary' or route['epoch'] != assessment['route_epoch']:
                raise FleetError('stale_assessment', 'deployment or accepted evidence changed', 409)
            decision = assessment['decision']
            epoch = route['epoch']
            if decision != 'HOLD':
                plan = self.planner.settle(route['plan'], decision)
                self.planner.validate_capacity(plan)
                epoch += 1
                self.store.execute('UPDATE routes SET epoch=?,plan=? WHERE alias=?', (epoch, canonical(plan), release['alias']))
                phase = 'promoted' if decision == 'PROMOTE' else 'rolled_back'
                self.store.execute("UPDATE releases SET phase='terminal_pending',route_epoch=?,decision=? WHERE id=?",
                                   (epoch, decision, release['id']))
                self.outbox.enqueue(operation_id, release['alias'], epoch, plan, release['id'], phase)
            result = {'release_id': release['id'], 'decision': decision, 'epoch': epoch,
                      'phase': 'canary' if decision == 'HOLD' else 'terminal_pending'}
            self.store.remember(operation_id, 'release.apply', fingerprint, result)
            self.store.audit('release.apply', release['id'], result, self.clock())
        return result

    def rollback(self, release_id, expected_epoch, operation_id):
        identifier(operation_id, 'operation_id')
        integer(expected_epoch, 'expected_epoch')
        fingerprint = digest([release_id, expected_epoch])
        with self.store.transaction():
            previous = self.store.operation(operation_id, 'release.rollback', fingerprint)
            if previous:
                return json.loads(previous['result'])
            release = self.get(release_id)
            route = self.route(release['alias'])
            if route['epoch'] != expected_epoch or release['phase'] != 'canary':
                raise FleetError('epoch_conflict', 'release is no longer the active canary', 409)
            plan = self.planner.settle(route['plan'], 'ROLLBACK')
            self.planner.validate_capacity(plan)
            epoch = expected_epoch + 1
            self.store.execute('UPDATE routes SET epoch=?,plan=? WHERE alias=?', (epoch, canonical(plan), release['alias']))
            self.store.execute("UPDATE releases SET phase='terminal_pending',route_epoch=?,decision='ROLLBACK' WHERE id=?", (epoch, release_id))
            self.outbox.enqueue(operation_id, release['alias'], epoch, plan, release_id, 'rolled_back')
            result = {'release_id': release_id, 'epoch': epoch, 'phase': 'terminal_pending'}
            self.store.remember(operation_id, 'release.rollback', fingerprint, result)
        return result

    def list(self):
        return [self.get(row['id']) for row in self.store.all('SELECT id FROM releases ORDER BY created,id')]
