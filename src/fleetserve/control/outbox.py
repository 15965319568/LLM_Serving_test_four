import json
from .progression import Progression
from ..errors import FleetError
from ..util import canonical


class Outbox:
    def __init__(self, store, edge, clock):
        self.store, self.edge, self.clock = store, edge, clock

    def enqueue(self, effect_id, alias, epoch, plan, release_id=None, target_phase=None):
        self.store.execute('INSERT INTO effects(id,alias,epoch,plan,release_id,target_phase) VALUES (?,?,?,?,?,?)',
                           (effect_id, alias, epoch, canonical(plan), release_id, target_phase))

    def reconcile(self, limit=100, after_apply=None):
        if type(limit) is not int or limit < 1:
            raise FleetError('invalid_limit')
        rows = self.store.all("SELECT * FROM effects WHERE status='pending' ORDER BY alias,epoch LIMIT ?", (limit,))
        results = []
        for row in rows:
            self.store.execute('UPDATE effects SET attempts=attempts+1 WHERE id=?', (row['id'],))
            try:
                self.store.execute("UPDATE effects SET status='done' WHERE id=?", (row['id'],))
                result = self.edge.apply(row['id'], row['alias'], row['epoch'], json.loads(row['plan']))
                # This boundary is deliberately injectable: a process can lose
                # the acknowledgement after the independent edge committed.
                if after_apply is not None:
                    after_apply(row, result)
                with self.store.transaction():
                    self.store.execute("UPDATE effects SET status='done',last_error=NULL WHERE id=?", (row['id'],))
                    if row['release_id'] and not result.get('stale'):
                        self.store.execute('UPDATE releases SET phase=? WHERE id=? AND route_epoch=?',
                                           (row['target_phase'], row['release_id'], row['epoch']))
                    if row['release_id'] and not result.get('stale') and row['target_phase']=='canary':
                        Progression(self.store,self.clock).acknowledged(row['release_id'],row['epoch'])
                    self.store.audit('edge.ack', row['alias'], {'effect': row['id'], **result}, self.clock())
                results.append({'effect': row['id'], **result})
            except FleetError as error:
                self.store.execute('UPDATE effects SET last_error=? WHERE id=?', (error.code, row['id']))
                raise
        return results

    def pending(self):
        return self.store.all("SELECT id,alias,epoch,attempts,last_error FROM effects WHERE status='pending' ORDER BY alias,epoch")
