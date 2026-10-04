"""Global request identities bridge edge routing and per-worker request journals."""
import json
import uuid
from .errors import FleetError
from .util import canonical


class RequestRepository:
    TERMINAL = ('completed','cancelled','failed')

    def __init__(self, store, clock):
        self.store, self.clock = store, clock

    def find(self, tenant, key):
        return self.store.one('SELECT * FROM bindings WHERE tenant=? AND request_key=?', (tenant,key))

    def owned(self, tenant, request_id):
        row = self.store.one('SELECT * FROM bindings WHERE id=?', (request_id,))
        if row is None or row['tenant'] != tenant:
            raise FleetError('not_found', 'request not found', 404)
        return row

    def replay(self, request):
        previous = self.find(request.tenant, request.key)
        if previous and json.loads(previous['request_json'])['prompt'] != request.prompt:
            raise FleetError('idempotency_conflict', 'key belongs to another original request', 409)
        return previous

    def reserve(self, request, revision, worker, epoch):
        request_id = uuid.uuid4().hex
        with self.store.transaction():
            self.store.execute('''INSERT INTO bindings(id,tenant,request_key,fingerprint,request_json,alias,
                revision,pool,worker,generation,route_epoch,state,created) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (request_id, request.tenant, request.key, request.fingerprint(), canonical(request.wire()),
                 request.model, revision, worker['pool'], worker['worker'], worker['generation'], epoch,
                 'admitting', self.clock()))
        return self.owned(request.tenant, request_id)

    def bind(self, row, engine_id):
        self.store.execute("UPDATE bindings SET engine_id=?,state='queued' WHERE id=? AND state='admitting'", (engine_id,row['id']))

    def finish(self, row, status):
        with self.store.transaction():
            current = self.owned(row['tenant'],row['id'])
            if current['state'] in self.TERMINAL:
                return False
            self.store.execute('UPDATE bindings SET state=?,completion_tokens=?,terminal_reason=? WHERE id=?',
                               (status['state'], status.get('output_tokens',0), status.get('error'), row['id']))
            self.store.audit('request.terminal',row['id'],{'revision':row['revision'],'tenant':row['tenant'],
                                                       'state':status['state'],'tokens':status.get('output_tokens',0)},self.clock())
        return True

    def active(self):
        return self.store.all("SELECT * FROM bindings WHERE state NOT IN ('completed','cancelled','failed') ORDER BY created,id")

    def public(self, row):
        return {key: row[key] for key in ('id','tenant','alias','revision','pool','worker','generation','route_epoch','state','completion_tokens','terminal_reason')}
