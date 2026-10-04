"""Admission and streams retain the worker, artifact and route epoch they accepted."""
import asyncio
import json
from pathlib import Path
from streamserve import Engine, Request, Settings, TenantSpec
from streamserve.backend import EchoBackend
from streamserve.protocol import Event, Ticket
from .errors import FleetError
from .requests import RequestRepository
from .util import digest


class Gateway:
    def __init__(self, store, edge, planner, artifacts, evidence, config, clock, state_dir, backend_factory=None):
        self.store, self.edge, self.planner, self.artifacts = store, edge, planner, artifacts
        self.evidence, self.config, self.clock = evidence, config, clock
        self.state_dir = Path(state_dir)
        self.backend_factory = backend_factory or (lambda worker, revision: EchoBackend())
        self.requests = RequestRepository(store,clock)
        self.engines, self.watchers = {}, {}
        self.admission_lock = asyncio.Lock()
        self.accepting = False
        self.closed = False

    def identity(self, row):
        return row['worker'], row['revision']

    def journal_path(self, row):
        return self.state_dir / 'workers' / (digest(self.identity(row))+'.sqlite3')

    async def engine_for(self, row):
        identity = self.identity(row)
        if identity not in self.engines:
            spec = self.artifacts.spec(row['revision'])
            pool = self.config['pools'][row['pool']]
            tenants = {name: TenantSpec(value['token'], pool['capacity'], 256, value.get('weight',1))
                       for name,value in self.config['tenants'].items()}
            settings = Settings(models={alias:spec for alias in self.config['aliases']},tenants=tenants,
                                max_active=pool['capacity'],kv_tokens=pool.get('kv_tokens',16384),
                                cache_tokens=pool.get('cache_tokens',8192),admin_token=self.config['admin_token'])
            engine = Engine(settings,self.backend_factory(row['worker'],row['revision']),self.journal_path(row))
            await engine.start()
            self.engines[identity] = engine
        return self.engines[identity]

    async def start(self):
        # Recovery consults the recorded journal before considering any new
        # placement. Missing admission is terminal; it is not inferred again.
        for row in self.requests.active():
            path = self.journal_path(row)
            if not path.exists():
                self.requests.finish(row,{'state':'failed','error':'admission_interrupted'})
                continue
            engine = await self.engine_for(row)
            original = engine.journal.find(row['tenant'],row['request_key'])
            if original is None:
                self.requests.finish(row,{'state':'failed','error':'admission_interrupted'})
                continue
            if row['engine_id'] is None:
                self.requests.bind(row,original['id'])
            self.requests.finish(row,engine.status(row['tenant'],original['id']))
        self.accepting = True

    async def submit(self, request):
        request.validate()
        if self.closed:
            raise FleetError('unavailable',status=503)
        if request.tenant not in self.config['tenants']:
            raise FleetError('unauthorized',status=401)
        if request.max_tokens > self.config['tenants'][request.tenant].get('max_tokens',4096):
            raise FleetError('tenant_token_limit',status=422)
        async with self.admission_lock:
            previous = self.requests.replay(request)
            if previous:
                return self.requests.public(previous)
            if not self.accepting:
                raise FleetError('draining',status=503)
            route = self.edge.get(request.model)
            revision, worker = self.planner.place(route['plan'],request.tenant,request.key)
            row = self.requests.reserve(request,revision,worker,route['epoch'])
            try:
                engine = await self.engine_for(row)
                ticket = await engine.submit(request)
                self.requests.bind(row,ticket.request_id)
                self.evidence.receipt({'request_id':row['id'],'tenant':request.tenant,'alias':request.model,
                                       'revision':revision,'cohort':self.config['tenants'][request.tenant]['cohort'],
                                       'admitted':row['created'],'kind':'live','source':'gateway'})
            except BaseException:
                self.requests.finish(row,{'state':'failed','error':'admission_failed'})
                raise
            row = self.requests.owned(request.tenant,row['id'])
            self.watchers[row['id']] = asyncio.create_task(self._watch(row,engine,ticket))
            return self.requests.public(row)

    async def _watch(self, row, engine, ticket):
        try:
            async for event in engine.stream(ticket):
                if event.kind == 'terminal':
                    status = engine.status(row['tenant'],ticket.request_id)
                    if self.requests.finish(row,status):
                        producer = self.config['telemetry'].get('local_gateway_producer')
                        if producer:
                            self.evidence.ingest(producer,[{'schema':2,'event_id':row['id']+'.terminal',
                                'update_seq':0,'request_id':row['id'],'finished_at':self.clock(),
                                'latency':max(0,self.clock()-row['created']),'latency_unit':'s',
                                'outcome':status['state'],'revision':row['revision']}])
        finally:
            self.watchers.pop(row['id'],None)

    async def stream(self, tenant, request_id, after=0):
        row = self.requests.owned(tenant,request_id)
        if row['engine_id'] is None:
            raise FleetError('admission_interrupted',status=409)
        engine = await self.engine_for(row)
        async for event in engine.stream(Ticket(row['engine_id'],tenant,row['revision']),after):
            data = dict(event.data)
            if event.kind == 'accepted':
                data.update(pool=row['pool'],worker=row['worker'],generation=row['generation'],route_epoch=row['route_epoch'])
            yield Event(request_id,event.seq,event.kind,data)

    async def cancel(self, tenant, request_id):
        row = self.requests.owned(tenant,request_id)
        if row['engine_id'] and row['state'] not in self.requests.TERMINAL:
            engine = await self.engine_for(row)
            await engine.cancel(tenant,row['engine_id'])
            watcher = self.watchers.get(request_id)
            if watcher:
                await watcher
        return self.status(tenant,request_id)

    def status(self, tenant, request_id):
        return self.requests.public(self.requests.owned(tenant,request_id))

    async def drain(self):
        self.accepting = False
        async with self.admission_lock:
            pass
        await asyncio.gather(*list(self.watchers.values()))
        await asyncio.gather(*(engine.drain() for engine in self.engines.values()))

    async def close(self):
        if self.closed:
            return
        self.accepting = False
        async with self.admission_lock:
            pass
        for row in self.requests.active():
            await self.cancel(row['tenant'],row['id'])
        await asyncio.gather(*list(self.watchers.values()))
        await asyncio.gather(*(engine.close() for engine in self.engines.values()))
        self.closed = True
