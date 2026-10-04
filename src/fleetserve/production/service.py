"""Regional numeric admission, actual execution and release evidence share identity."""
import asyncio
from copy import deepcopy
import time
import uuid
from mlserver.batching.compatibility import validate_request
from mlserver.types import InferenceResponse,Parameters
from ..errors import FleetError
from ..util import digest,identifier
from .ledger import AdmissionLedger


class ProductionService:
    def __init__(self,fleet,fabric):
        self.fleet,self.fabric=fleet,fabric
        self.ledger=AdmissionLedger(fleet.state_dir/'numeric-admissions.sqlite3')
        self.tasks={}
        self.accepting=False
        self.closed=False
        self.lock=asyncio.Lock()
        self.fleet.leases.reserved_extra=self.ledger.reserved

    @staticmethod
    def execution_alias(worker):
        return 'execution.'+digest([worker['worker'],worker['revision']])[:40]

    async def start(self):
        await self.fleet.start()
        await self.fabric.start()
        self.ledger.recover(self.fleet.clock())
        self.sync_evidence()
        self.accepting=True
        return self

    async def install(self,worker,generation,factor,operation_id,expected_epoch=0,**options):
        lease=self.fleet.leases.get(worker)
        if lease['generation']!=generation: raise FleetError('worker_generation',status=409)
        return await self.fabric.deploy(self.execution_alias(lease),lease['revision'],factor,
                                       operation_id,expected_epoch,**options)

    def _receipt(self,row):
        return dict(request_id=row['id'],tenant=row['tenant'],alias=row['alias'],revision=row['revision'],
                    cohort=row['cohort'],admitted=row['admitted'],kind='live',source='gateway')

    def sync_evidence(self):
        for row in self.ledger.rows():
            self.fleet.evidence.receipt(self._receipt(row))
        return len(self.ledger.rows())

    async def submit(self,tenant,alias,key,request):
        identifier(tenant,'tenant');identifier(alias,'alias');identifier(key,'key')
        if tenant not in self.fleet.config['tenants']: raise FleetError('unknown_tenant',status=403)
        if alias not in self.fleet.config['aliases']: raise FleetError('unknown_alias',status=404)
        request=request.model_copy(deep=True)
        rows=validate_request(request)
        supplied=getattr(request.parameters,'tenant',tenant) if request.parameters else tenant
        if supplied!=tenant: raise FleetError('tenant_conflict',status=403)
        fingerprint=digest([alias,request.model_dump(mode='json')])
        async with self.lock:
            old=self.ledger.replay(tenant,key,fingerprint)
            if old is not None: return old
            if not self.accepting: raise FleetError('draining',status=503)
            route=self.fleet.releases.route(alias)
            revision,worker=self.fleet.planner.place(route['plan'],tenant,key)
            execution=self.execution_alias(worker)
            deployment=self.fabric.deployment(execution)
            if deployment['spec']['revision']!=revision: raise FleetError('execution_revision',status=409)
            row=dict(id=uuid.uuid4().hex,tenant=tenant,key=key,external_id=request.id,alias=alias,
                     revision=revision,route_epoch=route['epoch'],worker=worker['worker'],pool=worker['pool'],
                     worker_generation=worker['generation'],execution_alias=execution,
                     cohort=self.fleet.config['tenants'][tenant]['cohort'],admitted=self.fleet.clock(),rows=rows)
            self.ledger.add(row,fingerprint)
            self.fleet.evidence.receipt(self._receipt(row))
            task=asyncio.create_task(self._execute(row,request))
            self.tasks[row['id']]=task
            task.add_done_callback(lambda done,identity=row['id']:self.tasks.pop(identity,None))
            # Returning an active ticket establishes physical admission. A failed
            # admission is durable as a terminal ticket, never silently retried.
            while not task.done() and not any(r['id']==row['id'] for r in self.fabric.journal.receipts(tenant)):
                await asyncio.sleep(0)
            return self.ledger.get(tenant,row['id'])

    async def _execute(self,row,request):
        started=time.monotonic()
        try:
            response=await self.fabric.infer(row['tenant'],row['execution_alias'],request,admission_id=row['id'])
            response=response.model_copy(deep=True)
            response.model_name=row['alias'];response.model_version=row['revision']
            params=response.parameters.model_dump() if response.parameters else {}
            response.parameters=Parameters(**dict(params,admission_id=row['id'],route_epoch=row['route_epoch'],
                 worker=row['worker'],worker_generation=row['worker_generation']))
            self.ledger.finish(row['id'],'completed',self.fleet.clock(),(time.monotonic()-started)*1000,
                               response.model_dump(mode='json'))
        except asyncio.CancelledError:
            self.ledger.finish(row['id'],'cancelled',self.fleet.clock(),(time.monotonic()-started)*1000,error='cancelled')
        except Exception as error:
            self.ledger.finish(row['id'],'failed',self.fleet.clock(),(time.monotonic()-started)*1000,
                               error=getattr(error,'code',type(error).__name__))

    async def wait(self,tenant,identity):
        self.ledger.get(tenant,identity)
        task=self.tasks.get(identity)
        if task: await task
        row=self.ledger.get(tenant,identity)
        if row['state']!='completed': raise FleetError('request_'+row['state'],row['error'] or '',409)
        return InferenceResponse.model_validate(row['response'])

    async def infer(self,tenant,alias,key,request):
        row=await self.submit(tenant,alias,key,request)
        return await self.wait(tenant,row['id'])

    async def cancel(self,tenant,identity):
        row=self.ledger.get(tenant,identity)
        task=self.tasks.get(identity)
        if task:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        return self.ledger.get(tenant,identity)

    def terminal_records(self,tenant=None):
        result=[]
        for row in self.ledger.rows(tenant):
            if row['state']=='active': continue
            result.append(dict(schema=2,event_id='terminal.'+row['id'],update_seq=0,request_id=row['id'],
                finished_at=row['finished'],latency=row['latency_ms'],latency_unit='ms',outcome=row['state'],revision=self.fleet.edge.get(row['alias'])['plan']['stable']))
        return result

    def accounting(self):
        groups={}
        for row in self.ledger.rows():
            key=(row['tenant'],row['alias'],row['revision'])
            value=groups.setdefault(key,dict(tenant=key[0],alias=key[1],revision=key[2],admitted=0,
                active=0,completed=0,cancelled=0,failed=0,completed_rows=0))
            value['admitted']+=1;value[row['state']]+=1
            if row['state']=='completed': value['completed_rows']+=row['rows']
        return [groups[key] for key in sorted(groups)]

    async def close(self):
        if self.closed:return
        self.accepting=False
        async with self.lock:
            for task in list(self.tasks.values()):task.cancel()
            await asyncio.gather(*list(self.tasks.values()),return_exceptions=True)
        await self.fabric.close()
        await self.fleet.close()
        self.ledger.close();self.closed=True
