"""Numeric-serving deployment controller used by the regional pilot."""
import asyncio
from copy import deepcopy
from pathlib import Path
import uuid
from mlserver.batching.compatibility import validate_request
from mlserver.types import Parameters
from ..errors import FleetError
from ..util import digest,identifier,integer,number
from .journal import FabricJournal
from .replicas import ReplicaSet


class ServingFabric:
    def __init__(self,state_dir,workers=2):
        self.state_dir=Path(state_dir)
        self.journal=FabricJournal(self.state_dir/'fabric.sqlite3')
        self.replicas=ReplicaSet(workers)
        self.current={}
        self.tasks=set()
        self.mutation=asyncio.Lock()
        self.accepting=False
        self.closed=False

    async def start(self):
        await self.replicas.start()
        self.journal.recover()
        for row in self.journal.deployments():
            await self.replicas.load(row['alias'],row['generation'],row['spec'])
            self.current[row['alias']]=row
        self.accepting=True
        return self

    def deployment(self,alias):
        if alias not in self.current: raise FleetError('unknown_alias',alias,404)
        return deepcopy(self.current[alias])

    async def deploy(self,alias,revision,factor,operation_id,expected_epoch=0,
                     max_batch_size=4,max_batch_time=.02,runtime_options=None,after_prepare=None):
        identifier(alias,'alias');identifier(revision,'revision');identifier(operation_id,'operation_id')
        integer(expected_epoch,'expected_epoch');integer(max_batch_size,'max_batch_size',1)
        number(max_batch_time,'max_batch_time',.001)
        spec=dict(revision=revision,factor=factor,max_batch_size=max_batch_size,
                  max_batch_time=max_batch_time,runtime_options=deepcopy(runtime_options or {}))
        fingerprint=digest([alias,revision])
        async with self.mutation:
            if not self.accepting: raise FleetError('draining',status=503)
            old=self.current.get(alias)
            if (old['epoch'] if old else 0)!=expected_epoch: raise FleetError('epoch_conflict',status=409)
            previous=self.journal.operation(operation_id,fingerprint)
            if previous is not None: return previous
            deployment=dict(alias=alias,epoch=expected_epoch+1,generation=revision,spec=spec)
            result=self.journal.commit(deployment,operation_id,fingerprint)
            self.current[alias]=deployment
            await self.replicas.load(alias,revision,spec)
            if after_prepare: await after_prepare(deepcopy(deployment))
            if old and old['generation']!=revision:
                await self.replicas.unload(alias,old['generation'])
            return result

    async def infer(self,tenant,alias,request,*,admission_id=None):
        identifier(tenant,'tenant')
        if not self.accepting: raise FleetError('draining',status=503)
        rows=validate_request(request)
        deployment=self.deployment(alias)
        request=request.model_copy(deep=True)
        params=request.parameters.model_dump() if request.parameters else {}
        if 'tenant' in params and params['tenant']!=tenant: raise FleetError('tenant_conflict',status=403)
        request.parameters=Parameters(**dict(params,tenant=tenant))
        identity=identifier(admission_id,'admission_id') if admission_id is not None else uuid.uuid4().hex
        self.journal.admit(identity,tenant,deployment,request,rows)
        task=asyncio.create_task(self.replicas.infer(alias,deployment['generation'],request))
        self.tasks.add(task)
        try:
            response=await task
            active=self.deployment(alias)
            self.journal.db.execute('UPDATE requests SET epoch=?,revision=?,generation=? WHERE id=?',
                (active['epoch'],active['spec']['revision'],active['generation'],identity))
            self.journal.finish(identity,'completed',response)
            response=response.model_copy(deep=True)
            response.model_name=alias;response.model_version=active['spec']['revision']
            params=response.parameters.model_dump() if response.parameters else {}
            response.parameters=Parameters(**dict(params,epoch=active['epoch'],receipt_id=identity))
            return response
        except asyncio.CancelledError:
            self.journal.finish(identity,'cancelled',error='client_cancelled');raise
        except Exception as error:
            self.journal.finish(identity,'failed',error=type(error).__name__);raise
        finally: self.tasks.discard(task)

    async def flush(self,alias):
        row=self.deployment(alias);await self.replicas.flush(alias,row['generation'])

    async def batching(self,alias):
        row=self.deployment(alias);return await self.replicas.batching(alias,row['generation'])

    async def drain(self):
        self.accepting=False
        while self.tasks:
            await asyncio.gather(*list(self.tasks),return_exceptions=True)
            await asyncio.sleep(0)

    async def close(self):
        if self.closed: return
        await self.drain()
        async with self.mutation:
            await self.replicas.close();self.journal.close();self.closed=True
