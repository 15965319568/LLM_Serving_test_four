"""Combined production socket: existing fleet control and numeric consumers."""
import asyncio
from mlserver.types import InferenceRequest
from ..errors import FleetError
from ..http import Application


class ProductionApplication(Application):
    def __init__(self,service,inbox):
        super().__init__(service.fleet)
        self.service,self.inbox=service,inbox

    async def control(self,method,path,value,scope):
        if method=='POST':
            if path=='/admin/numeric/install':return await self.service.install(**value)
            if path=='/admin/capture/receive':return self.inbox.receive(value['partition'],value['records'])
            if path=='/admin/capture/reconcile':return self.inbox.reconcile()
        if method=='GET':
            if path=='/admin/numeric':return dict(accounting=self.service.accounting(),workers=self.service.fabric.replicas.pids())
            if path=='/admin/capture':return dict(partitions=self.inbox.status(),audit=self.inbox.audit())
        return await super().control(method,path,value,scope)

    async def __call__(self,scope,receive,send):
        if scope['type']!='http' or not scope['path'].startswith('/v2/'):
            return await super().__call__(scope,receive,send)
        headers=dict(scope.get('headers',[]));path=scope['path'];method=scope['method']
        try:
            tenant=self.identity(headers)
            if path=='/v2/receipts' and method=='GET':
                return await self.json(send,200,{'requests':self.service.ledger.rows(tenant)})
            parts=path.strip('/').split('/')
            if len(parts)==3 and parts[:2]==['v2','requests']:
                if method=='GET':return await self.json(send,200,self.service.ledger.get(tenant,parts[2]))
                if method=='DELETE':return await self.json(send,200,await self.service.cancel(tenant,parts[2]))
            if len(parts)==4 and parts[:2]==['v2','models'] and parts[3]=='infer' and method=='POST':
                request=InferenceRequest.model_validate(await self.body(receive))
                key=headers.get(b'idempotency-key',b'').decode()
                task=asyncio.create_task(self.service.infer(tenant,parts[2],key,request))
                async def disconnected():
                    while (await receive())['type']!='http.disconnect':pass
                watcher=asyncio.create_task(disconnected())
                try:
                    await asyncio.wait([task,watcher],return_when=asyncio.FIRST_COMPLETED)
                    if task.done():
                        result=await task
                        return await self.json(send,200,result.model_dump(mode='json',exclude_none=True))
                finally:
                    for pending in (task,watcher):
                        if not pending.done():pending.cancel()
                    await asyncio.gather(task,watcher,return_exceptions=True)
                return
            raise FleetError('not_found',status=404)
        except FleetError as error:await self.json(send,error.status,error.wire())
        except (ValueError,TypeError,KeyError,UnicodeError):await self.json(send,400,{'error':{'code':'invalid_request'}})
