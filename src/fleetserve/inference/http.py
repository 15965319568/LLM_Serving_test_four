"""Authenticated V2 inference and online administration over the same socket."""
import asyncio
from types import SimpleNamespace
from mlserver.types import InferenceRequest
from ..http import Application
from ..errors import FleetError


class FabricApplication(Application):
    def __init__(self,fabric,config):
        super().__init__(SimpleNamespace(config=config))
        self.fabric=fabric

    async def __call__(self,scope,receive,send):
        if scope['type']!='http':
            return
        method,path=scope['method'],scope['path']
        headers=dict(scope.get('headers',[]))
        try:
            if path=='/health' and method=='GET':
                return await self.json(send,200,{'ready':self.fabric.accepting})
            if path.startswith('/admin/'):
                self.identity(headers,admin=True)
                if path=='/admin/fabric' and method=='GET':
                    return await self.json(send,200,{'deployments':list(self.fabric.current.values()),
                        'workers':self.fabric.replicas.pids(),'accounting':self.fabric.journal.accounting(),
                        'recovery_errors':self.fabric.replicas.recovery_errors})
                if path=='/admin/deploy' and method=='POST':
                    result=await self.fabric.deploy(**await self.body(receive))
                    return await self.json(send,200,result)
                if path=='/admin/flush' and method=='POST':
                    value=await self.body(receive)
                    await self.fabric.flush(value['alias'])
                    return await self.json(send,200,{'flushed':True})
                raise FleetError('not_found',status=404)
            tenant=self.identity(headers)
            if path=='/v2/receipts' and method=='GET':
                return await self.json(send,200,{'requests':self.fabric.journal.receipts(tenant)})
            parts=path.strip('/').split('/')
            if method=='POST' and len(parts)==4 and parts[:2]==['v2','models'] and parts[3]=='infer':
                request=InferenceRequest.model_validate(await self.body(receive))
                async def disconnected():
                    while (await receive())['type']!='http.disconnect':
                        pass
                work=asyncio.create_task(self.fabric.infer(tenant,parts[2],request))
                disconnect=asyncio.create_task(disconnected())
                try:
                    await asyncio.wait([work,disconnect],return_when=asyncio.FIRST_COMPLETED)
                    if not work.done():
                        return
                    response=await work
                    return await self.json(send,200,response.model_dump(mode='json',exclude_none=True))
                finally:
                    for task in (work,disconnect):
                        if not task.done(): task.cancel()
                    await asyncio.gather(work,disconnect,return_exceptions=True)
            raise FleetError('not_found',status=404)
        except FleetError as error:
            await self.json(send,error.status,error.wire())
        except (KeyError,TypeError,ValueError):
            await self.json(send,400,{'error':{'code':'invalid_request'}})
        except Exception:
            await self.json(send,503,{'error':{'code':'inference_failed'}})
