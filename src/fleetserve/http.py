"""Authenticated JSON control API and tenant SSE API on the same ASGI host."""
import asyncio
import json
from types import SimpleNamespace
from urllib.parse import parse_qs
from streamserve import Request
from streamserve.protocol import ServiceError
from streamserve.sse import frame
from .errors import FleetError
from .operations.reconciliation import accounting, audit_page, metrics, route_consistency


class Application:
    def __init__(self,fleet):
        self.fleet = fleet
        self.engine = SimpleNamespace(settings=SimpleNamespace(max_body_bytes=1048576))

    def identity(self,headers,admin=False):
        authorization = headers.get(b'authorization',b'').decode('utf-8')
        if not authorization.startswith('Bearer '):
            raise FleetError('unauthorized',status=401)
        token = authorization[7:]
        if token == self.fleet.config['admin_token']:
            if admin:
                return None
            raise FleetError('tenant_required',status=403)
        tenant = next((name for name,value in self.fleet.config['tenants'].items() if value['token']==token),None)
        if tenant is None:
            raise FleetError('unauthorized',status=401)
        if admin:
            raise FleetError('forbidden',status=403)
        return tenant

    async def body(self,receive):
        chunks = []
        size = 0
        while True:
            event = await receive()
            if event['type'] == 'http.disconnect':
                raise FleetError('disconnected')
            chunk = event.get('body',b'')
            size += len(chunk)
            if size > self.engine.settings.max_body_bytes:
                raise FleetError('body_too_large',status=413)
            chunks.append(chunk)
            if not event.get('more_body',False):
                break
        raw = b''.join(chunks)
        try:
            value = json.loads(raw) if raw else {}
        except (ValueError,UnicodeError):
            raise FleetError('invalid_json') from None
        if not isinstance(value,dict):
            raise FleetError('invalid_json')
        return value

    async def json(self,send,status,payload):
        raw = json.dumps(payload,ensure_ascii=False,allow_nan=False).encode()
        await send({'type':'http.response.start','status':status,
                    'headers':[(b'content-type',b'application/json'),(b'content-length',str(len(raw)).encode())]})
        await send({'type':'http.response.body','body':raw})

    async def sse(self,tenant,request_id,after,receive,send):
        source = self.fleet.gateway.stream(tenant,request_id,after)
        # Validate ownership and cursor before starting the HTTP response.
        try:
            first = await anext(source)
        except StopAsyncIteration:
            first = None
        await send({'type':'http.response.start','status':200,'headers':[(b'content-type',b'text/event-stream')]})
        async def transmit():
            if first:
                await send({'type':'http.response.body','body':frame(first),'more_body':True})
            async for item in source:
                await send({'type':'http.response.body','body':frame(item),'more_body':True})
            await send({'type':'http.response.body','body':b''})
        async def disconnected():
            while (await receive())['type'] != 'http.disconnect':
                pass
        sending,watching = asyncio.create_task(transmit()),asyncio.create_task(disconnected())
        try:
            await asyncio.wait([sending,watching],return_when=asyncio.FIRST_COMPLETED)
            if sending.done():
                await sending
        finally:
            for task in [sending,watching]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(sending,watching,return_exceptions=True)
            await source.aclose()

    async def __call__(self,scope,receive,send):
        if scope['type'] != 'http':
            return
        path,method = scope['path'],scope['method']
        headers = dict(scope.get('headers',[]))
        try:
            if path == '/health' and method == 'GET':
                return await self.json(send,200,{'ready':self.fleet.gateway.accepting})
            if path.startswith('/admin/') or path == '/metrics':
                self.identity(headers,admin=True)
                if path == '/metrics' and method == 'GET':
                    payload = metrics(self.fleet).encode()
                    await send({'type':'http.response.start','status':200,'headers':[(b'content-type',b'text/plain'),(b'content-length',str(len(payload)).encode())]})
                    return await send({'type':'http.response.body','body':payload})
                value = await self.body(receive) if method == 'POST' else {}
                result = await self.control(method,path,value,scope)
                return await self.json(send,200,result)
            tenant = self.identity(headers)
            if path == '/v1/generate' and method == 'POST':
                value = await self.body(receive)
                if 'tenant' in value:
                    raise FleetError('invalid_request','tenant comes from credentials')
                key = headers.get(b'idempotency-key',b'').decode()
                request = Request(tenant,key,value['model'],value['prompt'],value.get('max_tokens',8),tuple(value.get('stop',[])))
                result = await self.fleet.gateway.submit(request)
                if value.get('stream',False):
                    return await self.sse(tenant,result['id'],0,receive,send)
                return await self.json(send,202,result)
            parts = path.strip('/').split('/')
            if len(parts) in (3,4) and parts[:2] == ['v1','requests']:
                if method == 'GET' and len(parts)==3:
                    return await self.json(send,200,self.fleet.gateway.status(tenant,parts[2]))
                if method == 'DELETE' and len(parts)==3:
                    return await self.json(send,200,await self.fleet.gateway.cancel(tenant,parts[2]))
                if method == 'GET' and parts[-1]=='events':
                    after = int(headers.get(b'last-event-id',b'0'))
                    return await self.sse(tenant,parts[2],after,receive,send)
            raise FleetError('not_found',status=404)
        except (FleetError,ServiceError) as error:
            await self.json(send,error.status,error.wire())
        except (KeyError,ValueError,TypeError,UnicodeError):
            await self.json(send,400,{'error':{'code':'invalid_request','message':'invalid fields'}})

    async def control(self,method,path,value,scope):
        fleet = self.fleet
        if method=='GET':
            if path=='/admin/state': return fleet.snapshot()
            if path=='/admin/accounting': return accounting(fleet.store)
            if path=='/admin/consistency': return route_consistency(fleet)
            if path=='/admin/audit':
                query = parse_qs(scope.get('query_string',b'').decode())
                return audit_page(fleet.store,int(query.get('after',['0'])[0]),int(query.get('limit',['100'])[0]))
        if method=='POST':
            if path=='/admin/releases': return fleet.releases.begin(**value)
            if path=='/admin/evaluate': return fleet.assessor.evaluate(**value)
            if path=='/admin/apply': return fleet.releases.apply(**value)
            if path=='/admin/rollback': return fleet.releases.rollback(**value)
            if path=='/admin/reconcile': return {'effects':fleet.outbox.reconcile(value.get('limit',100))}
            if path=='/admin/workers': return fleet.leases.register(**value)
            if path=='/admin/heartbeat': return fleet.leases.heartbeat(**value)
            if path=='/admin/worker-drain': return fleet.leases.drain(**value)
            if path=='/admin/telemetry': return fleet.evidence.ingest(value['producer'],value['records'])
            if path=='/admin/receipts': return {'inserted':sum(fleet.evidence.receipt(row) for row in value['records'])}
            if path=='/admin/watermark': return {'advanced':fleet.evidence.watermark(**value)}
            if path=='/admin/drain':
                await fleet.gateway.drain()
                return {'drained':True}
        raise FleetError('not_found',status=404)
