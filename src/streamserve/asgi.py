"""ASGI HTTP application shared by the built-in host and external ASGI servers."""
import asyncio
import json
from urllib.parse import parse_qs
from .auth import require_admin, tenant_for
from .config import ModelSpec
from .protocol import Request, ServiceError, Ticket
from .sse import frame


class Application:
    def __init__(self, engine):
        self.engine = engine

    async def _body(self, receive):
        body = bytearray()
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                raise ServiceError('disconnected', 'client disconnected', 499)
            body.extend(message.get('body', b''))
            if len(body) > self.engine.settings.max_body_bytes:
                raise ServiceError('body_too_large', 'request body is too large', 413)
            if not message.get('more_body', False):
                break
        try:
            result = json.loads(body)
        except (ValueError, UnicodeError):
            raise ServiceError('invalid_json', 'request body must be JSON') from None
        if not isinstance(result, dict):
            raise ServiceError('invalid_json', 'request body must be an object')
        return result

    async def _json(self, send, status, value):
        body = json.dumps(value, ensure_ascii=False).encode()
        await send({'type': 'http.response.start', 'status': status,
                    'headers': [(b'content-type', b'application/json'), (b'content-length', str(len(body)).encode())]})
        await send({'type': 'http.response.body', 'body': body})

    async def _stream(self, ticket, after, receive, send):
        source = self.engine.stream(ticket, after)
        first = await anext(source, None)
        await send({'type': 'http.response.start', 'status': 200,
                    'headers': [(b'content-type', b'text/event-stream'), (b'cache-control', b'no-cache')]})

        async def write():
            try:
                if first is not None:
                    await send({'type': 'http.response.body', 'body': frame(first), 'more_body': True})
                async for event in source:
                    await send({'type': 'http.response.body', 'body': frame(event), 'more_body': True})
                await send({'type': 'http.response.body', 'body': b''})
            finally:
                await source.aclose()

        async def disconnect():
            while (await receive())['type'] != 'http.disconnect':
                pass

        writer = asyncio.create_task(write())
        watcher = asyncio.create_task(disconnect())
        try:
            done, _ = await asyncio.wait((writer, watcher), return_when=asyncio.FIRST_COMPLETED)
            if writer in done:
                await writer
        finally:
            writer.cancel()
            watcher.cancel()
            await asyncio.gather(writer, watcher, return_exceptions=True)

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                event = await receive()
                if event['type'] == 'lifespan.startup':
                    await self.engine.start()
                    await send({'type': 'lifespan.startup.complete'})
                elif event['type'] == 'lifespan.shutdown':
                    await self.engine.close()
                    await send({'type': 'lifespan.shutdown.complete'})
                    return
        if scope['type'] != 'http':
            return
        headers = {k.decode().lower(): v.decode() for k, v in scope.get('headers', [])}
        method, path = scope['method'], scope['path']
        try:
            if method == 'GET' and path == '/health':
                return await self._json(send, 200, {'ready': self.engine.accepting})
            if path.startswith('/admin/'):
                require_admin(self.engine.settings, headers)
                if method == 'POST' and path == '/admin/drain':
                    return await self._json(send, 200, await self.engine.drain())
                if method == 'POST' and path == '/admin/activate':
                    value = await self._body(receive)
                    if set(value) != {'alias', 'spec', 'expected_epoch', 'operation_id'} or not isinstance(value['spec'], dict):
                        raise ServiceError('invalid_activation', 'invalid activation fields')
                    try:
                        spec = ModelSpec(**value['spec'])
                        result = await self.engine.activate(value['alias'], spec, value['expected_epoch'], value['operation_id'])
                    except (TypeError, AttributeError):
                        raise ServiceError('invalid_activation', 'invalid activation parameters') from None
                    return await self._json(send, 200, result)
                raise ServiceError('not_found', 'route not found', 404)
            if method == 'GET' and path == '/metrics':
                require_admin(self.engine.settings, headers)
                await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain; version=0.0.4')]})
                return await send({'type': 'http.response.body', 'body': self.engine.prometheus().encode()})
            tenant = tenant_for(self.engine.settings, headers)
            if method == 'GET' and path == '/v1/models':
                return await self._json(send, 200, {'data': self.engine.catalog.list()})
            if method == 'POST' and path == '/v1/generate':
                value = await self._body(receive)
                allowed = {'model', 'prompt', 'max_tokens', 'stop', 'stream'}
                if set(value) - allowed:
                    raise ServiceError('invalid_request', 'unknown request fields')
                request = Request(tenant, headers.get('idempotency-key', ''), value.get('model', ''),
                                  value.get('prompt'), value.get('max_tokens', 8), value.get('stop', ()))
                ticket = await self.engine.submit(request)
                if value.get('stream', False):
                    return await self._stream(ticket, 0, receive, send)
                return await self._json(send, 202, {'request_id': ticket.request_id, 'revision': ticket.revision})
            parts = path.strip('/').split('/')
            if len(parts) in (3, 4) and parts[:2] == ['v1', 'requests']:
                request_id = parts[2]
                status = self.engine.status(tenant, request_id)
                if method == 'GET' and len(parts) == 3:
                    return await self._json(send, 200, status)
                if method == 'DELETE' and len(parts) == 3:
                    return await self._json(send, 200, await self.engine.cancel(tenant, request_id))
                if method == 'GET' and parts[-1] == 'events':
                    query = parse_qs(scope.get('query_string', b'').decode())
                    cursor = headers.get('last-event-id', query.get('after', ['0'])[0])
                    try:
                        after = int(cursor)
                    except ValueError:
                        raise ServiceError('invalid_cursor', 'event cursor must be an integer') from None
                    return await self._stream(Ticket(request_id, tenant, status['revision']), after, receive, send)
            raise ServiceError('not_found', 'route not found', 404)
        except ServiceError as error:
            await self._json(send, error.status, error.wire())
