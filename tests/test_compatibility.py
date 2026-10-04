"""Public regressions for the existing, single-deployment service."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from streamserve import Engine, ModelSpec, Request, ServiceError, Settings, TenantSpec
from streamserve.asgi import Application
from streamserve.backend import EchoBackend
from streamserve.cache import PrefixCache
from streamserve.sampling import StopFilter
from streamserve.scheduler import Scheduler
from streamserve.sse import Decoder, frame
from streamserve.protocol import Event, WorkerEvent
from streamserve.transport import HTTPServer
from streamserve.rpc_backend import ProcessBackend


async def collect(engine, ticket, after=0):
    return [event async for event in engine.stream(ticket, after)]


class ScriptBackend(EchoBackend):
    async def generate(self, work):
        for index, text in enumerate(['hello ', '世', '界<', 'end', '>ignored']):
            yield WorkerEvent(work.attempt, index, text)
        yield WorkerEvent(work.attempt, 5, finish_reason='stop')


class EngineCompatibility(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend = EchoBackend()
        self.engine = Engine(Settings(), self.backend)
        await self.engine.start()

    async def asyncTearDown(self):
        await self.engine.close()

    async def test_complete_and_accounting(self):
        ticket = await self.engine.submit(Request('demo', 'one', 'chat', '汉字', 3))
        events = await collect(self.engine, ticket)
        self.assertEqual([e.seq for e in events], list(range(1, 6)))
        self.assertEqual(events[-1].data['usage'], {'prompt_tokens': 2, 'completion_tokens': 3})
        self.assertEqual(events[-1].data['state'], 'completed')
        self.assertEqual(self.engine.snapshot()['scheduler']['kv_used'], 0)
        self.assertEqual(self.engine.snapshot()['cache']['pinned'], 0)

    async def test_idempotent_live_and_completed(self):
        request = Request('demo', 'same', 'chat', 'hello', 2)
        first, second = await asyncio.gather(self.engine.submit(request), self.engine.submit(request))
        self.assertEqual(first, second)
        original = await collect(self.engine, first)
        third = await self.engine.submit(request)
        self.assertEqual(original, await collect(self.engine, third))
        self.assertEqual(len(self.backend.calls), 1)

    async def test_conflict(self):
        await self.engine.submit(Request('demo', 'same', 'chat', 'a'))
        with self.assertRaises(ServiceError) as context:
            await self.engine.submit(Request('demo', 'same', 'chat', 'b'))
        self.assertEqual(context.exception.status, 409)

    async def test_queue_cancellation(self):
        self.backend.delay = .01
        tickets = [await self.engine.submit(Request('demo', str(i), 'chat', 'x', 8)) for i in range(4)]
        await self.engine.cancel('demo', tickets[-1].request_id)
        events = await collect(self.engine, tickets[-1])
        self.assertEqual(events[-1].data['state'], 'cancelled')
        self.assertEqual(events[-1].data['usage']['completion_tokens'], 0)

    async def test_replay_suffix(self):
        ticket = await self.engine.submit(Request('demo', 'replay', 'chat', 'hello', 2))
        original = await collect(self.engine, ticket)
        self.assertEqual(await collect(self.engine, ticket, 2), original[2:])

    async def test_tenant_isolation(self):
        ticket = await self.engine.submit(Request('demo', 'private', 'chat', 'secret', 2))
        with self.assertRaises(ServiceError) as context:
            self.engine.status('other', ticket.request_id)
        self.assertEqual(context.exception.status, 404)

    async def test_invalid_request_no_admission(self):
        for request in [Request('demo', '', 'chat', 'x'), Request('demo', 'bad', 'chat', 'x', True),
                        Request('demo', 'long', 'chat', 'x' * 1024, 2)]:
            with self.assertRaises(ServiceError):
                await self.engine.submit(request)
        self.assertEqual(self.engine.snapshot()['scheduler']['queued'], 0)

    async def test_metrics_no_prompt_or_key(self):
        ticket = await self.engine.submit(Request('demo', 'private-key', 'chat', 'private-prompt', 2))
        await collect(self.engine, ticket)
        metrics = self.engine.prometheus()
        self.assertIn('streamserve_terminal_total{model="chat",outcome="completed"} 1', metrics)
        for secret in ('private-key', 'private-prompt', ticket.request_id, 'demo-token'):
            self.assertNotIn(secret, metrics)


class StopCompatibility(unittest.IsolatedAsyncioTestCase):
    async def test_stop_and_generated_token_usage(self):
        engine = Engine(Settings(), ScriptBackend())
        await engine.start()
        try:
            ticket = await engine.submit(Request('demo', 'stop', 'chat', 'p', 8, ('<end>',)))
            events = await collect(engine, ticket)
            self.assertEqual(''.join(e.data['text'] for e in events if e.kind == 'delta'), 'hello 世界')
            self.assertEqual(events[-1].data['reason'], 'stop')
            self.assertEqual(events[-1].data['usage']['completion_tokens'], 5)
        finally:
            await engine.close()


class ComponentCompatibility(unittest.TestCase):
    def test_partial_stop_flush(self):
        stop = StopFilter(['abc'])
        self.assertEqual(stop.feed('hello ab'), 'hello ')
        self.assertEqual(stop.finish(), 'ab')

    def test_earliest_stop(self):
        stop = StopFilter(['end', 'x'])
        self.assertEqual(stop.feed('hi x later end'), 'hi ')
        self.assertTrue(stop.stopped)

    def test_prefix_pins_and_tenant_scope(self):
        cache = PrefixCache(4)
        a = cache.acquire('a', 'r1', 'unicode-v1', (1, 2))
        b = cache.acquire('a', 'r1', 'unicode-v1', (1, 2))
        self.assertEqual(a, b)
        other = cache.acquire('b', 'r1', 'unicode-v1', (1, 2))
        self.assertNotEqual(a, other)
        self.assertIsNone(cache.acquire('c', 'r1', 'unicode-v1', (3,)))
        cache.release(a); cache.release(b); cache.release(other)
        self.assertIsNotNone(cache.acquire('c', 'r1', 'unicode-v1', (3,)))
        self.assertEqual(cache.snapshot()['hits'], 1)

    def test_revision_cache_isolation(self):
        cache = PrefixCache(9)
        a = cache.acquire('a', 'r1', 'unicode-v1', (1,))
        b = cache.acquire('a', 'r2', 'unicode-v1', (1,))
        self.assertNotEqual(a, b)
        cache.release(a); cache.retire('r1')
        self.assertEqual(cache.snapshot()['revisions'], ['r2'])

    def test_scheduler_capacity_and_progress(self):
        config = Settings(tenants={'a': TenantSpec('a', 1, 4), 'b': TenantSpec('b', 1, 4)}, kv_tokens=10)
        scheduler = Scheduler(config)
        scheduler.enqueue('a1', 'a', 8); scheduler.enqueue('a2', 'a', 3); scheduler.enqueue('b1', 'b', 2)
        self.assertEqual(scheduler.take(), 'a1')
        self.assertEqual(scheduler.take(), 'b1')
        self.assertIsNone(scheduler.take())
        scheduler.release('a1')
        self.assertEqual(scheduler.take(), 'a2')

    def test_sse_every_byte_boundary(self):
        payload = frame(Event('r', 1, 'delta', {'text': '汉🙂字\nmore'}))
        decoder = Decoder()
        events = []
        for byte in payload:
            events.extend(decoder.feed(bytes([byte])))
        self.assertEqual(len(events), 1)
        self.assertEqual(json.loads(events[0]['data']), {'text': '汉🙂字\nmore'})


class WireCompatibility(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = Engine(Settings(), EchoBackend())
        await self.engine.start()
        self.server = await HTTPServer(Application(self.engine)).start()

    async def asyncTearDown(self):
        await self.server.close()
        await self.engine.close()

    async def wire(self, method, path, body=None, token='demo-token', extra=''):
        reader, writer = await asyncio.open_connection('127.0.0.1', self.server.port)
        data = json.dumps(body).encode() if body is not None else b''
        writer.write((f'{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Length: {len(data)}\r\n{extra}\r\n').encode() + data)
        await writer.drain()
        response = await reader.read()
        writer.close(); await writer.wait_closed()
        head, payload = response.split(b'\r\n\r\n', 1)
        if b'Transfer-Encoding: chunked' in head:
            chunks = []
            while True:
                size, payload = payload.split(b'\r\n', 1)
                count = int(size, 16)
                if count == 0: break
                chunks.append(payload[:count]); payload = payload[count+2:]
            payload = b''.join(chunks)
        return int(head.split(b' ')[1]), payload

    async def test_health_and_auth(self):
        self.assertEqual((await self.wire('GET', '/health'))[0], 200)
        self.assertEqual((await self.wire('GET', '/v1/models', token='wrong'))[0], 401)
        self.assertEqual((await self.wire('GET', '/metrics'))[0], 403)

    async def test_post_real_sse(self):
        status, data = await self.wire('POST', '/v1/generate', {'model': 'chat', 'prompt': 'hi', 'max_tokens': 2, 'stream': True}, extra='Idempotency-Key: wire\r\n')
        self.assertEqual(status, 200)
        events = Decoder().feed(data)
        self.assertEqual([e['event'] for e in events], ['accepted', 'delta', 'delta', 'terminal'])

    async def test_request_status_and_conflict(self):
        status, data = await self.wire('POST', '/v1/generate', {'model': 'chat', 'prompt': 'hi'}, extra='Idempotency-Key: key\r\n')
        self.assertEqual(status, 202)
        request_id = json.loads(data)['request_id']
        self.assertEqual((await self.wire('GET', '/v1/requests/'+request_id))[0], 200)
        self.assertEqual((await self.wire('POST', '/v1/generate', {'model': 'chat', 'prompt': 'changed'}, extra='Idempotency-Key: key\r\n'))[0], 409)


@unittest.skipIf(__import__('os').name == 'nt', 'worker uses POSIX stdin pipes; run in WSL/Linux')
class ProcessCompatibility(unittest.IsolatedAsyncioTestCase):
    async def test_multiplexed_worker(self):
        backend = ProcessBackend()
        engine = Engine(Settings(), backend)
        await engine.start()
        try:
            tickets = [await engine.submit(Request('demo', str(i), 'chat', 'p', 4)) for i in range(4)]
            runs = await asyncio.gather(*(collect(engine, ticket) for ticket in tickets))
            self.assertTrue(all(run[-1].data['state'] == 'completed' for run in runs))
            self.assertEqual(len(backend.workers), 1)
        finally:
            await engine.close()
        self.assertFalse(backend.workers)


if __name__ == '__main__':
    unittest.main()
