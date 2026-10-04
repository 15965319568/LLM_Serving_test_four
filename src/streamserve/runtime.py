"""Coordinator for deployment leases, durable streams and request finalization."""
import asyncio
from dataclasses import dataclass, field
import json
import time
import uuid
from .cache import PrefixCache
from .catalog import Catalog
from .deployment import DeploymentManager
from .durable import DurableJournal
from .metrics import Metrics
from .protocol import BackendFault, ServiceError, Ticket, Work
from .sampling import StopFilter
from .scheduler import Scheduler
from .tokenizer import encode


@dataclass
class LiveRequest:
    request: object
    spec: object
    tokens: tuple
    submitted: float
    cache_lease: object = None
    cleaning: bool = False
    cleaned: asyncio.Event = field(default_factory=asyncio.Event)


class Engine:
    def __init__(self, settings, backend, journal_path=':memory:'):
        settings.validate()
        self.settings, self.backend = settings, backend
        self.journal = DurableJournal(journal_path, settings)
        self.catalog = Catalog(settings.models)
        self.scheduler = Scheduler(settings)
        self.cache = PrefixCache(settings.cache_tokens)
        self.metrics = Metrics()
        self.requests, self.tasks = {}, {}
        self.changed = asyncio.Condition()
        self.wakeup = asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.started = self.closed = self.accepting = False
        self.pump = self.close_task = None
        self.control = DeploymentManager(self.journal, self.catalog, backend, self.cache, lambda: self.accepting)

    async def start(self):
        if self.started:
            return
        if self.closed:
            raise ServiceError('unavailable', 'service is closed', 503)
        try:
            for row in self.journal.all():
                if row['state'] in ('queued', 'running'):
                    self.journal.terminal(row['id'], 'failed', 'server_restart')
            for row in self.journal.all():
                alias = json.loads(row['request_json'])['model']
                self.metrics.count('requests_total', alias, 'accepted')
                self.metrics.count('terminal_total', alias, row['state'])
                self.metrics.count('tokens_total', alias, 'generated', row['output_tokens'])
                if row['ttft'] is not None:
                    self.metrics.observe(alias, row['ttft'])
            await self.control.start()
        except BaseException:
            self.journal.close()
            self.closed = True
            raise
        self.started = self.accepting = True
        self.pump = asyncio.create_task(self._pump())

    def _owned(self, tenant, request_id):
        row = self.journal.get(request_id)
        if row is None or row['tenant'] != tenant:
            raise ServiceError('not_found', 'request not found', 404)
        return row

    async def submit(self, request):
        request.validate()
        if not self.started or self.closed:
            raise ServiceError('unavailable', 'service is not available', 503)
        if request.tenant not in self.settings.tenants:
            raise ServiceError('unauthorized', 'unknown tenant', 401)
        previous = self.journal.find(request.tenant, request.key)
        if previous is not None:
            if previous['fingerprint'] != request.fingerprint():
                raise ServiceError('idempotency_conflict', 'key is bound to another request', 409)
            return Ticket(previous['id'], request.tenant, previous['revision'])
        if not self.accepting:
            raise ServiceError('draining', 'service is draining', 503)
        spec = self.catalog.resolve(request.model)
        tokens = encode(request.prompt, spec.tokenizer)
        if len(tokens) + request.max_tokens > spec.context_limit:
            raise ServiceError('context_limit', 'request exceeds model context limit', 422)
        ticket = Ticket(uuid.uuid4().hex, request.tenant, spec.revision)
        self.scheduler.enqueue(ticket.request_id, request.tenant, len(tokens) + request.max_tokens)
        try:
            self.journal.create(ticket, request, len(tokens))
        except BaseException:
            self.scheduler.release(ticket.request_id)
            raise
        self.control.pin(spec.revision)
        self.requests[ticket.request_id] = LiveRequest(request, spec, tokens, time.monotonic())
        self.metrics.count('requests_total', request.model, 'accepted')
        self.idle.clear()
        self.wakeup.set()
        return ticket

    async def activate(self, alias, spec, expected_epoch, operation_id):
        return await self.control.activate(alias, spec, expected_epoch, operation_id)

    async def _notify(self):
        async with self.changed:
            self.changed.notify_all()

    async def _pump(self):
        while True:
            await self.wakeup.wait()
            self.wakeup.clear()
            while (request_id := self.scheduler.take()) is not None:
                self.journal.running(request_id)
                self.tasks[request_id] = asyncio.create_task(self._run(request_id))

    async def _finish(self, request_id, state, reason):
        if self.journal.terminal(request_id, state, reason):
            live = self.requests[request_id]
            self.metrics.count('terminal_total', live.request.model, state)
        await self._notify()

    async def _cleanup(self, request_id):
        live = self.requests.get(request_id)
        if live is None:
            return
        if live.cleaning:
            await live.cleaned.wait()
            return
        live.cleaning = True
        try:
            self.scheduler.release(request_id)
            self.cache.release(live.cache_lease)
            await self.control.release(live.spec.revision)
        finally:
            self.requests.pop(request_id, None)
            self.tasks.pop(request_id, None)
            live.cleaned.set()
            self.wakeup.set()
            if not self.requests:
                self.idle.set()
            await self._notify()

    async def _run(self, request_id):
        live = self.requests[request_id]
        request, spec = live.request, live.spec
        live.cache_lease = self.cache.acquire(request.tenant, spec.revision, spec.tokenizer, live.tokens)
        stop = StopFilter(request.stop)
        accepted = 0
        try:
            for attempt in (1, 2):
                source = self.backend.generate(Work(request_id, spec.revision, attempt, live.tokens, request.max_tokens))
                expected_index = 0
                try:
                    async for item in source:
                        if item.attempt != attempt:
                            continue
                        if type(item.index) is not int or item.index < 0:
                            await self._finish(request_id, 'failed', 'backend_protocol')
                            return
                        if item.finish_reason is not None:
                            if item.index != expected_index or item.finish_reason not in ('stop', 'length'):
                                await self._finish(request_id, 'failed', 'backend_protocol')
                            else:
                                self.journal.delta(request_id, stop.finish())
                                await self._finish(request_id, 'completed', item.finish_reason)
                            return
                        if item.index < expected_index:
                            continue
                        if item.index != expected_index or not isinstance(item.text, str):
                            await self._finish(request_id, 'failed', 'backend_protocol')
                            return
                        elapsed = max(0, time.monotonic() - live.submitted) if accepted == 0 else None
                        text = stop.feed(item.text)
                        self.journal.token(request_id, text, elapsed)
                        if accepted == 0:
                            self.metrics.observe(request.model, elapsed)
                        accepted += 1
                        expected_index += 1
                        self.metrics.count('tokens_total', request.model, 'generated')
                        await self._notify()
                        if stop.stopped:
                            await self._finish(request_id, 'completed', 'stop')
                            return
                        if accepted >= request.max_tokens:
                            self.journal.delta(request_id, stop.finish())
                            await self._finish(request_id, 'completed', 'length')
                            return
                    await self._finish(request_id, 'failed', 'backend_eof')
                    return
                except BackendFault as error:
                    if not (error.retryable and accepted == 0 and attempt == 1):
                        await self._finish(request_id, 'failed', 'backend_error')
                        return
                finally:
                    await source.aclose()
        except asyncio.CancelledError:
            await self._finish(request_id, 'cancelled', 'client_cancel')
        except Exception:
            await self._finish(request_id, 'failed', 'internal_error')
        finally:
            await self._cleanup(request_id)

    async def stream(self, ticket, after=0):
        if type(after) is not int or after < 0:
            raise ServiceError('invalid_cursor', 'event cursor must be a nonnegative integer')
        self._owned(ticket.tenant, ticket.request_id)
        latest = self.journal.db.execute('SELECT COALESCE(MAX(seq),0) FROM events WHERE request_id=?', (ticket.request_id,)).fetchone()[0]
        if after > latest:
            raise ServiceError('invalid_cursor', 'event cursor does not exist')
        cursor = after
        while True:
            async with self.changed:
                # Bounded replay batches avoid duplicating the entire historical
                # stream in every slow subscriber's memory.
                batch = self.journal.events(ticket.request_id, cursor, limit=32)
                row = self._owned(ticket.tenant, ticket.request_id)
                if not batch and row['state'] in ('queued', 'running'):
                    await self.changed.wait()
                    continue
            for event in batch:
                cursor = event.seq
                yield event
            if row['state'] not in ('queued', 'running') and len(batch) < 32:
                return

    async def cancel(self, tenant, request_id):
        row = self._owned(tenant, request_id)
        if row['state'] not in ('queued', 'running'):
            return self.status(tenant, request_id)
        await self._finish(request_id, 'cancelled', 'client_cancel')
        task = self.tasks.get(request_id)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._cleanup(request_id)
        return self.status(tenant, request_id)

    def status(self, tenant, request_id):
        row = self._owned(tenant, request_id)
        return {k: row[k] for k in ('id', 'revision', 'state', 'prompt_tokens', 'output_tokens', 'error')}

    def snapshot(self):
        return {'scheduler': self.scheduler.snapshot(), 'cache': self.cache.snapshot(), 'models': self.catalog.list(),
                'lifecycle': {'accepting': self.accepting, 'live_requests': len(self.requests),
                              'generation_tasks': len(self.tasks), 'loaded_revisions': sorted(self.control.loaded)}}

    def prometheus(self):
        return self.metrics.render(self.scheduler.snapshot(), self.cache.snapshot())

    async def drain(self):
        self.accepting = False
        await self.idle.wait()
        # A prepared deployment must finish rolling back before the barrier.
        async with self.control.lock:
            pass
        return {'drained': True}

    async def _close(self):
        self.accepting = False
        try:
            if self.pump:
                self.pump.cancel()
                await asyncio.gather(self.pump, return_exceptions=True)
            for request_id, live in list(self.requests.items()):
                await self.cancel(live.request.tenant, request_id)
            await self.control.close()
        finally:
            self.journal.close()
            self.closed = True

    async def close(self):
        if self.closed:
            return
        if self.close_task is None:
            self.close_task = asyncio.create_task(self._close())
        await asyncio.shield(self.close_task)
