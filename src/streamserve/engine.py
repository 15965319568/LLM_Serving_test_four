"""Single-deployment inference coordinator.

The journal, scheduler and cache each own one part of a request's lifetime.
Public methods are called from one asyncio event loop. Generation tasks are
independent of individual HTTP subscribers and remain observable by request ID.
"""
import asyncio
import time
import uuid
from .cache import PrefixCache
from .catalog import Catalog
from .journal import Journal
from .metrics import Metrics
from .protocol import BackendFault, ServiceError, Ticket, Work
from .sampling import StopFilter
from .scheduler import Scheduler
from .tokenizer import encode


class Engine:
    def __init__(self, settings, backend, journal_path=':memory:'):
        settings.validate()
        self.settings = settings
        self.backend = backend
        self.journal = Journal(journal_path)
        self.catalog = Catalog(settings.models)
        self.scheduler = Scheduler(settings)
        self.cache = PrefixCache(settings.cache_tokens)
        self.metrics = Metrics()
        self.requests = {}
        self.tasks = {}
        self.cache_leases = {}
        self.changed = asyncio.Condition()
        self.wakeup = asyncio.Event()
        self.pump = None
        self.started = False
        self.closed = False

    async def start(self):
        if self.started:
            return
        for spec in self.catalog.revisions().values():
            await self.backend.load(spec)
        self.started = True
        self.pump = asyncio.create_task(self._pump())

    def _owned(self, tenant, request_id):
        row = self.journal.get(request_id)
        if row is None or row['tenant'] != tenant:
            raise ServiceError('not_found', 'request not found', 404)
        return row

    async def submit(self, request):
        request.validate()
        if not self.started or self.closed:
            raise ServiceError('unavailable', 'service is not accepting work', 503)
        if request.tenant not in self.settings.tenants:
            raise ServiceError('unauthorized', 'unknown tenant', 401)
        previous = self.journal.find(request.tenant, request.key)
        if previous is not None:
            if previous['fingerprint'] != request.fingerprint():
                raise ServiceError('idempotency_conflict', 'key is bound to another request', 409)
            return Ticket(previous['id'], request.tenant, previous['revision'])
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
        self.requests[ticket.request_id] = (request, spec, tokens, time.monotonic())
        self.metrics.count('requests_total', request.model, 'accepted')
        self.wakeup.set()
        return ticket

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
        changed = self.journal.terminal(request_id, state, reason)
        if changed:
            request = self.requests[request_id][0]
            self.metrics.count('terminal_total', request.model, state)
            self.scheduler.release(request_id)
            self.cache.release(self.cache_leases.pop(request_id, None))
        self.wakeup.set()
        await self._notify()

    async def _run(self, request_id):
        request, spec, tokens, submitted = self.requests[request_id]
        self.cache_leases[request_id] = self.cache.acquire(request.tenant, spec.revision, spec.tokenizer, tokens)
        stop = StopFilter(request.stop)
        first = True
        source = self.backend.generate(Work(request_id, spec.revision, 1, tokens, request.max_tokens))
        try:
            async for item in source:
                if item.finish_reason is not None:
                    self.journal.delta(request_id, stop.finish())
                    await self._finish(request_id, 'completed', item.finish_reason)
                    return
                if first:
                    self.metrics.observe(request.model, time.monotonic() - submitted)
                    first = False
                self.journal.token(request_id, stop.feed(item.text))
                self.metrics.count('tokens_total', request.model, 'generated')
                await self._notify()
                if stop.stopped:
                    await self._finish(request_id, 'completed', 'stop')
                    return
            await self._finish(request_id, 'failed', 'backend_eof')
        except asyncio.CancelledError:
            await self._finish(request_id, 'cancelled', 'client_cancel')
        except BackendFault:
            await self._finish(request_id, 'failed', 'backend_error')
        except Exception:
            await self._finish(request_id, 'failed', 'internal_error')
        finally:
            await source.aclose()

    async def stream(self, ticket, after=0):
        if type(after) is not int or after < 0:
            raise ServiceError('invalid_cursor', 'event cursor must be a nonnegative integer')
        self._owned(ticket.tenant, ticket.request_id)
        cursor = after
        while True:
            async with self.changed:
                batch = self.journal.events(ticket.request_id, cursor)
                row = self._owned(ticket.tenant, ticket.request_id)
                if not batch and row['state'] in ('queued', 'running'):
                    await self.changed.wait()
                    continue
            for event in batch:
                cursor = event.seq
                yield event
            if row['state'] not in ('queued', 'running'):
                return

    async def cancel(self, tenant, request_id):
        row = self._owned(tenant, request_id)
        if row['state'] not in ('queued', 'running'):
            return self.status(tenant, request_id)
        task = self.tasks.get(request_id)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._finish(request_id, 'cancelled', 'client_cancel')
        return self.status(tenant, request_id)

    def status(self, tenant, request_id):
        row = self._owned(tenant, request_id)
        return {k: row[k] for k in ('id', 'revision', 'state', 'prompt_tokens', 'output_tokens', 'error')}

    def snapshot(self):
        return {'scheduler': self.scheduler.snapshot(), 'cache': self.cache.snapshot(), 'models': self.catalog.list()}

    def prometheus(self):
        return self.metrics.render(self.scheduler.snapshot(), self.cache.snapshot())

    async def close(self):
        if self.closed:
            return
        self.closed = True
        if self.pump:
            self.pump.cancel()
            await asyncio.gather(self.pump, return_exceptions=True)
        for request_id in list(self.requests):
            await self.cancel(self.requests[request_id][0].tenant, request_id)
        for revision in self.catalog.revisions():
            await self.backend.unload(revision)
        self.journal.close()
