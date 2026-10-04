"""Deployment ownership and durable compare-and-swap control operations."""
import asyncio
from collections import Counter
import json
from .config import ModelSpec
from .protocol import ServiceError


class DeploymentManager:
    def __init__(self, journal, catalog, backend, cache, accepting):
        self.journal, self.catalog = journal, catalog
        self.backend, self.cache = backend, cache
        self.accepting = accepting
        self.specs = journal.revisions()
        self.loaded = set()
        self.pins = Counter()
        self.lock = asyncio.Lock()
        for alias, (revision, epoch) in journal.deployments().items():
            self.catalog.aliases[alias] = self.specs[revision]
            self.catalog.epochs[alias] = epoch

    async def start(self):
        try:
            for revision, spec in self.catalog.revisions().items():
                await self.backend.load(spec)
                self.loaded.add(revision)
        except BaseException:
            for revision in list(self.loaded):
                await self.backend.unload(revision)
                self.loaded.discard(revision)
            raise

    def pin(self, revision):
        self.pins[revision] += 1

    async def release(self, revision):
        async with self.lock:
            self.pins[revision] -= 1
            if self.pins[revision] < 0:
                raise RuntimeError('revision lease released twice')
            await self._retire()

    async def _retire(self):
        current = set(self.catalog.revisions())
        for revision in sorted(self.loaded - current):
            if self.pins[revision] == 0:
                await self.backend.unload(revision)
                self.loaded.remove(revision)
                self.cache.retire(revision)

    async def activate(self, alias, spec, expected_epoch, operation_id):
        if not isinstance(spec, ModelSpec) or type(expected_epoch) is not int or expected_epoch < 0:
            raise ServiceError('invalid_activation', 'invalid spec or expected epoch')
        if not isinstance(alias, str) or not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ServiceError('invalid_activation', 'invalid alias or operation id')
        spec.validate()
        fingerprint = json.dumps([alias, spec.wire(), expected_epoch], sort_keys=True)
        async with self.lock:
            previous = self.journal.operation(operation_id)
            if previous is not None:
                if previous['fingerprint'] != fingerprint:
                    raise ServiceError('operation_conflict', 'operation id already has different parameters', 409)
                return json.loads(previous['result'])
            if not self.accepting():
                raise ServiceError('draining', 'service is draining', 503)
            self.catalog.resolve(alias)
            if self.catalog.epochs[alias] != expected_epoch:
                raise ServiceError('epoch_conflict', 'deployment epoch changed', 409)
            if spec.revision in self.specs and self.specs[spec.revision] != spec:
                raise ServiceError('revision_conflict', 'revision definition is immutable', 409)
            prepared = spec.revision not in self.loaded
            committed = False
            try:
                if prepared:
                    try:
                        await self.backend.load(spec)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        raise ServiceError('load_failed', 'candidate failed to load', 503) from None
                    self.loaded.add(spec.revision)
                if not self.accepting():
                    raise ServiceError('draining', 'service began draining during preparation', 503)
                result = {'alias': alias, 'revision': spec.revision, 'epoch': expected_epoch + 1}
                self.journal.commit_deployment(alias, spec, expected_epoch + 1, operation_id, fingerprint, result)
                self.specs[spec.revision] = spec
                self.catalog.aliases[alias] = spec
                self.catalog.epochs[alias] = expected_epoch + 1
                committed = True
                await self._retire()
                return result
            finally:
                if prepared and not committed:
                    # load may allocate before raising; unload is idempotent.
                    await self.backend.unload(spec.revision)
                    self.loaded.discard(spec.revision)

    async def close(self):
        async with self.lock:
            for revision in sorted(self.loaded):
                await self.backend.unload(revision)
                self.cache.retire(revision)
            self.loaded.clear()
