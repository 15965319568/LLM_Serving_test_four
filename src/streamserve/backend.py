"""Backend boundary plus a deterministic CPU driver.

The driver has real asynchronous cancellation and load/unload lifetimes; it
does not download model weights and does not call a commercial model API.
"""
import asyncio
from .protocol import BackendFault, WorkerEvent


class EchoBackend:
    def __init__(self, delay=0):
        self.delay = delay
        self.loaded = {}
        self.loads = []
        self.unloads = []
        self.calls = []
        self.active = set()

    async def load(self, spec):
        if spec.revision in self.loaded and self.loaded[spec.revision] != spec:
            raise BackendFault('revision already loaded with another definition', False)
        self.loaded[spec.revision] = spec
        self.loads.append(spec.revision)

    async def unload(self, revision):
        self.loaded.pop(revision, None)
        self.unloads.append(revision)

    async def generate(self, work):
        if work.revision not in self.loaded:
            raise BackendFault('revision not loaded')
        self.calls.append(work)
        identity = (work.request_id, work.attempt)
        self.active.add(identity)
        try:
            for index in range(work.max_tokens):
                await asyncio.sleep(self.delay)
                yield WorkerEvent(work.attempt, index, f'{work.revision}:{index} ')
            yield WorkerEvent(work.attempt, work.max_tokens, finish_reason='length')
        finally:
            self.active.discard(identity)
