"""Model generations hosted by MLServer's real process pool and data plane."""
import asyncio
from prometheus_client import CollectorRegistry
from mlserver.settings import Settings, ModelSettings, ModelParameters
from mlserver.registry import MultiModelRegistry
from mlserver.handlers.dataplane import DataPlane
from mlserver.parallel.pool import InferencePool
from mlserver.batching.hooks import load_batching
from .runtime import MatrixRuntime


class ReplicaSet:
    def __init__(self, workers=2):
        self.settings = Settings(parallel_workers=workers, parallel_workers_timeout=3, cache_size=1048576)
        self.pool = None
        self.registry = None
        self.plane = None
        self.lock = asyncio.Lock()
        self.monitor = None
        self.loaded = set()
        self.recovery_errors = []

    async def start(self):
        self.pool = InferencePool(self.settings)
        self.registry = MultiModelRegistry(on_model_load=[self.pool.load_model, load_batching],
                                          on_model_unload=[self._retire])
        self.plane = DataPlane(self.settings, self.registry, CollectorRegistry())
        self.monitor = asyncio.create_task(self._monitor())

    async def _retire(self, model):
        batcher = getattr(model, '__adaptive_batching__', None)
        await self.pool.unload_model(model)
        if batcher is not None:
            await batcher.close()
        return model

    async def load(self, alias, generation, spec):
        options = dict(spec.get('runtime_options', {}), revision=spec['revision'], factor=spec['factor'])
        settings = ModelSettings(name=alias, implementation=MatrixRuntime,
            max_batch_size=spec['max_batch_size'], max_batch_time=spec['max_batch_time'],
            parameters=ModelParameters(version=generation, extra=options))
        async with self.lock:
            await self.registry.load(settings)
            self.loaded.add((alias,generation))

    async def unload(self, alias, generation):
        async with self.lock:
            if (alias,generation) in self.loaded:
                await self.registry.unload_version(alias,generation)
                self.loaded.discard((alias,generation))

    async def infer(self, alias, generation, request):
        return await self.plane.infer(request,alias,generation)

    async def flush(self, alias, generation):
        model = await self.registry.get_model(alias,generation)
        batcher = getattr(model,'__adaptive_batching__',None)
        if batcher:
            await batcher.flush()

    async def batching(self, alias, generation):
        model = await self.registry.get_model(alias,generation)
        batcher = getattr(model,'__adaptive_batching__',None)
        return batcher.stats() if batcher else None

    def pids(self):
        return sorted(self.pool._workers) if self.pool else []

    async def replace_worker(self,pid):
        async with self.lock:
            worker = self.pool._workers.get(pid)
            if worker is None:
                raise ValueError('worker does not belong to this pool')
            worker.kill()
            await asyncio.to_thread(worker.join,3)
            await self.pool.on_worker_stop(pid,worker.exitcode)
        return self.pids()

    async def _monitor(self):
        while True:
            await asyncio.sleep(.05)
            async with self.lock:
                for pid,worker in list(self.pool._workers.items()):
                    if worker.exitcode is not None:
                        try:
                            await self.pool.on_worker_stop(pid,worker.exitcode)
                        except Exception as error:
                            self.recovery_errors.append(type(error).__name__)

    async def close(self):
        if self.monitor:
            self.monitor.cancel()
            await asyncio.gather(self.monitor,return_exceptions=True)
        if self.pool:
            for alias,generation in list(self.loaded):
                await self.unload(alias,generation)
            await self.pool.close()
            self.pool = None
