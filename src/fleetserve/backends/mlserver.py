"""MLServer's real data plane, with a small deterministic CPU token runtime.

Native mode uses predict_stream. Parallel mode performs token steps through
MLServer's multiprocessing inference pool; it does not claim native streaming
support in ParallelModel. Both modes use the same V2 request/response types.
"""
import asyncio
import os
from mlserver import MLModel
from mlserver.settings import Settings, ModelSettings, ModelParameters
from mlserver.types import InferenceRequest, InferenceResponse, RequestInput, ResponseOutput, Parameters
from mlserver.registry import MultiModelRegistry
from mlserver.handlers.dataplane import DataPlane
from prometheus_client import CollectorRegistry
from streamserve.protocol import WorkerEvent


class TokenRuntime(MLModel):
    async def load(self):
        self.closed = False
        self.active = 0
        return True

    async def unload(self):
        self.closed = True
        return True

    async def predict(self, payload):
        extra = self.settings.parameters.extra or {}
        await asyncio.sleep(extra.get('delay', 0))
        if self.closed:
            raise RuntimeError('runtime was unloaded during prediction')
        index = int(payload.inputs[0].data.root[0])
        label = extra.get('label', self.version)
        return InferenceResponse(model_name=self.name, model_version=self.version,
                                 parameters=Parameters(worker_pid=os.getpid()),
                                 outputs=[ResponseOutput(name='token', datatype='BYTES', shape=[1],
                                                         data=[f'{label}:{index} '])])

    async def predict_stream(self, payloads):
        self.active += 1
        try:
            async for payload in payloads:
                count = int(getattr(payload.parameters, 'token_count', 1))
                for index in range(count):
                    step = payload.model_copy(deep=True)
                    step.inputs[0].data.root[0] = index
                    yield await self.predict(step)
        finally:
            self.active -= 1


class MLServerCluster:
    def __init__(self, parallel=False, workers=1):
        self.parallel, self.workers = parallel, workers
        self.pool = None
        self.metrics = CollectorRegistry()
        self.settings = Settings(parallel_workers=workers if parallel else 0,
                                 parallel_workers_timeout=5, cache_size=1048576)
        self.registry = MultiModelRegistry()
        self.dataplane = DataPlane(self.settings, self.registry, metrics_registry=self.metrics)
        self.lock = asyncio.Lock()
        self.loaded = {}
        self.worker_pids = set()

    def __call__(self, worker, revision):
        return MLServerBackend(self, worker, revision)

    async def load(self, name, revision):
        async with self.lock:
            key = (name, revision)
            if self.loaded.get(key, 0):
                self.loaded[key] += 1
                return
            if self.parallel and self.pool is None:
                from mlserver.parallel.pool import InferencePool
                self.pool = InferencePool(self.settings)
                self.registry = MultiModelRegistry(on_model_load=[self.pool.load_model],
                    on_model_reload=[self.pool.reload_model], on_model_unload=[self.pool.unload_model])
                self.dataplane._model_registry = self.registry
            await self.registry.load(ModelSettings(name=name, implementation=TokenRuntime,
                                     parameters=ModelParameters(version=revision)))
            self.loaded[key] = 1

    async def unload(self, name, revision):
        async with self.lock:
            key = (name, revision)
            self.loaded[key] -= 1
            if not self.loaded[key]:
                await self.registry.unload_version(name, revision)
                del self.loaded[key]

    async def close(self):
        for name in {name for name, _ in self.loaded}:
            await self.registry.unload(name)
        self.loaded.clear()
        if self.pool:
            await self.pool.close()
            self.pool = None


class MLServerBackend:
    def __init__(self, cluster, name, revision):
        self.cluster, self.name, self.revision = cluster, name, revision

    async def load(self, spec):
        await self.cluster.load(self.name, spec.revision)

    async def unload(self, revision):
        await self.cluster.unload(self.name, revision)

    async def generate(self, work):
        def payload(index):
            return InferenceRequest(id=f'{work.request_id}.{work.attempt}.{index}',
                parameters=Parameters(token_count=work.max_tokens),
                inputs=[RequestInput(name='index', datatype='INT64', shape=[1], data=[index])])
        if self.cluster.parallel:
            for index in range(work.max_tokens):
                result = await self.cluster.dataplane.infer(payload(index), self.name, work.revision)
                self.cluster.worker_pids.add(result.parameters.worker_pid)
                yield WorkerEvent(work.attempt, index, result.outputs[0].data.root[0])
        else:
            async def inputs():
                yield payload(0)
            source = self.cluster.dataplane.infer_stream(inputs(), self.name, work.revision)
            try:
                index = 0
                async for result in source:
                    self.cluster.worker_pids.add(result.parameters.worker_pid)
                    yield WorkerEvent(work.attempt, index, result.outputs[0].data.root[0])
                    index += 1
            finally:
                await source.aclose()
        yield WorkerEvent(work.attempt, work.max_tokens, finish_reason='length')
