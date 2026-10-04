"""Deterministic numeric runtime for serving exercises, not a neural network."""
import asyncio
import json
import os
from pathlib import Path
import uuid
from mlserver import MLModel
from mlserver.types import InferenceResponse, ResponseOutput, Parameters


class MatrixRuntime(MLModel):
    def event(self, kind, **values):
        record = dict(kind=kind, pid=os.getpid(), version=self.version, **values)
        if self.options.get('events'):
            path = Path(self.options['events'])
            path.mkdir(parents=True, exist_ok=True)
            with (path/(str(os.getpid())+'.jsonl')).open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(record, sort_keys=True)+'\n')
        return record

    async def barrier(self, name):
        path = self.options.get(name)
        if path:
            while not Path(path).exists():
                await asyncio.sleep(.005)

    async def load(self):
        self.options = self.settings.parameters.extra or {}
        self.active = 0
        self.closed = False
        self.event('loading')
        await self.barrier('load_gate')
        if os.getpid() == self.options.get('fail_pid'):
            self.event('load_failed')
            raise RuntimeError('runtime artifact rejected on this replica')
        self.event('loaded')
        return True

    async def unload(self):
        self.event('unloaded', active=self.active)
        self.closed = True
        return True

    async def predict(self, payload):
        self.active += 1
        invocation = uuid.uuid4().hex
        rows = payload.inputs[0].shape[0]
        self.event('started', invocation=invocation, rows=rows)
        try:
            await self.barrier('predict_gate')
            if self.closed:
                raise RuntimeError('runtime unloaded during active inference')
            factor = self.options.get('factor', 1)
            offset = getattr(payload.parameters, 'offset', 0) if payload.parameters else 0
            outputs = []
            requested = {item.name for item in payload.outputs} if payload.outputs is not None else None
            for head in payload.inputs:
                name = head.name + '_out'
                if requested is not None and name not in requested:
                    continue
                values = [float(value)*factor + offset for value in head.data.root]
                outputs.append(ResponseOutput(name=name, datatype='FP64', shape=head.shape,
                                              data=values, parameters=Parameters(unit='score')))
            self.event('finished', invocation=invocation, rows=rows)
            return InferenceResponse(id=payload.id, model_name=self.name, model_version=self.version,
                parameters=Parameters(worker_pid=os.getpid(), invocation=invocation, revision=self.options['revision']),
                outputs=outputs)
        finally:
            self.active -= 1
