from __future__ import annotations
import asyncio
from collections import OrderedDict
import time
from ..utils import generate_uuid, schedule_with_callback
from .. import metrics
from ..context import model_context
from .requests import BatchedRequests
from .compatibility import compatibility_key


class AdaptiveBatcher:
    def __init__(self, model):
        self._model = model
        self._max_batch_size = model.settings.max_batch_size
        self._max_batch_time = model.settings.max_batch_time
        self._predict_fn = model.predict
        self.__requests = None
        self._async_responses = {}
        self._batching_task = None
        self._running = set()
        self._flush_event = asyncio.Event()
        self._closed = False
        self._dispatched = 0
        metrics.register('batch_request_queue', 'counter of request queue batch size')

    @property
    def _requests(self):
        if self.__requests is None:
            self.__requests = asyncio.Queue()
        return self.__requests

    def stats(self):
        return {'queued': self._requests.qsize(), 'waiting': len(self._async_responses),
                'running_batches': len(self._running), 'dispatched_batches': self._dispatched,
                'closed': self._closed}

    async def predict(self, req):
        if self._closed:
            raise RuntimeError('batcher is closed')
        compatibility_key(req)
        internal_id, _ = await self._queue_request(req)
        self._start_batcher_if_needed()
        return await self._wait_response(internal_id)

    async def _queue_request(self, req):
        internal_id = generate_uuid()
        future = asyncio.get_running_loop().create_future()
        self._async_responses[internal_id] = future
        self._requests.put_nowait((internal_id, req))
        self._batch_queue_monitor()
        return internal_id, future

    def _batch_queue_monitor(self):
        with model_context(self._model.settings):
            metrics.log(batch_request_queue=self._requests.qsize())

    async def _wait_response(self, internal_id):
        try:
            return await self._async_responses[internal_id]
        finally:
            self._async_responses.pop(internal_id, None)

    def _start_batcher_if_needed(self):
        if self._batching_task is None or self._batching_task.done():
            self._batching_task = schedule_with_callback(self._batcher(), self._batching_task_callback)

    def _batching_task_callback(self, task):
        if task.cancelled():
            self._clear_queue(RuntimeError('batcher stopped'))
        elif task.exception():
            self._clear_queue(task.exception())

    def _clear_queue(self, err):
        for future in list(self._async_responses.values()):
            if not future.done():
                future.set_exception(err)
        while not self._requests.empty():
            self._requests.get_nowait()

    async def _batcher(self):
        async for batched in self._batch_requests():
            self._dispatched += 1
            task = asyncio.create_task(self._predict_fn(batched.merged_request))
            self._running.add(task)
            task.add_done_callback(lambda done, group=batched: self._predict_callback(group, done))

    def _predict_callback(self, batched, task):
        self._running.discard(task)
        try:
            responses = batched.split_response(task.result())
            for identity, response in responses.items():
                future = self._async_responses.get(identity)
                if future is not None and not future.done():
                    future.set_result(response)
        except BaseException as error:
            if isinstance(error, asyncio.CancelledError):
                error = RuntimeError('model batch was cancelled')
            for identity in batched.inference_requests:
                future = self._async_responses.get(identity)
                if future is not None and not future.done():
                    future.set_exception(error)

    async def _batch_requests(self):
        while not self._requests.empty():
            groups = OrderedDict()
            count = 0
            deadline = time.monotonic() + self._max_batch_time
            while count < self._max_batch_size:
                try:
                    identity, req = await self._get_request(max(0, deadline-time.monotonic()))
                except asyncio.TimeoutError:
                    break
                future = self._async_responses.get(identity)
                if future is None or future.done():
                    continue
                try:
                    key = repr(sorted((head.name,head.datatype,head.shape[1:]) for head in req.inputs))
                except Exception as error:
                    future.set_exception(error)
                    continue
                groups.setdefault(key, {})[identity] = req
                count += 1
            for requests in groups.values():
                live = requests
                if live:
                    yield BatchedRequests(live)

    async def _get_request(self, timeout):
        if not self._requests.empty():
            return self._requests.get_nowait()
        if self._flush_event.is_set() or timeout <= 0:
            raise asyncio.TimeoutError()
        getter = asyncio.create_task(self._requests.get())
        flush = asyncio.create_task(self._flush_event.wait())
        try:
            done, _ = await asyncio.wait([getter, flush], timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if getter in done:
                return getter.result()
            raise asyncio.TimeoutError()
        finally:
            for task in (getter, flush):
                if not task.done():
                    task.cancel()
            await asyncio.gather(getter, flush, return_exceptions=True)

    async def flush(self):
        self._flush_event.set()
        try:
            if not self._requests.empty():
                self._start_batcher_if_needed()
            if self._batching_task:
                await asyncio.shield(self._batching_task)
            while self._running:
                await asyncio.gather(*list(self._running), return_exceptions=True)
        finally:
            self._flush_event.clear()

    async def close(self):
        self._closed = True
        return
