"""Out-of-process CPU driver using the same interface as an accelerator worker."""
import asyncio
from dataclasses import asdict
import json
import sys
from .protocol import BackendFault, WorkerEvent


class Worker:
    def __init__(self, process):
        self.process = process
        self.pending = {}
        self.reader = None
        self.write_lock = asyncio.Lock()

    async def send(self, message):
        async with self.write_lock:
            self.process.stdin.write((json.dumps(message, ensure_ascii=False) + '\n').encode())
            await self.process.stdin.drain()

    async def read(self):
        try:
            while line := await self.process.stdout.readline():
                message = json.loads(line)
                queue = self.pending.get((message.get('id'), message.get('attempt', 1)))
                if queue is not None:
                    queue.put_nowait(message)
        except (ValueError, ConnectionError):
            pass
        finally:
            for (request_id, attempt), queue in list(self.pending.items()):
                queue.put_nowait({'id': request_id, 'attempt': attempt, 'error': 'worker_lost'})

    async def close(self):
        if self.process.returncode is None:
            self.process.terminate()
        await self.process.wait()
        if self.reader:
            await self.reader


class ProcessBackend:
    def __init__(self, worker_module='streamserve.worker_process'):
        self.workers = {}
        self.worker_module = worker_module

    async def load(self, spec):
        if spec.revision in self.workers:
            return
        process = await asyncio.create_subprocess_exec(sys.executable, '-u', '-m', self.worker_module,
                    json.dumps(spec.wire()), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL)
        worker = Worker(process)
        try:
            ready = json.loads(await process.stdout.readline())
            if ready != {'ready': spec.revision}:
                raise BackendFault('worker did not become ready', False)
        except BaseException:
            await worker.close()
            raise
        worker.reader = asyncio.create_task(worker.read())
        self.workers[spec.revision] = worker

    async def unload(self, revision):
        worker = self.workers.pop(revision, None)
        if worker:
            await worker.close()

    async def generate(self, work):
        worker = self.workers.get(work.revision)
        if worker is None or worker.process.returncode is not None:
            raise BackendFault('worker unavailable')
        queue = asyncio.Queue()
        identity = (work.request_id, work.attempt)
        worker.pending[identity] = queue
        try:
            await worker.send({'op': 'generate', 'work': asdict(work)})
            while True:
                message = await queue.get()
                if message.get('error'):
                    raise BackendFault(message['error'])
                if message.get('end'):
                    return
                yield WorkerEvent(**message['event'])
        except (BrokenPipeError, ConnectionError):
            raise BackendFault('worker disconnected') from None
        finally:
            worker.pending.pop(identity, None)
            if worker.process.returncode is None:
                try:
                    await worker.send({'op': 'cancel', 'id': work.request_id, 'attempt': work.attempt})
                except (BrokenPipeError, ConnectionError):
                    pass
