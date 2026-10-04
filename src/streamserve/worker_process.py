"""Multiplexed JSONL worker. stdout is reserved for protocol records."""
import asyncio
import json
import sys
from dataclasses import asdict
from .backend import EchoBackend
from .config import ModelSpec
from .protocol import Work


async def main():
    spec = ModelSpec(**json.loads(sys.argv[1]))
    backend = EchoBackend()
    await backend.load(spec)
    jobs = {}

    def emit(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)

    async def generate(value):
        work = Work(**value)
        source = backend.generate(work)
        try:
            async for event in source:
                emit({'id': work.request_id, 'event': asdict(event)})
        except asyncio.CancelledError:
            pass
        except Exception:
            emit({'id': work.request_id, 'error': 'worker_error'})
        finally:
            await source.aclose()
            emit({'id': work.request_id, 'end': True})
            jobs.pop(work.request_id, None)

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    emit({'ready': spec.revision})
    while line := await reader.readline():
        command = json.loads(line)
        if command['op'] == 'generate':
            value = command['work']
            jobs[value['request_id']] = asyncio.create_task(generate(value))
        elif command['op'] == 'cancel':
            task = jobs.get(command['id'])
            if task:
                task.cancel()
    for task in list(jobs.values()):
        task.cancel()
    await asyncio.gather(*list(jobs.values()), return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(main())
