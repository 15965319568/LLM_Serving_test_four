"""Standalone HTTP server entry point."""
import argparse
import asyncio
import signal
from .asgi import Application
from .backend import EchoBackend
from .config import Settings
from .engine import Engine
from .rpc_backend import ProcessBackend
from .transport import HTTPServer


async def serve(args):
    settings = Settings.load(args.config) if args.config else Settings()
    backend = ProcessBackend() if args.backend == 'process' else EchoBackend()
    engine = Engine(settings, backend, args.journal)
    await engine.start()
    server = await HTTPServer(Application(engine), args.host, args.port).start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    print(f'listening http://{args.host}:{server.port}', flush=True)
    try:
        await stop.wait()
    finally:
        await server.close()
        await engine.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config')
    parser.add_argument('--journal', default='state/requests.sqlite3')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8081)
    parser.add_argument('--backend', choices=('echo', 'process'), default='process')
    asyncio.run(serve(parser.parse_args()))
