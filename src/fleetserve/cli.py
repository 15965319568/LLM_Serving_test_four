"""Local administration. Commands opening state require the serving owner to be stopped."""
import argparse
import asyncio
import json
from pathlib import Path
import signal
from streamserve.rpc_backend import ProcessBackend
from streamserve.transport import HTTPServer
from .compat.dashboard import summarize_export
from .compat.route_preview import preview
from .configuration import load_profile
from .errors import FleetError
from .http import Application
from .operations.imports import import_bundle, json_records
from .operations.load import drive
from .operations.reconciliation import accounting, audit_page, route_consistency
from .operations.supervisor import WorkerSupervisor
from .service import Fleet


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument('--manifest',type=Path,default=Path('config/fleet/manifest.json'))
    root.add_argument('--profile',default='production')
    root.add_argument('--state',type=Path,default=Path('state/fleet'))
    commands = root.add_subparsers(dest='command',required=True)
    commands.add_parser('resolve')
    commands.add_parser('init')
    commands.add_parser('status')
    progress = commands.add_parser('progress'); progress.add_argument('release')
    commands.add_parser('reconcile')
    commands.add_parser('accounting')
    audit = commands.add_parser('audit'); audit.add_argument('--after',type=int,default=0)
    serve = commands.add_parser('serve')
    serve.add_argument('--host',default='127.0.0.1'); serve.add_argument('--port',type=int,default=8081)
    serve.add_argument('--backend',choices=['echo','process','mlserver','mlserver-parallel'],default='mlserver')
    serve.add_argument('--demo-workers',action='store_true')
    load = commands.add_parser('load')
    load.add_argument('--tenant',default='north'); load.add_argument('--alias',default='chat')
    load.add_argument('--count',type=int,default=12); load.add_argument('--tokens',type=int,default=8)
    load.add_argument('--prefix',default='load'); load.add_argument('--parallel',type=int,default=3)
    imp = commands.add_parser('import'); imp.add_argument('bundle',type=Path)
    begin = commands.add_parser('begin')
    begin.add_argument('release'); begin.add_argument('alias'); begin.add_argument('revision')
    begin.add_argument('--bps',type=int,required=True); begin.add_argument('--epoch',type=int,required=True)
    begin.add_argument('--operation',required=True)
    evaluate = commands.add_parser('evaluate')
    evaluate.add_argument('release'); evaluate.add_argument('--start',type=float,required=True)
    evaluate.add_argument('--end',type=float,required=True); evaluate.add_argument('--assessment',required=True)
    apply = commands.add_parser('apply'); apply.add_argument('assessment'); apply.add_argument('--operation',required=True)
    rollback = commands.add_parser('rollback'); rollback.add_argument('release')
    rollback.add_argument('--epoch',type=int,required=True); rollback.add_argument('--operation',required=True)
    tick = commands.add_parser('tick'); tick.add_argument('--start',type=float,required=True); tick.add_argument('--end',type=float,required=True)
    dashboard = commands.add_parser('dashboard'); dashboard.add_argument('file',type=Path)
    route = commands.add_parser('preview'); route.add_argument('file',type=Path)
    return root


async def serve(fleet,args):
    supervisor = WorkerSupervisor(fleet,fleet.config.get('demo_workers',[]))
    if args.demo_workers:
        supervisor.register()
    task = asyncio.create_task(supervisor.run())
    server = await HTTPServer(Application(fleet),args.host,args.port).start()
    print(json.dumps({'listening':server.port,'backend':args.backend}),flush=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for value in [signal.SIGINT,signal.SIGTERM]:
        loop.add_signal_handler(value,stop.set)
    try:
        await stop.wait()
    finally:
        await supervisor.close(); await task
        await server.close()


async def execute(args):
    if args.command == 'resolve':
        resolved = load_profile(args.manifest,args.profile)
        # The resolved profile includes public demo credentials. Do not use
        # this offline command with production secrets in a shared terminal.
        return resolved
    if args.command == 'dashboard':
        return summarize_export([row for _,row in json_records(args.file)])
    if args.command == 'preview':
        return preview(json.loads(args.file.read_text(encoding='utf-8')))
    backend = (lambda worker,revision:ProcessBackend()) if getattr(args,'backend',None)=='process' else None
    if getattr(args,'backend',None) == 'echo':
        from streamserve.backend import EchoBackend
        backend = lambda worker,revision:EchoBackend()
    if getattr(args,'backend',None) == 'mlserver-parallel':
        from .backends.mlserver import MLServerCluster
        backend = MLServerCluster(parallel=True)
    fleet = Fleet.from_profile(args.manifest,args.state,args.profile,backend_factory=backend)
    try:
        await fleet.start()
        if args.command=='init':
            # Initialization is explicit; ordinary status/import commands must
            # not refresh expired workers or replace their generations.
            supervisor = WorkerSupervisor(fleet,fleet.config.get('demo_workers',[]),ttl=3600)
            supervisor.register()
            return fleet.snapshot()
        if args.command=='serve':
            return await serve(fleet,args)
        if args.command=='status': return fleet.snapshot()
        if args.command=='progress': return fleet.releases.progress(args.release)
        if args.command=='reconcile': return fleet.outbox.reconcile()
        if args.command=='accounting': return accounting(fleet.store)
        if args.command=='audit': return audit_page(fleet.store,args.after)
        if args.command=='import': return import_bundle(fleet,args.bundle)
        if args.command=='begin': return fleet.releases.begin(args.release,args.alias,args.revision,args.bps,args.epoch,args.operation)
        if args.command=='evaluate': return fleet.assessor.evaluate(args.release,args.start,args.end,args.assessment)
        if args.command=='apply': return fleet.releases.apply(args.assessment,args.operation)
        if args.command=='rollback': return fleet.releases.rollback(args.release,args.epoch,args.operation)
        if args.command=='tick': return fleet.controller.tick(args.start,args.end)
        if args.command=='load': return await drive(fleet.gateway,args.tenant,args.alias,args.count,args.tokens,args.prefix,args.parallel)
        raise FleetError('unknown_command')
    finally:
        await fleet.close()


def main():
    args = parser().parse_args()
    try:
        result = asyncio.run(execute(args))
    except (FleetError,ValueError,OSError) as error:
        print(json.dumps({'error':getattr(error,'code',type(error).__name__),'message':str(error)},ensure_ascii=False))
        return 1
    if result is not None:
        print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0
