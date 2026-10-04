"""Production host and offline collection replay. Stop the host before offline operations."""
import argparse
import asyncio
import json
from pathlib import Path
import signal
from .. import Fleet
from ..inference import ServingFabric
from ..operations.imports import import_receipts
from . import DeliveryInbox,ProductionService
from .http import ProductionApplication
from streamserve.transport import HTTPServer


async def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,default=Path('config/fleet/manifest.json'))
    parser.add_argument('--numeric',type=Path,default=Path('config/production-numeric.json'))
    parser.add_argument('--state',type=Path,default=Path('state/production'))
    parser.add_argument('--profile',default='production')
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('init');commands.add_parser('status')
    serving=commands.add_parser('serve');serving.add_argument('--port',type=int,default=8083)
    capture=commands.add_parser('import');capture.add_argument('capture_manifest',type=Path);capture.add_argument('--receipts',type=Path)
    args=parser.parse_args()
    numeric=json.loads(args.numeric.read_text(encoding='utf-8'))
    fleet=Fleet.from_profile(args.manifest,args.state/'fleet',args.profile)
    service=ProductionService(fleet,ServingFabric(args.state/'fabric',workers=numeric.get('workers',2)))
    inbox=None
    try:
        await service.start();inbox=DeliveryInbox(fleet,numeric['partitions'])
        if args.command=='init':
            for spec in fleet.config['demo_workers']:
                existing={row['worker']:row for row in fleet.leases.list()}
                lease=existing.get(spec['worker']) or fleet.leases.register(**spec,ttl=86400)
                await service.install(lease['worker'],lease['generation'],numeric['revision_factors'][lease['revision']],
                                      'init.'+lease['worker'],max_batch_size=8)
        elif args.command=='import':
            if args.receipts:print(json.dumps(import_receipts(fleet.evidence,args.receipts)))
            print(json.dumps(inbox.import_manifest(args.capture_manifest),ensure_ascii=False))
        elif args.command=='serve':
            server=await HTTPServer(ProductionApplication(service,inbox),port=args.port).start()
            print(json.dumps({'port':server.port,'state':str(args.state)}),flush=True)
            stop=asyncio.Event();loop=asyncio.get_running_loop()
            for sig in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(sig,stop.set)
            try:await stop.wait()
            finally:await server.close()
        print(json.dumps(dict(fleet=fleet.snapshot(),accounting=service.accounting(),partitions=inbox.status()),ensure_ascii=False,indent=2))
    finally:
        if inbox:inbox.close()
        await service.close()


if __name__=='__main__':asyncio.run(main())
