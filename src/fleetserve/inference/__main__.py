import argparse
import asyncio
import json
from pathlib import Path
import signal
from streamserve.transport import HTTPServer
from .fabric import ServingFabric
from .http import FabricApplication


async def main(args):
    config=json.loads(args.config.read_text(encoding='utf-8'))
    fabric=await ServingFabric(args.state,workers=config.get('workers',2)).start()
    server=None
    try:
        for item in config['deployments']:
            if item['alias'] not in fabric.current:
                await fabric.deploy(**item)
        server=await HTTPServer(FabricApplication(fabric,config),args.host,args.port).start()
        print(json.dumps({'listening':f'http://{args.host}:{server.port}','workers':fabric.replicas.pids()}),flush=True)
        stopped=asyncio.Event()
        loop=asyncio.get_running_loop()
        for sig in (signal.SIGINT,signal.SIGTERM):
            loop.add_signal_handler(sig,stopped.set)
        await stopped.wait()
    finally:
        if server: await server.close()
        await fabric.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description='Serve actual MLServer batches on a replicated CPU deployment')
    parser.add_argument('--config',type=Path,default=Path('config/fabric.json'))
    parser.add_argument('--state',type=Path,default=Path('state/fabric'))
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8082)
    asyncio.run(main(parser.parse_args()))
