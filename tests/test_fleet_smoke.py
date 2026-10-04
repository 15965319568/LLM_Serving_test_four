from pathlib import Path
import tempfile
import unittest
import fleetserve
from fleetserve import Fleet
from streamserve import Request
from fleetserve.backends.mlserver import MLServerCluster

ROOT=Path(fleetserve.__file__).resolve().parents[2]


class FleetSmoke(unittest.IsolatedAsyncioTestCase):
    async def test_native_mlserver_request_and_reopen(self):
        with tempfile.TemporaryDirectory() as state:
            fleet=Fleet.from_profile(ROOT/'config/fleet/manifest.json',state)
            try:
                await fleet.start()
                for worker in fleet.config['demo_workers']:
                    fleet.leases.register(**worker,ttl=3600)
                ticket=await fleet.gateway.submit(Request('north','smoke','chat','hello',3))
                events=[e async for e in fleet.gateway.stream('north',ticket['id'])]
                await fleet.gateway.drain()
                self.assertIsInstance(fleet.backend_factory,MLServerCluster)
                self.assertEqual(events[-1].kind,'terminal')
                self.assertEqual(fleet.gateway.status('north',ticket['id'])['state'],'completed')
                self.assertTrue(fleet.backend_factory.worker_pids)
            finally:
                await fleet.close()
            fleet=Fleet.from_profile(ROOT/'config/fleet/manifest.json',state)
            try:
                await fleet.start()
                self.assertEqual(fleet.gateway.status('north',ticket['id'])['completion_tokens'],3)
            finally:
                await fleet.close()
