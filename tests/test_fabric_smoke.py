"""Public pilot smoke tests. They do not exercise the incident interleavings."""
import tempfile
import unittest
from fleetserve.inference import ServingFabric
from mlserver.types import InferenceRequest,RequestInput


class FabricSmoke(unittest.IsolatedAsyncioTestCase):
    async def test_real_cpu_service_boot_and_single_request(self):
        with tempfile.TemporaryDirectory() as directory:
            fabric=await ServingFabric(directory,2).start()
            try:
                await fabric.deploy('ranker','r1',2,'boot')
                response=await fabric.infer('north','ranker',InferenceRequest(id='demo',inputs=[
                    RequestInput(name='x',datatype='FP64',shape=[2,1],data=[1.,3.])]))
                self.assertEqual(response.outputs[0].data.root,[2.,6.])
                self.assertEqual(response.model_version,'r1')
                self.assertIn(response.parameters.worker_pid,fabric.replicas.pids())
                self.assertEqual(fabric.journal.receipts('north')[0]['state'],'completed')
            finally: await fabric.close()
