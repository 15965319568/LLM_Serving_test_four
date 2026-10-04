"""Own the demo worker registrations and renew only this process's generations."""
import asyncio
from ..errors import FleetError


class WorkerSupervisor:
    def __init__(self,fleet,workers,ttl=30):
        self.fleet,self.workers,self.ttl = fleet,workers,ttl
        self.registrations = {}
        self.stop = asyncio.Event()

    def register(self):
        for value in self.workers:
            row = self.fleet.leases.register(**value,ttl=self.ttl)
            self.registrations[row['worker']] = row
        return list(self.registrations.values())

    def renew(self):
        results = []
        for name,row in list(self.registrations.items()):
            result = self.fleet.leases.heartbeat(name,row['generation'],row['sequence']+1,row['capacity'],self.ttl)
            results.append({'worker':name,**result})
            if result['accepted']:
                row['sequence'] += 1
            else:
                # Another supervisor replaced this worker identity; an old
                # process must not refresh the replacement's lease.
                del self.registrations[name]
        return results

    async def run(self):
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),self.ttl/3)
            except asyncio.TimeoutError:
                self.renew()

    async def close(self):
        self.stop.set()
