"""Composition root used by both the HTTP host and offline replay commands."""
from pathlib import Path
from streamserve.ownership import JournalOwner
from .artifacts import ArtifactRegistry
from .configuration import load_profile, validate
from .control.outbox import Outbox
from .control.releases import Releases
from .control.loop import ReleaseLoop
from .database import Database
from .edge.cache import EdgeCache
from .gateway import Gateway
from .leases import LeaseRegistry
from .routing import Planner
from .telemetry.assessment import Assessor
from .telemetry.ingestion import EvidenceStore
from .util import SystemClock


class Fleet:
    def __init__(self, config, state_dir, clock=None, backend_factory=None):
        if backend_factory is None:
            from .backends.mlserver import MLServerCluster
            backend_factory = MLServerCluster()
        self.backend_factory = backend_factory
        self.config = validate(config)
        self.state_dir = Path(state_dir)
        self.clock = clock or SystemClock()
        self.owner = JournalOwner(self.state_dir/'control.sqlite3')
        try:
            self.store = Database(self.state_dir/'control.sqlite3')
            self.edge = EdgeCache(self.state_dir/'edge.sqlite3')
            self.artifacts = ArtifactRegistry(self.store)
            self.leases = LeaseRegistry(self.store,self.artifacts,config,self.clock)
            self.planner = Planner(config,self.artifacts,self.leases)
            self.outbox = Outbox(self.store,self.edge,self.clock)
            self.releases = Releases(self.store,self.planner,self.outbox,config,self.clock)
            self.evidence = EvidenceStore(self.store,config,self.clock)
            self.assessor = Assessor(self.store,self.evidence,self.releases,config)
            self.releases.assessor = self.assessor
            self.gateway = Gateway(self.store,self.edge,self.planner,self.artifacts,self.evidence,
                                   config,self.clock,self.state_dir,backend_factory)
            self.controller = ReleaseLoop(self)
            self.started = self.closed = False
        except BaseException:
            self.owner.close()
            raise

    @classmethod
    def from_profile(cls, manifest, state_dir, profile='production', **kwargs):
        resolved = load_profile(manifest,profile)
        fleet = cls(resolved['config'],state_dir,**kwargs)
        try:
            for path in resolved['config'].get('artifact_manifests',[]):
                fleet.artifacts.import_manifest(Path(manifest).resolve().parent/path)
            fleet.resolved_profile = resolved
        except BaseException:
            fleet.edge.close(); fleet.store.close(); fleet.owner.close()
            raise
        return fleet

    async def start(self):
        if self.started:
            return
        self.releases.bootstrap()
        self.outbox.reconcile()
        await self.gateway.start()
        self.started = True

    def snapshot(self):
        return {'ready':self.started and not self.closed and self.gateway.accepting,
                'routes':self.edge.list(),'releases':self.releases.list(),
                'workers':self.leases.list(),'pending_effects':self.outbox.pending(),
                'active_requests':len(self.gateway.requests.active()),'evidence_version':self.evidence.version()}

    async def close(self):
        if self.closed:
            return
        try:
            await self.gateway.close()
        finally:
            closer = getattr(self.backend_factory, 'close', None)
            if closer:
                await closer()
            self.edge.close(); self.store.close(); self.owner.close()
            self.closed = True

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self,*unused):
        await self.close()
