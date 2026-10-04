"""A restartable reconciliation tick; scheduling is supplied by the process host."""
from ..errors import FleetError
from ..util import digest, timestamp


class ReleaseLoop:
    def __init__(self,fleet):
        self.fleet = fleet

    def tick(self,window_start,window_end):
        start,end = timestamp(window_start),timestamp(window_end)
        published = self.fleet.outbox.reconcile()
        decisions = []
        for release in self.fleet.releases.list():
            if release['phase'] != 'canary':
                continue
            version = self.fleet.evidence.version()
            identity = digest([release['id'],release['route_epoch'],start,end,version])[:32]
            assessment_id = 'auto.'+identity
            assessment = self.fleet.assessor.evaluate(release['id'],start,end,assessment_id)
            try:
                applied = self.fleet.releases.apply(assessment_id,'decision.'+identity)
            except FleetError as error:
                if error.code != 'stale_assessment':
                    raise
                decisions.append({'release_id':release['id'],'retry':True,'reason':error.code})
            else:
                decisions.append(applied)
        published.extend(self.fleet.outbox.reconcile())
        return {'decisions':decisions,'published':published}
