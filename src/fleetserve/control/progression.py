"""Persistent exposure history for staged releases."""
import json
from ..errors import FleetError
from ..util import integer, number


def validate_rollout(policy, initial_bps=None):
    rollout = policy.get('rollout')
    if rollout is None:
        return None
    if not isinstance(rollout, dict) or not {'steps_bps','healthy_windows','min_window_seconds'} <= rollout.keys():
        raise FleetError('invalid_rollout')
    steps = rollout.get('steps_bps')
    if not isinstance(steps, list) or len(steps) < 2:
        raise FleetError('invalid_rollout')
    for step in steps:
        integer(step, 'rollout step', 1)
    if steps != sorted(set(steps)) or steps[-1] != 10000:
        raise FleetError('invalid_rollout')
    integer(rollout['healthy_windows'], 'healthy_windows', 1)
    number(rollout['min_window_seconds'], 'min_window_seconds', .001)
    if initial_bps is not None and initial_bps != steps[0]:
        raise FleetError('invalid_initial_exposure', status=409)
    return rollout


class Progression:
    def __init__(self, store, clock):
        self.store, self.clock = store, clock

    def begin(self, release_id, epoch, policy, bps):
        if validate_rollout(policy, bps):
            self.store.execute('INSERT INTO rollout_progress VALUES (?,?,0,0,NULL,NULL)', (release_id,epoch))

    def get(self, release_id):
        row = self.store.one('SELECT * FROM rollout_progress WHERE release_id=?', (release_id,))
        if row is None:
            return None
        observations = self.store.all('SELECT * FROM rollout_windows WHERE release_id=? ORDER BY id', (release_id,))
        return {**row, 'observations': observations}

    def observe(self, release, assessment, preview):
        rollout = release['policy']['rollout']
        state = self.get(release['id'])
        if assessment['decision'] != 'PROMOTE':
            return assessment['decision'], None, 'observation'
        start,end = assessment['window']
        self.store.execute('INSERT OR REPLACE INTO rollout_windows(release_id,route_epoch,start,end,assessment_id,decision,counted) VALUES (?,?,?,?,?,?,1)',
            (release['id'],release['route_epoch'],start,end,assessment['assessment_id'],assessment['decision']))
        self.store.execute('UPDATE rollout_progress SET healthy_windows=healthy_windows+1,last_end=? WHERE release_id=?',(end,release['id']))
        target = rollout['steps_bps'][state['stage_index']+1]
        return ('PROMOTE' if target==10000 else 'ADVANCE'),target,'healthy_window'

    def committed(self, release_id, epoch, decision):
        if decision == 'HOLD':
            return
        self.store.execute('''UPDATE rollout_progress SET route_epoch=?,healthy_windows=0,
            stage_index=stage_index+?,effective_at=NULL,last_end=NULL WHERE release_id=?''',
            (epoch,int(decision in ('PROMOTE','ADVANCE')),release_id))
        self.store.execute('UPDATE rollout_progress SET effective_at=? WHERE release_id=?',(self.clock(),release_id))
        self.store.execute('UPDATE rollout_windows SET counted=0 WHERE release_id=?', (release_id,))

    def acknowledged(self, release_id, epoch):
        self.store.execute('''UPDATE rollout_progress SET effective_at=COALESCE(effective_at,?)
            WHERE release_id=? AND route_epoch=? AND stage_index>0''', (self.clock(),release_id,epoch))
