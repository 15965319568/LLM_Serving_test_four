from collections import defaultdict
import json
from ..errors import FleetError
from ..util import canonical, digest, identifier, timestamp
from .projection import project
from .statistics import mix_distance, summarize


class Assessor:
    def __init__(self, store, evidence, releases, config):
        self.store, self.evidence, self.releases, self.config = store, evidence, releases, config

    def evaluate(self, release_id, start, end, assessment_id):
        identifier(assessment_id, 'assessment_id')
        start, end = timestamp(start), timestamp(end)
        if end <= start:
            raise FleetError('invalid_window')
        release = self.releases.get(release_id)
        if release['phase'] != 'canary':
            raise FleetError('release_not_observing', release_id, 409)
        route = self.releases.route(release['alias'])
        if route['epoch'] != release['route_epoch']:
            raise FleetError('epoch_conflict', 'release no longer owns this route', 409)
        version = self.evidence.version()
        old = self.store.one('SELECT * FROM assessments WHERE id=?', (assessment_id,))
        if old:
            if (old['release_id'],old['start'],old['end'],old['route_epoch'],old['evidence_version']) != (release_id,start,end,route['epoch'],version):
                raise FleetError('assessment_conflict', 'assessment id already describes another snapshot', 409)
            return json.loads(old['payload'])
        stable, candidate = release['candidate_plan']['stable'], release['candidate_plan']['candidate']
        projection = project(self.store, release['alias'], {stable,candidate}, start, end)
        policy = self.config['policy']
        groups = defaultdict(list)
        for sample in projection['samples']:
            groups[(sample['revision'],sample['cohort'])].append(sample)
        missing, regressions, metrics = [], [], {}
        for cohort in policy['required_cohorts']:
            before = summarize(groups[(stable,cohort)])
            after = summarize(groups[(candidate,cohort)])
            metrics[cohort] = {'stable': before, 'candidate': after}
            complete = min(before['count'],after['count'],before['quality_count'],after['quality_count']) >= policy['min_samples']
            for producer in self.config['telemetry']['required_sources']:
                watermark = self.store.one('SELECT through_time FROM watermarks WHERE producer=? AND cohort=?', (producer,cohort))
                if watermark is None or watermark['through_time'] < end:
                    complete = False
            if cohort in projection['conflict_cohorts']:
                complete = False
            if not complete:
                missing.append(cohort)
                continue
            if after['error_rate'] > policy['max_error_rate']:
                regressions.append(cohort+':error_rate')
            if after['p95_ms'] > before['p95_ms']*policy['max_latency_ratio']:
                regressions.append(cohort+':latency')
            if before['quality_mean']-after['quality_mean'] > policy['max_quality_drop'] + 1e-12:
                regressions.append(cohort+':quality')
        a = [s for s in projection['samples'] if s['revision']==stable and s['cohort'] in policy['required_cohorts']]
        b = [s for s in projection['samples'] if s['revision']==candidate and s['cohort'] in policy['required_cohorts']]
        distance = mix_distance(a,b,policy['required_cohorts'])
        if missing:
            decision, reasons = 'HOLD', ['incomplete:'+c for c in sorted(missing)]
        elif regressions:
            decision, reasons = 'ROLLBACK', sorted(regressions)
        elif distance is not None and distance > policy['max_mix_distance']:
            decision, reasons = 'HOLD', ['traffic_mix']
        else:
            decision, reasons = 'PROMOTE', []
        result = {'assessment_id': assessment_id, 'release_id': release_id, 'route_epoch': route['epoch'],
                  'evidence_version': version, 'window': [start,end], 'decision': decision, 'reasons': reasons,
                  'metrics': metrics, 'mix_distance': distance, 'evidence_digest': projection['digest'],
                  'sample_ids': sorted(s['request_id'] for s in projection['samples']),
                  'ignored': projection['ignored']}
        with self.store.transaction():
            self.store.execute('INSERT INTO assessments VALUES (?,?,?,?,?,?,?,?,?)',
                               (assessment_id,release_id,route['epoch'],start,end,projection['digest'],version,decision,canonical(result)))
        return result
