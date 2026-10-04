"""Join the retained telemetry exports for release dashboards."""
from collections import defaultdict
import json
from ..util import canonical, digest


def project(store, alias, revisions, start, end):
    receipts = {row['request_id']: row for row in store.all('SELECT * FROM receipts WHERE alias=?', (alias,))}
    candidates = defaultdict(list)
    ignored = []
    conflicts = set()
    for row in store.all('SELECT * FROM raw_events ORDER BY producer,event_id'):
        event = json.loads(row['payload'])
        tag = event['producer'] + ':' + event['event_id']
        receipt = receipts.get(event['request_id'])
        reason = None
        if row['conflict']:
            reason = 'conflicting_update'
            # An unresolved authoritative conflict inside the population is
            # evidence incompleteness even if another producer has a value.
            if receipt and receipt['kind']=='live' and start <= receipt['admitted'] < end and receipt['revision'] in revisions and event['role'] in ('gateway','quality'):
                conflicts.add(receipt['cohort'])
        elif event['deleted']:
            reason = 'deleted'
        elif event['role'] not in ('gateway','quality'):
            reason = 'advisory_source'
        elif receipt is None:
            reason = 'no_admission'
        elif receipt['revision'] not in revisions:
            reason = 'other_revision'
        elif receipt['kind'] != 'live' or event['sample_kind'] != 'live':
            reason = 'non_live'
        elif event['claimed_revision'] is not None and event['claimed_revision'] != receipt['revision']:
            reason = 'revision_mismatch'
        elif not start <= event['occurred'] < end:
            reason = 'outside_window'
        if reason:
            ignored.append({'record': tag, 'reason': reason})
            continue
        candidates[(event['request_id'], event['role'])].append(event)
    selected = {}
    for key, events in sorted(candidates.items()):
        # Producer rank is part of the documented provenance registry, not
        # arrival order; exported v1 snapshots must not replace live v2 facts.
        rank = min(event['rank'] for event in events)
        finalists = [event for event in events if event['rank'] == rank]
        bodies = {(event['latency_ms'], event['outcome'], event['quality'], event['occurred']) for event in finalists}
        if len(bodies) != 1:
            conflicts.add(receipts[key[0]]['cohort'])
            ignored.extend({'record': e['producer']+':'+e['event_id'], 'reason': 'conflicting_authorities'} for e in finalists)
            continue
        winner = min(finalists, key=lambda event: (event['producer'], event['event_id']))
        selected[key] = winner
        for event in events:
            if event is not winner:
                ignored.append({'record': event['producer']+':'+event['event_id'], 'reason': 'duplicate_or_lower_authority'})
    samples = []
    for (request_id, role), event in sorted(selected.items()):
        if role != 'gateway':
            continue
        receipt = receipts[request_id]
        quality = selected.get((request_id, 'quality'))
        samples.append({'request_id': request_id, 'revision': receipt['revision'], 'cohort': receipt['cohort'],
                        'latency_ms': event['latency_ms'], 'outcome': event['outcome'],
                        'quality': quality['quality'] if quality else None,
                        'gateway_record': event['producer']+':'+event['event_id'],
                        'quality_record': quality['producer']+':'+quality['event_id'] if quality else None})
    result = {'samples': samples, 'ignored': sorted(ignored, key=lambda v: (v['record'],v['reason'])),
              'conflict_cohorts': sorted(conflicts)}
    return {**result, 'digest': digest(result)}
