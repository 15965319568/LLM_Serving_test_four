from collections import defaultdict
import json


def accounting(store):
    grouped = defaultdict(lambda:{'admitted':0,'completed':0,'failed':0,'cancelled':0,'active':0,'tokens':0})
    for row in store.all('SELECT tenant,alias,revision,state,completion_tokens FROM bindings'):
        key = row['tenant'],row['alias'],row['revision']
        value = grouped[key]
        value['admitted'] += 1
        value[row['state'] if row['state'] in ('completed','failed','cancelled') else 'active'] += 1
        value['tokens'] += row['completion_tokens']
    return [{'tenant':key[0],'alias':key[1],'revision':key[2],**value} for key,value in sorted(grouped.items())]


def route_consistency(fleet):
    records = []
    for desired in fleet.store.all('SELECT * FROM routes ORDER BY alias'):
        actual = fleet.edge.get(desired['alias'])
        records.append({'alias':desired['alias'],'desired_epoch':desired['epoch'],'edge_epoch':actual['epoch'],
                        'matches':desired['epoch']==actual['epoch'] and json.loads(desired['plan'])==actual['plan']})
    return records


def audit_page(store, after=0, limit=100):
    from ..errors import FleetError
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
        raise FleetError('invalid_cursor')
    rows = store.all('SELECT * FROM audit WHERE seq>? ORDER BY seq LIMIT ?', (after,limit))
    return {'records':[{**row,'payload':json.loads(row['payload'])} for row in rows],
            'next':rows[-1]['seq'] if rows else after}


def metrics(fleet):
    # Historical revision identities are finite entries in the artifact registry.
    # Request ids, prompts, idempotency keys and credentials are never labels.
    lines = []
    totals = defaultdict(lambda:[0,0,0])
    for row in accounting(fleet.store):
        value = totals[(row['alias'],row['revision'])]
        value[0] += row['admitted']; value[1] += row['tokens']; value[2] += row['active']
    for (alias,revision),(admitted,tokens,active) in sorted(totals.items()):
        label = 'alias='+json.dumps(alias)+',revision='+json.dumps(revision)
        for name,value in [('requests_total',admitted),('tokens_total',tokens),('active',active)]:
            lines.append('fleet_'+name+'{'+label+'} '+str(value))
    lines.append('fleet_pending_effects '+str(len(fleet.outbox.pending())))
    return '\n'.join(lines)+'\n'
