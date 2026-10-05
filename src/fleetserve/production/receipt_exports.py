"""Gateway admission exports are bound to acknowledged routing facts."""
from ..operations.handoff_sources import latest

def gateway_index(gateways, routes, cutoff):
    index, receipts = {}, []
    for gateway in gateways:
        matching = [row for row in routes if row['receipt'] == gateway['route_receipt']
                    and row['state'] in ('published', 'prepared')]
        route = latest(matching, 'sequence', ('tenant', 'alias', 'revision', 'cohort'))
        receipt = None
        if route is not None and route['tenant'] == gateway['tenant']:
            receipt = dict(request_id=gateway['admission_id'], tenant=gateway['tenant'], alias=route['alias'],
                           revision=route['revision'], cohort=route['cohort'], admitted=float(gateway['accepted_at']),
                           kind=gateway['kind'], source='gateway')
            receipts.append(receipt)
        index[gateway['ticket']] = (gateway, receipt)
    return index, sorted(receipts, key=lambda r: r['request_id'])
