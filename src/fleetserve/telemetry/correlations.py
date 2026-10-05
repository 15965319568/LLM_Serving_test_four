"""Associate trace claims under the approved, time-applicable binding history."""
from ..operations.handoff_sources import latest

def bind(row, links, gateways, cutoff):
    matching = [link for link in links
                if link['trace_id'] == row['trace_id']
                and link['status'] != 'revoked'
                and float(link['valid_from']) <= row['trace_time']
                and (not link['valid_to'] or row['trace_time'] <= float(link['valid_to']))]
    try:
        link = latest(matching, 'binding_revision', ('status', 'boot', 'ticket'))
    except ValueError:
        return None, 'binding_conflict'
    if link is None:
        return None, 'unresolved_binding'
    if link['status'] == 'revoked':
        return None, 'revoked_binding'
    pair = gateways.get(link['ticket'])
    if pair is None:
        return None, 'missing_admission'
    gateway, receipt = pair
    if gateway['tenant'] != row['tenant']:
        return None, 'cross_tenant'
    if receipt is None:
        return None, 'unpublished_route'
    return receipt['request_id'], 'bound'
