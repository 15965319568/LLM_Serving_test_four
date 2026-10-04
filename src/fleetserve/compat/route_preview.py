"""Offline v1 percentage preview retained for the previous operations client."""
from ..errors import FleetError


def preview(document):
    if document.get('schema') != 1:
        raise FleetError('unsupported_preview')
    percent = document.get('candidate_percent',0)
    if type(percent) not in (int,float) or not 0 <= percent <= 100:
        raise FleetError('invalid_preview')
    return {'model':document['model'],'stable':document['stable'],
            'candidate':document.get('candidate'),'candidate_percent':percent,
            'stable_percent':100-percent,'scope':'offline-preview'}
