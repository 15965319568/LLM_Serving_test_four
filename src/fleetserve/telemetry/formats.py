from ..errors import FleetError
from ..util import identifier, integer, number, timestamp


def normalize(record, producer, specification):
    if not isinstance(record, dict):
        raise FleetError('invalid_record', 'record must be an object')
    fmt = specification['format']
    if fmt == 'event-v2':
        if record.get('schema') != 2:
            raise FleetError('invalid_record', 'expected event-v2')
        event_id, sequence = record['event_id'], record['update_seq']
        request_id, occurred = record['request_id'], timestamp(record['finished_at'])
        duration = record.get('latency')
        unit = record.get('latency_unit', 'ms')
        outcome = record.get('outcome')
        quality = record.get('score')
    elif fmt == 'export-v1':
        if record.get('schema') != 1:
            raise FleetError('invalid_record', 'expected export-v1')
        event_id, sequence = record['id'], record.get('version', 0)
        request_id = record['request']
        occurred = number(record['finished_ms'], 'finished_ms') / 1000
        duration = record.get('duration_ms')
        unit = 'ms'
        outcome = {'ok': 'completed', 'error': 'failed', 'cancel': 'cancelled'}.get(record.get('status'))
        quality = record.get('quality')
    else:
        raise FleetError('unsupported_format', fmt)
    identifier(event_id, 'event_id'); identifier(request_id, 'request_id')
    integer(sequence, 'update_seq')
    deleted = record.get('deleted', False)
    if type(deleted) is not bool:
        raise FleetError('invalid_record', 'deleted must be boolean')
    role = specification['role']
    latency_ms = None
    if not deleted and role == 'gateway':
        if unit not in ('ms', 's', 'us'):
            raise FleetError('invalid_unit', str(unit))
        latency_ms = number(duration, 'latency') * {'ms': 1, 's': 1000, 'us': 1}[unit]
        if outcome not in ('completed', 'failed', 'cancelled'):
            raise FleetError('invalid_record', 'gateway event must be terminal')
    if not deleted and role == 'quality':
        quality = number(quality, 'quality')
        if quality > 1:
            raise FleetError('invalid_record', 'quality must be in [0,1]')
    return {'producer': producer, 'event_id': event_id, 'sequence': sequence, 'request_id': request_id,
            'role': role, 'rank': specification['rank'], 'occurred': occurred, 'latency_ms': latency_ms,
            'outcome': outcome, 'quality': quality, 'deleted': deleted,
            'claimed_revision': record.get('revision'), 'sample_kind': record.get('sample_kind', 'live')}
