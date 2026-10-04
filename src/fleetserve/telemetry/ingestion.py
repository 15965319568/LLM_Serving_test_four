import json
from ..errors import FleetError
from ..util import canonical, digest, identifier, timestamp
from .formats import normalize


class EvidenceStore:
    def __init__(self, store, config, clock):
        self.store, self.config, self.clock = store, config, clock

    def version(self):
        return int(self.store.one("SELECT value FROM meta WHERE key='evidence_version'")['value'])

    def bump(self):
        self.store.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='evidence_version'")

    def receipt(self, value):
        fields = ('request_id','tenant','alias','revision','cohort','admitted','kind','source')
        value = {**value, 'source': value.get('source', 'gateway')}
        for key in ('request_id','tenant','alias','revision','cohort'):
            identifier(value[key], key)
        value['admitted'] = timestamp(value['admitted'])
        if value['tenant'] not in self.config['tenants'] or value['alias'] not in self.config['aliases']:
            raise FleetError('unknown_receipt_scope')
        if value['kind'] not in ('live','shadow','replay') or value['source'] != 'gateway':
            raise FleetError('invalid_receipt')
        with self.store.transaction():
            old = self.store.one('SELECT * FROM receipts WHERE request_id=?', (value['request_id'],))
            if old:
                if any(old[key] != value[key] for key in fields):
                    raise FleetError('receipt_conflict', 'admission identity is immutable', 409)
                return False
            self.store.execute('INSERT INTO receipts VALUES (?,?,?,?,?,?,?,?)', tuple(value[key] for key in fields))
            self.bump()
        return True

    def ingest(self, producer, records):
        specification = self.config['telemetry']['producers'].get(producer)
        if specification is None:
            raise FleetError('unknown_producer', producer, 404)
        if not isinstance(records, list) or len(records) > 10000:
            raise FleetError('invalid_batch')
        result = {'inserted': 0, 'updated': 0, 'duplicates': 0, 'older': 0, 'conflicts': 0, 'rejected': []}
        with self.store.transaction():
            changed = False
            for index, raw in enumerate(records):
                try:
                    value = normalize(raw, producer, specification)
                except (FleetError, KeyError, TypeError, ValueError) as error:
                    result['rejected'].append({'index': index, 'code': getattr(error, 'code', 'invalid_record')})
                    continue
                fingerprint = digest(value)
                old = self.store.one('SELECT * FROM raw_events WHERE producer=? AND event_id=?', (producer, value['event_id']))
                if old and value['sequence'] == old['update_seq']:
                    if old['fingerprint'] == fingerprint:
                        result['duplicates'] += 1
                    else:
                        result['conflicts'] += 1
                        if not old['conflict']:
                            self.store.execute('UPDATE raw_events SET conflict=1 WHERE producer=? AND event_id=?', (producer, value['event_id']))
                            changed = True
                else:
                    self.store.execute('INSERT OR REPLACE INTO raw_events VALUES (?,?,?,?,?,?,0)',
                                       (producer, value['event_id'], value['sequence'], fingerprint, canonical(value), self.clock()))
                    result['updated' if old else 'inserted'] += 1
                    changed = True
            if changed:
                self.bump()
        return {**result, 'evidence_version': self.version()}

    def watermark(self, producer, cohort, through):
        if producer not in self.config['telemetry']['producers']:
            raise FleetError('unknown_producer', producer, 404)
        identifier(cohort, 'cohort')
        through = timestamp(through)
        with self.store.transaction():
            old = self.store.one('SELECT * FROM watermarks WHERE producer=? AND cohort=?', (producer, cohort))
            if old and old['through_time'] >= through:
                return False
            self.store.execute('INSERT OR REPLACE INTO watermarks VALUES (?,?,?)', (producer, cohort, through))
            self.bump()
        return True

    def export_raw(self):
        return [{**json.loads(row['payload']), 'conflict': bool(row['conflict'])}
                for row in self.store.all('SELECT * FROM raw_events ORDER BY producer,event_id')]
