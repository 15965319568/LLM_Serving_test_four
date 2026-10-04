"""Durable delivery reconciliation over configured producer partitions."""
import csv
import gzip
import json
from pathlib import Path
import sqlite3
from streamserve.ownership import JournalOwner
from ..errors import FleetError
from ..telemetry.formats import normalize
from ..util import canonical,digest,identifier,integer,timestamp


class DeliveryInbox:
    def __init__(self,fleet,partitions,path=None):
        self.fleet=fleet
        self.partitions=dict(partitions)
        if not self.partitions:raise FleetError('invalid_partitions')
        for partition,producer in self.partitions.items():
            identifier(partition,'partition')
            if producer not in fleet.config['telemetry']['producers']:raise FleetError('unknown_producer')
        path=Path(path or fleet.state_dir/'delivery.sqlite3');path.parent.mkdir(parents=True,exist_ok=True)
        self.owner=JournalOwner(path)
        self.db=sqlite3.connect(path,isolation_level=None);self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deliveries(partition TEXT,offset INTEGER,fingerprint TEXT,
                payload TEXT,state TEXT,reason TEXT,PRIMARY KEY(partition,offset));
            CREATE TABLE IF NOT EXISTS marks(partition TEXT,cohort TEXT,through REAL,PRIMARY KEY(partition,cohort));
        ''')
        old=self.db.execute("SELECT value FROM metadata WHERE key='partitions'").fetchone()
        if old and json.loads(old[0])!=self.partitions:
            self.db.close();self.owner.close();raise FleetError('partition_registry_conflict',status=409)
        self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('partitions',?)",(canonical(self.partitions),))

    def receive(self,partition,records):
        if partition not in self.partitions:raise FleetError('unknown_partition',status=404)
        if not isinstance(records,list) or len(records)>10000:raise FleetError('invalid_batch')
        result=dict(inserted=0,duplicates=0,rejected=[])
        for index,record in enumerate(records):
            try:
                if not isinstance(record,dict) or record.get('schema')!='delivery/1':raise FleetError('invalid_delivery')
                offset=integer(record['offset'],'offset')
                if record['kind'] not in ('event','watermark'):raise FleetError('invalid_delivery')
                if not isinstance(record['body'],dict):raise FleetError('invalid_delivery')
                if record['kind']=='event':identifier(record['admission_id'],'admission_id')
                fingerprint=digest(record)
                old=self.db.execute('SELECT fingerprint FROM deliveries WHERE partition=? AND offset=?',(partition,offset)).fetchone()
                if old:
                    if old[0]!=fingerprint:raise FleetError('delivery_conflict',status=409)
                    result['duplicates']+=1
                else:
                    self.db.execute("INSERT INTO deliveries VALUES (?,?,?,?,'pending',NULL)",
                                    (partition,offset,fingerprint,canonical(record)))
                    result['inserted']+=1
            except (FleetError,KeyError,TypeError,ValueError) as error:
                result['rejected'].append(dict(index=index,code=getattr(error,'code','invalid_delivery')))
        return result

    def frontier(self,partition):
        expected=0
        for row in self.db.execute('SELECT offset,state FROM deliveries WHERE partition=? ORDER BY offset',(partition,)):
            expected=max(expected,row['offset']+1)
        return expected

    def _event(self,partition,envelope):
        producer=self.partitions[partition]
        specification=self.fleet.config['telemetry']['producers'][producer]
        payload=dict(envelope['body'])
        field='request' if specification['format']=='export-v1' else 'request_id'
        payload.setdefault(field,envelope['admission_id'])
        normalize(payload,producer,specification)
        known=self.fleet.store.one('SELECT request_id FROM receipts WHERE request_id=?',(envelope['admission_id'],))
        if known is None:return True
        result=self.fleet.evidence.ingest(producer,[payload])
        if result['rejected']:raise FleetError(result['rejected'][0]['code'])
        return True

    def _publish_marks(self):
        rows=[dict(r) for r in self.db.execute('SELECT * FROM marks')]
        for producer in sorted(set(self.partitions.values())):
            sources={p for p,value in self.partitions.items() if value==producer}
            for cohort in sorted({r['cohort'] for r in rows}):
                values=[r['through'] for r in rows if r['partition'] in sources and r['cohort']==cohort]
                if values:
                    self.fleet.evidence.watermark(producer,cohort,max(values))

    def reconcile(self,after_apply=None):
        applied=rejected=0
        for row in list(self.db.execute("SELECT * FROM deliveries WHERE state='pending' ORDER BY partition,offset")):
            envelope=json.loads(row['payload'])
            try:
                if envelope['kind']=='event':
                    if not self._event(row['partition'],envelope):continue
                else:
                    if self.frontier(row['partition'])<row['offset']:continue
                    mark=envelope['body'];identifier(mark['cohort'],'cohort')
                    through=timestamp(mark['through'])
                    self.db.execute('''INSERT INTO marks VALUES (?,?,?) ON CONFLICT(partition,cohort)
                        DO UPDATE SET through=MAX(through,excluded.through)''',(row['partition'],mark['cohort'],through))
                if after_apply:after_apply(dict(row))
                self.db.execute("UPDATE deliveries SET state='applied',reason=NULL WHERE partition=? AND offset=?",
                                (row['partition'],row['offset']))
                applied+=1
            except (FleetError,KeyError,TypeError,ValueError) as error:
                self.db.execute("UPDATE deliveries SET state='rejected',reason=? WHERE partition=? AND offset=?",
                                (getattr(error,'code','invalid_record'),row['partition'],row['offset']))
                rejected+=1
        self._publish_marks()
        return dict(applied=applied,rejected=rejected,partitions=self.status())

    def status(self):
        return [dict(partition=p,producer=self.partitions[p],next_offset=self.frontier(p),
            pending=self.db.execute("SELECT COUNT(*) FROM deliveries WHERE partition=? AND state='pending'",(p,)).fetchone()[0],
            rejected=self.db.execute("SELECT COUNT(*) FROM deliveries WHERE partition=? AND state='rejected'",(p,)).fetchone()[0])
            for p in sorted(self.partitions)]

    def audit(self):
        return [dict(row) for row in self.db.execute('SELECT partition,offset,state,reason,payload FROM deliveries ORDER BY partition,offset')]

    def import_manifest(self,path):
        path=Path(path).resolve();manifest=json.loads(path.read_text(encoding='utf-8-sig'))
        result=[]
        for source in manifest['files']:
            file=(path.parent/source['path']).resolve()
            if not file.is_relative_to(path.parent):raise FleetError('invalid_capture_path')
            partition=source['partition']
            if partition not in self.partitions:raise FleetError('unknown_partition')
            opener=gzip.open if file.suffix=='.gz' else open
            inserted=duplicates=0;errors=[]
            with opener(file,'rt',encoding='utf-8-sig',newline='') as stream:
                if source.get('format','jsonl')=='csv':
                    for number,row in enumerate(csv.DictReader(stream),2):
                        try:
                            envelope=dict(schema=row['schema'],offset=int(row['offset']),kind=row['kind'],
                                          body=json.loads(row['body']),admission_id=row.get('admission_id',''))
                            outcome=self.receive(partition,[envelope])
                        except (KeyError,TypeError,ValueError):outcome=dict(inserted=0,duplicates=0,rejected=[{'code':'invalid_csv'}])
                        inserted+=outcome['inserted'];duplicates+=outcome['duplicates']
                        errors.extend(dict(line=number,code=e['code']) for e in outcome['rejected'])
                else:
                    for number,line in enumerate(stream,1):
                        if not line.strip():continue
                        try:outcome=self.receive(partition,[json.loads(line)])
                        except ValueError:outcome=dict(inserted=0,duplicates=0,rejected=[{'code':'invalid_json'}])
                        inserted+=outcome['inserted'];duplicates+=outcome['duplicates']
                        errors.extend(dict(line=number,code=e['code']) for e in outcome['rejected'])
            result.append(dict(path=source['path'],partition=partition,inserted=inserted,duplicates=duplicates,rejected=errors))
        self.reconcile()
        return result

    def close(self):
        try:self.db.close()
        finally:self.owner.close()
