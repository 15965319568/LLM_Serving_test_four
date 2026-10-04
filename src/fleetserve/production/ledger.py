"""Business admissions survive independently of the model's execution journal."""
import json
from pathlib import Path
import sqlite3
from streamserve.ownership import JournalOwner
from ..errors import FleetError
from ..util import canonical


class AdmissionLedger:
    def __init__(self,path):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.owner=JournalOwner(path)
        self.db=sqlite3.connect(path,isolation_level=None)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS admissions(
            id TEXT PRIMARY KEY,tenant TEXT NOT NULL,request_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL,admission TEXT NOT NULL,state TEXT NOT NULL,
            response TEXT,error TEXT,finished REAL,latency_ms REAL,
            UNIQUE(tenant,request_key))''')

    def decode(self,row):
        if row is None: return None
        return {**json.loads(row['admission']),**{key:row[key] for key in
            ('state','error','finished','latency_ms')},
            'response':json.loads(row['response']) if row['response'] else None}

    def replay(self,tenant,key,fingerprint):
        row=self.db.execute('SELECT * FROM admissions WHERE request_key=?',(key,)).fetchone()
        if row and row['fingerprint']!=fingerprint:
            raise FleetError('idempotency_conflict',status=409)
        return self.decode(row)

    def add(self,row,fingerprint):
        self.db.execute('INSERT INTO admissions VALUES (?,?,?,?,?,\'active\',NULL,NULL,NULL,NULL)',
                        (row['id'],row['tenant'],row['key'],fingerprint,canonical(row)))
        return self.get(row['tenant'],row['id'])

    def get(self,tenant,identity):
        row=self.db.execute('SELECT * FROM admissions WHERE id=? AND tenant=?',(identity,tenant)).fetchone()
        if row is None: raise FleetError('not_found',status=404)
        return self.decode(row)

    def finish(self,identity,state,now,latency_ms=0,response=None,error=None):
        self.db.execute('''UPDATE admissions SET state=?,finished=?,latency_ms=?,response=?,error=?
            WHERE id=? AND state='active' ''',
            (state,now,latency_ms,canonical(response) if response is not None else None,error,identity))

    def rows(self,tenant=None):
        sql='SELECT * FROM admissions';args=()
        if tenant is not None: sql+=' WHERE tenant=?';args=(tenant,)
        return [self.decode(row) for row in self.db.execute(sql+' ORDER BY rowid',args)]

    def reserved(self,worker,generation):
        return sum(r['state']=='active' and r['worker']==worker and r['worker_generation']==generation for r in self.rows())

    def recover(self,now):
        return self.rows()

    def close(self):
        try: self.db.close()
        finally: self.owner.close()
