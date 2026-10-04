"""Durable public deployment identity and per-request outcomes."""
import json
from pathlib import Path
import sqlite3
import time
from streamserve.ownership import JournalOwner
from ..errors import FleetError
from ..util import canonical


class FabricJournal:
    def __init__(self,path):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.owner = JournalOwner(path)
        self.db = sqlite3.connect(path,isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS deployments(alias TEXT PRIMARY KEY,epoch INTEGER NOT NULL,
                generation TEXT NOT NULL,spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY,tenant TEXT NOT NULL,alias TEXT NOT NULL,
                external_id TEXT,epoch INTEGER NOT NULL,revision TEXT NOT NULL,generation TEXT NOT NULL,
                rows INTEGER NOT NULL,state TEXT NOT NULL,worker_pid INTEGER,invocation TEXT,error TEXT,
                admitted REAL NOT NULL,finished REAL);
        ''')

    def deployments(self):
        return [dict(row,spec=json.loads(row['spec'])) for row in self.db.execute('SELECT * FROM deployments ORDER BY alias')]

    def operation(self,identity,fingerprint):
        row=self.db.execute('SELECT * FROM operations WHERE id=?',(identity,)).fetchone()
        if row:
            if row['fingerprint']!=fingerprint:
                raise FleetError('operation_conflict',status=409)
            return json.loads(row['result'])

    def commit(self,deployment,operation,fingerprint):
        result={k:deployment[k] for k in ['alias','epoch','generation']}
        result['revision']=deployment['spec']['revision']
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('INSERT OR REPLACE INTO deployments VALUES (?,?,?,?)',
                            (deployment['alias'],deployment['epoch'],deployment['generation'],canonical(deployment['spec'])))
            self.db.execute('INSERT INTO operations VALUES (?,?,?)',(operation,fingerprint,canonical(result)))
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        return result

    def admit(self,identity,tenant,deployment,request,rows):
        self.db.execute('INSERT INTO requests VALUES (?,?,?,?,?,?,?,?,\'active\',NULL,NULL,NULL,?,NULL)',
            (identity,tenant,deployment['alias'],request.id,deployment['epoch'],deployment['spec']['revision'],
             deployment['generation'],rows,time.time()))

    def finish(self,identity,state,response=None,error=None):
        params=response.parameters if response is not None else None
        self.db.execute('UPDATE requests SET state=?,worker_pid=?,invocation=?,error=?,finished=? WHERE id=?',
            (state,getattr(params,'worker_pid',None),getattr(params,'invocation',None),error,time.time(),identity))

    def recover(self):
        return  # The pilot reopens the existing receipt ledger.

    def receipts(self,tenant=None):
        if tenant is None:
            return [dict(r) for r in self.db.execute('SELECT * FROM requests ORDER BY admitted,id')]
        return [dict(r) for r in self.db.execute('SELECT * FROM requests WHERE tenant=? ORDER BY admitted,id',(tenant,))]

    def accounting(self):
        return [dict(r) for r in self.db.execute('''SELECT tenant,alias,epoch,revision,COUNT(*) AS admitted,
            SUM(state='active') AS active,SUM(state='completed') AS completed,SUM(state='failed') AS failed,
            SUM(state='cancelled') AS cancelled,SUM(CASE WHEN state='completed' THEN rows ELSE 0 END) AS completed_rows
            FROM requests GROUP BY tenant,alias,epoch,revision ORDER BY tenant,alias,epoch''')]

    def close(self):
        try:
            self.db.close()
        finally:
            self.owner.close()
