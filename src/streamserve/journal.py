"""SQLite request journal, schema version 1.

Each request is owned by one process. SQLite transactions atomically append
events and update accounting. JSON payloads here are request data, never SQL.
"""
from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3
import time
from .protocol import Event


class Journal:
    def __init__(self, path):
        self.path = str(path)
        if self.path != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS requests (
          id TEXT PRIMARY KEY, tenant TEXT NOT NULL, request_key TEXT NOT NULL,
          fingerprint TEXT NOT NULL, request_json TEXT NOT NULL,
          revision TEXT NOT NULL, state TEXT NOT NULL,
          prompt_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL DEFAULT 0,
          error TEXT, created REAL NOT NULL, finished REAL,
          UNIQUE(tenant, request_key));
        CREATE TABLE IF NOT EXISTS events (
          request_id TEXT NOT NULL REFERENCES requests(id), seq INTEGER NOT NULL,
          kind TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(request_id, seq));
        ''')
        if self.db.execute('PRAGMA user_version').fetchone()[0] == 0:
            self.db.execute('PRAGMA user_version=1')

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def _append(self, request_id, kind, data):
        seq = self.db.execute('SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE request_id=?', (request_id,)).fetchone()[0]
        self.db.execute('INSERT INTO events VALUES (?,?,?,?)', (request_id, seq, kind, json.dumps(data, ensure_ascii=False, sort_keys=True)))
        return Event(request_id, seq, kind, data)

    def create(self, ticket, request, prompt_tokens):
        with self.transaction():
            self.db.execute('INSERT INTO requests(id,tenant,request_key,fingerprint,request_json,revision,state,prompt_tokens,created) VALUES (?,?,?,?,?,?,?,?,?)',
                            (ticket.request_id, request.tenant, request.key, request.fingerprint(),
                             json.dumps(request.wire(), ensure_ascii=False), ticket.revision, 'queued', prompt_tokens, time.time()))
            self._append(ticket.request_id, 'accepted', {'revision': ticket.revision, 'model': request.model})

    def get(self, request_id):
        row = self.db.execute('SELECT * FROM requests WHERE id=?', (request_id,)).fetchone()
        return dict(row) if row else None

    def find(self, tenant, key):
        row = self.db.execute('SELECT * FROM requests WHERE tenant=? AND request_key=?', (tenant, key)).fetchone()
        return dict(row) if row else None

    def all(self):
        return [dict(row) for row in self.db.execute('SELECT * FROM requests ORDER BY created,id')]

    def running(self, request_id):
        self.db.execute("UPDATE requests SET state='running' WHERE id=? AND state='queued'", (request_id,))

    def token(self, request_id, text):
        with self.transaction():
            self.db.execute('UPDATE requests SET output_tokens=output_tokens+1 WHERE id=?', (request_id,))
            if text:
                self._append(request_id, 'delta', {'text': text})

    def delta(self, request_id, text):
        if text:
            with self.transaction():
                self._append(request_id, 'delta', {'text': text})

    def terminal(self, request_id, state, reason):
        with self.transaction():
            row = self.get(request_id)
            if row['state'] not in ('queued', 'running'):
                return False
            self.db.execute('UPDATE requests SET state=?,error=?,finished=? WHERE id=?',
                            (state, reason if state != 'completed' else None, time.time(), request_id))
            self._append(request_id, 'terminal', {'state': state, 'reason': reason,
                        'usage': {'prompt_tokens': row['prompt_tokens'], 'completion_tokens': row['output_tokens']}})
            return True

    def events(self, request_id, after=0):
        return [Event(request_id, row['seq'], row['kind'], json.loads(row['data']))
                for row in self.db.execute('SELECT * FROM events WHERE request_id=? AND seq>? ORDER BY seq', (request_id, after))]

    def close(self):
        self.db.close()
