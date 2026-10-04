"""Additive v1-to-v2 migration and transactional deployment persistence."""
import json
from .config import ModelSpec
from .journal import Journal
from .ownership import JournalOwner
from .protocol import Event


class DurableJournal(Journal):
    def __init__(self, path, settings):
        self.owner = JournalOwner(path)
        try:
            super().__init__(path)
            with self.transaction():
                columns = {row['name'] for row in self.db.execute('PRAGMA table_info(requests)')}
                if 'ttft' not in columns:
                    self.db.execute('ALTER TABLE requests ADD COLUMN ttft REAL')
                self.db.execute('CREATE TABLE IF NOT EXISTS revisions(revision TEXT PRIMARY KEY,spec TEXT NOT NULL)')
                self.db.execute('CREATE TABLE IF NOT EXISTS deployments(alias TEXT PRIMARY KEY,revision TEXT NOT NULL REFERENCES revisions(revision),epoch INTEGER NOT NULL)')
                self.db.execute('CREATE TABLE IF NOT EXISTS activation_ops(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,result TEXT NOT NULL)')
                for alias, spec in settings.models.items():
                    row = self.db.execute('SELECT alias FROM deployments WHERE alias=?', (alias,)).fetchone()
                    if row is None:
                        self.db.execute('INSERT OR IGNORE INTO revisions VALUES (?,?)', (spec.revision, json.dumps(spec.wire(), sort_keys=True)))
                        self.db.execute('INSERT INTO deployments VALUES (?,?,0)', (alias, spec.revision))
                self.db.execute('PRAGMA user_version=2')
        except BaseException:
            if hasattr(self, 'db'):
                self.db.close()
            self.owner.close()
            raise

    def revisions(self):
        return {row['revision']: ModelSpec(**json.loads(row['spec'])) for row in self.db.execute('SELECT * FROM revisions')}

    def deployments(self):
        return {row['alias']: (row['revision'], row['epoch']) for row in self.db.execute('SELECT * FROM deployments')}

    def operation(self, operation_id):
        row = self.db.execute('SELECT * FROM activation_ops WHERE id=?', (operation_id,)).fetchone()
        return dict(row) if row else None

    def commit_deployment(self, alias, spec, epoch, operation_id, fingerprint, result):
        with self.transaction():
            self.db.execute('INSERT OR IGNORE INTO revisions VALUES (?,?)', (spec.revision, json.dumps(spec.wire(), sort_keys=True)))
            self.db.execute('UPDATE deployments SET revision=?,epoch=? WHERE alias=?', (spec.revision, epoch, alias))
            self.db.execute('INSERT INTO activation_ops VALUES (?,?,?)', (operation_id, fingerprint, json.dumps(result, sort_keys=True)))

    def token(self, request_id, text, ttft=None):
        with self.transaction():
            self.db.execute('UPDATE requests SET output_tokens=output_tokens+1,ttft=COALESCE(ttft,?) WHERE id=?', (ttft, request_id))
            if text:
                self._append(request_id, 'delta', {'text': text})

    def events(self, request_id, after=0, limit=32):
        return [Event(request_id, row['seq'], row['kind'], json.loads(row['data']))
                for row in self.db.execute('SELECT * FROM events WHERE request_id=? AND seq>? ORDER BY seq LIMIT ?',
                                           (request_id, after, limit))]

    def close(self):
        try:
            super().close()
        finally:
            self.owner.close()
