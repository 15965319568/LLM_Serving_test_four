"""One connection per owner, short explicit transactions, additive schema migrations."""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from .errors import FleetError
from .migrations import migrate
from .util import canonical


class Database:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.path), isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        migrate(self.db)

    def one(self, sql, values=()):
        row = self.db.execute(sql, values).fetchone()
        return dict(row) if row is not None else None

    def all(self, sql, values=()):
        return [dict(row) for row in self.db.execute(sql, values)]

    def execute(self, sql, values=()):
        return self.db.execute(sql, values)

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield self
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def operation(self, operation_id, scope, fingerprint):
        previous = self.one('SELECT * FROM operations WHERE id=?', (operation_id,))
        if previous and (previous['scope'] != scope or previous['fingerprint'] != fingerprint):
            raise FleetError('operation_conflict', 'operation id already has another intent', 409)
        return previous

    def remember(self, operation_id, scope, fingerprint, result):
        self.execute('INSERT INTO operations VALUES (?,?,?,?)',
                     (operation_id, scope, fingerprint, canonical(result)))

    def audit(self, action, subject, payload, now):
        self.execute('INSERT INTO audit(action,subject,payload,created) VALUES (?,?,?,?)',
                     (action, subject, canonical(payload), now))

    def close(self):
        self.db.close()
