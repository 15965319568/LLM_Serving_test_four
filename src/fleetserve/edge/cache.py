import json
from pathlib import Path
import sqlite3
from ..errors import FleetError
from ..util import canonical, digest


class EdgeCache:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS routes(alias TEXT PRIMARY KEY, epoch INTEGER NOT NULL, plan TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS applied(effect_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL);
        ''')

    def apply(self, effect_id, alias, epoch, plan):
        fingerprint = digest([alias, epoch, plan])
        self.db.execute('BEGIN IMMEDIATE')
        try:
            old = self.db.execute('SELECT * FROM applied WHERE effect_id=?', (effect_id,)).fetchone()
            if old:
                if old['fingerprint'] != fingerprint:
                    raise FleetError('effect_conflict', 'effect identity reused', 409)
                result = json.loads(old['result'])
            else:
                route = self.db.execute('SELECT * FROM routes WHERE alias=?', (alias,)).fetchone()
                previous = route['epoch'] if route else -1
                if epoch <= previous:
                    self.db.execute('INSERT OR REPLACE INTO routes VALUES (?,?,?)', (alias, epoch, canonical(plan)))
                    result = {'applied': True, 'epoch': epoch, 'stale': False}
                elif epoch != previous + 1:
                    raise FleetError('edge_epoch_gap', 'earlier route effect is missing', 409)
                else:
                    self.db.execute('INSERT OR REPLACE INTO routes VALUES (?,?,?)', (alias, epoch, canonical(plan)))
                    result = {'applied': True, 'epoch': epoch, 'stale': False}
                self.db.execute('INSERT INTO applied VALUES (?,?,?)', (effect_id, fingerprint, canonical(result)))
            self.db.execute('COMMIT')
            return result
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def get(self, alias):
        row = self.db.execute('SELECT * FROM routes WHERE alias=?', (alias,)).fetchone()
        if row is None:
            raise FleetError('route_unavailable', alias, 503)
        return {'alias': alias, 'epoch': row['epoch'], 'plan': json.loads(row['plan'])}

    def list(self):
        return [self.get(row[0]) for row in self.db.execute('SELECT alias FROM routes ORDER BY alias')]

    def close(self):
        self.db.close()
