"""The control database's on-disk contract. Edge and request journals are separate."""
from .errors import FleetError


V1 = '''
CREATE TABLE artifacts(revision TEXT PRIMARY KEY, digest TEXT NOT NULL,
 tokenizer TEXT NOT NULL, context_limit INTEGER NOT NULL, payload TEXT NOT NULL);
CREATE TABLE leases(worker TEXT PRIMARY KEY, generation INTEGER NOT NULL, sequence INTEGER NOT NULL,
 pool TEXT NOT NULL, revision TEXT NOT NULL, capacity INTEGER NOT NULL, free INTEGER NOT NULL,
 expires REAL NOT NULL, state TEXT NOT NULL, tokenizer TEXT NOT NULL);
CREATE TABLE routes(alias TEXT PRIMARY KEY, epoch INTEGER NOT NULL, plan TEXT NOT NULL);
CREATE TABLE operations(id TEXT PRIMARY KEY, scope TEXT NOT NULL, fingerprint TEXT NOT NULL, result TEXT NOT NULL);
CREATE TABLE releases(id TEXT PRIMARY KEY, alias TEXT NOT NULL, phase TEXT NOT NULL,
 route_epoch INTEGER NOT NULL, stable_plan TEXT NOT NULL, candidate_plan TEXT NOT NULL,
 policy TEXT NOT NULL, created REAL NOT NULL, decision TEXT);
CREATE TABLE effects(id TEXT PRIMARY KEY, alias TEXT NOT NULL, epoch INTEGER NOT NULL,
 plan TEXT NOT NULL, release_id TEXT, target_phase TEXT, status TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT);
CREATE TABLE bindings(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, request_key TEXT NOT NULL,
 fingerprint TEXT NOT NULL, request_json TEXT NOT NULL, alias TEXT NOT NULL,
 revision TEXT NOT NULL, pool TEXT NOT NULL, worker TEXT NOT NULL, generation INTEGER NOT NULL,
 route_epoch INTEGER NOT NULL, engine_id TEXT, state TEXT NOT NULL, created REAL NOT NULL,
 completion_tokens INTEGER NOT NULL DEFAULT 0, terminal_reason TEXT,
 UNIQUE(tenant,request_key));
CREATE TABLE receipts(request_id TEXT PRIMARY KEY, tenant TEXT NOT NULL, alias TEXT NOT NULL,
 revision TEXT NOT NULL, cohort TEXT NOT NULL, admitted REAL NOT NULL, kind TEXT NOT NULL);
CREATE TABLE raw_events(producer TEXT NOT NULL, event_id TEXT NOT NULL, update_seq INTEGER NOT NULL,
 fingerprint TEXT NOT NULL, payload TEXT NOT NULL, received REAL NOT NULL, conflict INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(producer,event_id));
CREATE TABLE watermarks(producer TEXT NOT NULL, cohort TEXT NOT NULL, through_time REAL NOT NULL,
 PRIMARY KEY(producer,cohort));
CREATE TABLE assessments(id TEXT PRIMARY KEY, release_id TEXT NOT NULL, route_epoch INTEGER NOT NULL,
 start REAL NOT NULL, end REAL NOT NULL, evidence_digest TEXT NOT NULL, evidence_version INTEGER NOT NULL,
 decision TEXT NOT NULL, payload TEXT NOT NULL);
CREATE TABLE audit(seq INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, subject TEXT NOT NULL,
 payload TEXT NOT NULL, created REAL NOT NULL);
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT INTO meta VALUES ('evidence_version','0');
'''


def migrate(db):
    version = db.execute('PRAGMA user_version').fetchone()[0]
    if version > 3:
        raise FleetError('unsupported_schema', 'control database is newer than this binary', 409)
    if version == 0:
        db.executescript('BEGIN IMMEDIATE;\n'+V1+'\nPRAGMA user_version=1;\nCOMMIT;')
        version = 1
    if version == 1:
        # Version 1 used receipt row arrival time as its only provenance field.
        db.executescript('''BEGIN IMMEDIATE;
        ALTER TABLE receipts ADD COLUMN source TEXT NOT NULL DEFAULT 'gateway';
        CREATE INDEX bindings_active ON bindings(worker,generation,state);
        CREATE INDEX receipts_window ON receipts(alias,revision,admitted);
        CREATE INDEX pending_effects ON effects(status,alias,epoch);
        PRAGMA user_version=2;
        COMMIT;''')

        version = 2
    if version == 2:
        db.executescript('''BEGIN IMMEDIATE;
        CREATE TABLE rollout_progress(release_id TEXT PRIMARY KEY,route_epoch INTEGER NOT NULL,
            stage_index INTEGER NOT NULL,healthy_windows INTEGER NOT NULL,last_end REAL,effective_at REAL);
        CREATE TABLE rollout_windows(id INTEGER PRIMARY KEY AUTOINCREMENT,release_id TEXT NOT NULL,
            route_epoch INTEGER NOT NULL,start REAL NOT NULL,end REAL NOT NULL,assessment_id TEXT NOT NULL,
            decision TEXT NOT NULL,counted INTEGER NOT NULL,
            UNIQUE(release_id,route_epoch,start,end));
        PRAGMA user_version=3;
        COMMIT;''')
