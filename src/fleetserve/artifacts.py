"""Content-addressed model metadata; CPU artifacts stand in for immutable weights."""
import hashlib
import json
from pathlib import Path
from streamserve import ModelSpec
from .errors import FleetError
from .util import canonical, identifier, integer


class ArtifactRegistry:
    def __init__(self, store):
        self.store = store

    def register(self, manifest, content):
        revision = identifier(manifest['revision'], 'revision')
        spec = ModelSpec(revision, manifest['tokenizer'], integer(manifest['context_limit'], 'context_limit', 2))
        spec.validate()
        actual = hashlib.sha256(content).hexdigest()
        if manifest.get('sha256') != actual:
            raise FleetError('artifact_digest', 'artifact content does not match manifest', 422)
        normalized = {**spec.wire(), 'sha256': actual}
        with self.store.transaction():
            old = self.store.one('SELECT * FROM artifacts WHERE revision=?', (revision,))
            if old and old['payload'] != canonical(normalized):
                raise FleetError('revision_conflict', 'revision identity is immutable', 409)
            if not old:
                self.store.execute('INSERT INTO artifacts VALUES (?,?,?,?,?)',
                                   (revision, actual, spec.tokenizer, spec.context_limit, canonical(normalized)))
        return normalized

    def import_manifest(self, path):
        path = Path(path)
        manifest = json.loads(path.read_text(encoding='utf-8'))
        content_path = (path.parent / manifest['content']).resolve()
        return self.register(manifest, content_path.read_bytes())

    def spec(self, revision):
        row = self.store.one('SELECT * FROM artifacts WHERE revision=?', (revision,))
        if row is None:
            raise FleetError('unknown_revision', revision, 404)
        return ModelSpec(revision, row['tokenizer'], row['context_limit'])

    def list(self):
        return [json.loads(row['payload']) for row in self.store.all('SELECT payload FROM artifacts ORDER BY revision')]
