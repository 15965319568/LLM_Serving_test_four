"""The fixed revision catalogue used by the original single-deployment service."""
from .protocol import ServiceError


class Catalog:
    def __init__(self, models):
        self.aliases = dict(models)

    def resolve(self, alias):
        if alias not in self.aliases:
            raise ServiceError('unknown_model', 'model alias is not configured', 404)
        return self.aliases[alias]

    def list(self):
        return [{'id': name, **spec.wire()} for name, spec in sorted(self.aliases.items())]

    def revisions(self):
        return {spec.revision: spec for spec in self.aliases.values()}
