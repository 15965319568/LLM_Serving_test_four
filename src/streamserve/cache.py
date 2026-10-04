"""Tenant-isolated prefix cache with pinned entries and LRU eviction."""
from collections import OrderedDict
from dataclasses import dataclass


@dataclass
class Entry:
    tokens: tuple
    pins: int = 0


class PrefixCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.entries = OrderedDict()
        self.hits = 0
        self.misses = 0

    @property
    def used(self):
        return sum(len(e.tokens) for e in self.entries.values())

    def acquire(self, tenant, revision, tokenizer, tokens):
        key = (tenant, revision, tokenizer, tuple(tokens))
        entry = self.entries.get(key)
        if entry is not None:
            self.hits += 1
            self.entries.move_to_end(key)
            entry.pins += 1
            return key
        self.misses += 1
        if len(tokens) > self.capacity:
            return None
        for victim in list(self.entries):
            if self.used + len(tokens) <= self.capacity:
                break
            if self.entries[victim].pins == 0:
                del self.entries[victim]
        if self.used + len(tokens) > self.capacity:
            return None
        self.entries[key] = Entry(tuple(tokens), 1)
        return key

    def release(self, key):
        if key is None:
            return
        entry = self.entries[key]
        if entry.pins <= 0:
            raise RuntimeError('prefix released twice')
        entry.pins -= 1

    def retire(self, revision):
        for key in list(self.entries):
            if key[1] == revision and self.entries[key].pins == 0:
                del self.entries[key]

    def snapshot(self):
        return {'used': self.used, 'capacity': self.capacity, 'hits': self.hits,
                'misses': self.misses, 'pinned': sum(e.pins for e in self.entries.values()),
                'revisions': sorted({key[1] for key in self.entries})}
