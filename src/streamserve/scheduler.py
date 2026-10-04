"""Weighted tenant round robin with separate queue and active KV budgets."""
from collections import Counter, deque
from .protocol import ServiceError


class Scheduler:
    def __init__(self, settings):
        self.settings = settings
        self.queues = {name: deque() for name in settings.tenants}
        self.ring = deque(name for name, spec in settings.tenants.items() for _ in range(spec.weight))
        self.active = {}
        self.by_tenant = Counter()
        self.used = 0

    def enqueue(self, request_id, tenant, reservation):
        if reservation > self.settings.kv_tokens:
            raise ServiceError('capacity', 'request exceeds total KV capacity', 429)
        queue = self.queues[tenant]
        if len(queue) >= self.settings.tenants[tenant].max_queued:
            raise ServiceError('queue_full', 'tenant queue is full', 429)
        queue.append((request_id, reservation))

    def take(self):
        if len(self.active) >= self.settings.max_active:
            return None
        for _ in range(len(self.ring)):
            tenant = self.ring[0]
            self.ring.rotate(-1)
            queue = self.queues[tenant]
            if not queue or self.by_tenant[tenant] >= self.settings.tenants[tenant].max_active:
                continue
            request_id, size = queue[0]
            if self.used + size > self.settings.kv_tokens:
                continue
            queue.popleft()
            self.active[request_id] = (tenant, size)
            self.used += size
            self.by_tenant[tenant] += 1
            return request_id
        return None

    def release(self, request_id):
        item = self.active.pop(request_id, None)
        if item is not None:
            tenant, size = item
            self.used -= size
            self.by_tenant[tenant] -= 1
        else:
            for queue in self.queues.values():
                for item in list(queue):
                    if item[0] == request_id:
                        queue.remove(item)

    def snapshot(self):
        return {'active': len(self.active), 'queued': sum(map(len, self.queues.values())),
                'kv_used': self.used, 'kv_capacity': self.settings.kv_tokens,
                'tenant_active': dict(self.by_tenant)}
