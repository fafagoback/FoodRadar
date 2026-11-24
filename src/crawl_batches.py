"""Deterministic workload distribution across exactly 15 long-lived crawl jobs."""
from heapq import heapify, heappop, heappush

MAX_PARALLEL = 15
MAX_BATCHES = MAX_PARALLEL  # internal compatibility: one partition per worker


def plan_batches(store_ids, weights):
    ids = sorted(set(store_ids))
    if not ids:
        raise ValueError("No discovered stores")
    count = MAX_PARALLEL
    estimated = {sid: max(1, int(weights.get(sid, 400))) for sid in ids}
    heap = [(0, 0, i) for i in range(count)]
    heapify(heap)
    owners = {}
    for sid in sorted(ids, key=lambda x: (-estimated[x], x)):
        load, n, idx = heappop(heap)
        owners[sid] = idx
        heappush(heap, (load + estimated[sid], n + 1, idx))
    # Empty partitions are valid for tiny test rosters. Worker IDs are stable.
    return owners, count, estimated
