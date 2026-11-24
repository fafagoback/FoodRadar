"""GitHub-native dynamic microbatch queue; no external task database."""
from math import ceil
from heapq import heapify, heappop, heappush

MAX_PARALLEL = 15
MAX_BATCHES = 256

def plan_batches(store_ids, weights):
    ids = sorted(set(store_ids))
    if not ids:
        raise ValueError("No discovered stores")
    # About two waves at the current scope; larger nationwide runs use
    # progressively more chunks, bounded by the native matrix limit.
    count = min(len(ids), MAX_BATCHES, max(MAX_PARALLEL, ceil(len(ids) / 12)))
    estimated = {sid: max(1, int(weights.get(sid, 400))) for sid in ids}
    heap = [(0, 0, i) for i in range(count)]
    heapify(heap)
    batches = [[] for _ in range(count)]
    for sid in sorted(ids, key=lambda x: (-estimated[x], x)):
        load, n, idx = heappop(heap)
        batches[idx].append(sid)
        heappush(heap, (load + estimated[sid], n + 1, idx))
    # Native matrix creates heavy jobs first; unstarted jobs are allocated
    # to the next free slot automatically, never by Git/API writes.
    batches.sort(key=lambda group: (-sum(estimated[s] for s in group), group[0]))
    owners = {sid: idx for idx, group in enumerate(batches) for sid in group}
    return owners, count, estimated
