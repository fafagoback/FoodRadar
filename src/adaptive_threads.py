"""Bounded per-run adaptive store concurrency."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
import time

MIN_THREADS = 1
MAX_THREADS = 12
DEFAULT_THREADS = 2

def valid_threads(value):
    return type(value) is int and MIN_THREADS <= value <= MAX_THREADS

@dataclass
class AdaptiveThreads:
    initial: int
    window_seconds: float = 30
    minimum_successes: int = 20
    target: int = field(init=False)
    best: int | None = field(init=False, default=None)
    best_rate: float = field(init=False, default=0.0)
    baseline: dict = field(default_factory=dict)
    since: float = 0.0
    saturated: bool = True
    next_probe: float = 0.0
    samples: list = field(default_factory=list)

    def __post_init__(self):
        if not valid_threads(self.initial):
            raise ValueError("invalid initial thread count")
        self.target = self.initial

    def reset(self, now, snapshot):
        self.since = now
        self.baseline = dict(snapshot)
        self.saturated = True

    def observe(self, now, snapshot, saturated):
        if not self.baseline:
            self.reset(now, snapshot)
            return
        if snapshot["rate_limit_events"] > self.baseline["rate_limit_events"]:
            self.target = max(MIN_THREADS, self.target // 2)
            if self.best is not None and self.best > self.target:
                self.best = None
                self.best_rate = 0.0
            self.next_probe = now + max(60, snapshot["cooldown_remaining_seconds"])
            self.reset(now, snapshot)
            self.saturated = False
            return
        if snapshot["cooldown_remaining_seconds"] > 0 or snapshot["interval_seconds"] > 0.180001:
            self.reset(now, snapshot)
            self.saturated = False
            return
        self.saturated = self.saturated and saturated
        elapsed = now - self.since
        if elapsed < self.window_seconds:
            return
        successes = snapshot["successes"] - self.baseline["successes"]
        if not self.saturated or successes < self.minimum_successes:
            self.reset(now, snapshot)
            return
        rate = successes / elapsed
        self.samples.append({"threads": self.target, "successes": successes,
                             "seconds": round(elapsed, 2), "rate": round(rate, 4)})
        improved = self.best is None or rate > self.best_rate * 1.05
        equivalent_lower = self.best is not None and self.target < self.best and rate >= self.best_rate * 0.95
        if improved or equivalent_lower:
            self.best, self.best_rate = self.target, rate
        if now >= self.next_probe:
            if self.best is not None and self.target > self.best and not improved:
                self.target = self.best
                self.next_probe = now + 120
            elif self.target < MAX_THREADS:
                self.target += 1
        self.reset(now, snapshot)

    def report(self):
        return {"initial_threads": self.initial, "optimal_threads": self.best,
                "samples": self.samples}

def run_adaptive(items, capture, limiter, initial, on_result, clock=time.monotonic):
    """At most target whole stores are in flight. Never cancel a successful request."""
    ctrl = AdaptiveThreads(initial)
    pending = iter(items)
    exhausted = False
    active = {}
    ctrl.reset(clock(), limiter.snapshot())
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        while active or not exhausted:
            snap = limiter.snapshot()
            # Assess occupancy before freeing completed futures.
            if active:
                ctrl.observe(clock(), snap, len(active) >= ctrl.target and not exhausted)
            if snap["cooldown_remaining_seconds"] <= 0:
                while not exhausted and len(active) < ctrl.target:
                    try:
                        item = next(pending)
                    except StopIteration:
                        exhausted = True
                        break
                    active[pool.submit(capture, item)] = item
            if not active:
                if not exhausted:
                    time.sleep(min(1, max(.05, snap["cooldown_remaining_seconds"])))
                continue
            done, _ = wait(active, timeout=.25, return_when=FIRST_COMPLETED)
            ctrl.observe(clock(), limiter.snapshot(), len(active) >= ctrl.target and not exhausted)
            for future in done:
                active.pop(future)
                on_result(future.result())
    return ctrl.report()

def average_best(reports):
    values = [r.get("optimal_threads") for r in reports]
    values = [v for v in values if valid_threads(v)]
    if not values:
        return None
    return (2 * sum(values) + len(values)) // (2 * len(values))
