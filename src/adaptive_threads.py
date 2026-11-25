"""Bounded per-run adaptive store concurrency."""
from collections import deque
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
    window_seconds: float = 15
    minimum_successes: int = 12
    target: int = field(init=False)
    best: int | None = field(init=False, default=None)
    best_rate: float = field(init=False, default=0.0)
    baseline: dict = field(default_factory=dict)
    since: float = 0.0
    saturated: bool = True
    next_probe: float = 0.0
    direction: int = 1
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
        # Refresh the reference on every saturated window of the current
        # best setting. A morning peak must not dominate an hours-long run.
        if improved or equivalent_lower or self.target == self.best:
            self.best, self.best_rate = self.target, rate
        if now >= self.next_probe:
            if self.best is not None and self.target != self.best:
                self.direction = -1 if self.target > self.best else 1
                self.target = self.best
                self.next_probe = now + 120
            else:
                candidate = self.target + self.direction
                if not valid_threads(candidate):
                    self.direction *= -1
                    candidate = self.target + self.direction
                if valid_threads(candidate):
                    self.target = candidate
        self.reset(now, snapshot)

    def resume(self, prior, now, snapshot):
        """Carry learning across job rollover without carrying process clocks."""
        target=prior.get("target")
        best=prior.get("optimal_threads")
        rate=prior.get("best_rate")
        if not valid_threads(target) or (best is not None and not valid_threads(best)):
            raise ValueError("Invalid saved thread learning")
        if type(rate) not in (int,float) or not 0<=rate<1e6:
            raise ValueError("Invalid saved throughput")
        if prior.get("direction") not in (-1,1):
            raise ValueError("Invalid probe direction")
        delay=prior.get("probe_delay",0)
        if type(delay) not in (int,float) or not 0<=delay<=300:
            raise ValueError("Invalid saved probe delay")
        self.target,self.best,self.best_rate=target,best,rate
        self.direction=prior["direction"]
        self.next_probe=now+delay
        self.reset(now,snapshot)

    def report(self, now=None):
        if now is None:now=time.monotonic()
        return {"initial_threads": self.initial, "optimal_threads": self.best,
                "samples": self.samples,"target":self.target,"best_rate":self.best_rate,
                "direction":self.direction,"probe_delay":min(300,max(0,self.next_probe-now))}

def run_adaptive(items, capture, limiter, initial, on_result, clock=time.monotonic):
    """At most target whole stores are in flight. Never cancel a successful request."""
    ctrl = AdaptiveThreads(initial)
    pending = deque(items)
    active = {}
    ctrl.reset(clock(), limiter.snapshot())
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        while active or pending:
            snap = limiter.snapshot()
            # React to a newly observed throttle before admitting new work.
            if snap["rate_limit_events"] > ctrl.baseline["rate_limit_events"]:
                ctrl.observe(clock(), snap, False)
            if snap["cooldown_remaining_seconds"] <= 0:
                while pending and len(active) < ctrl.target:
                    item = pending.popleft()
                    active[pool.submit(capture, item)] = item
            if not active:
                if pending:
                    ctrl.observe(clock(), limiter.snapshot(), False)
                    time.sleep(min(1, max(.05, snap["cooldown_remaining_seconds"])))
                continue
            done, _ = wait(active, timeout=.25, return_when=FIRST_COMPLETED)
            ctrl.observe(clock(), limiter.snapshot(), len(active) >= ctrl.target and bool(pending))
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
