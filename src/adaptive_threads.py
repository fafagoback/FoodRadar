"""Per-worker exponential search followed by integer bisection.

Only measure when work is available. A real upstream throttle is a failed
probe; insufficient work is not. Keep the HTTP limiter and Retry-After intact.
"""
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
import json
import time

MIN_THREADS = 1
MAX_THREADS = 64
DEFAULT_THREADS = 4
REPROBE_SECONDS = 300

def valid_threads(value):
    return type(value) is int and MIN_THREADS <= value <= MAX_THREADS

@dataclass
class AdaptiveThreads:
    initial: int
    window_seconds: float = 10
    minimum_successes: int = 12
    target: int = field(init=False)
    lower: int = 0
    upper: int | None = None
    best: int | None = None
    best_rate: float = 0.0
    baseline: dict = field(default_factory=dict)
    last_tick: float = 0.0
    saturated: bool = False
    busy_seconds: float = 0.0
    busy_successes: int = 0
    next_probe: float = 0.0
    settling: bool = False
    samples: list = field(default_factory=list)
    events: list = field(default_factory=list)

    def __post_init__(self):
        if not valid_threads(self.initial):
            raise ValueError("invalid initial thread count")
        self.target = self.initial

    def reset(self, now, snapshot):
        self.baseline = dict(snapshot)
        self.last_tick = now
        self.busy_seconds = 0.0
        self.busy_successes = 0
        self.saturated = False

    def _log(self, tested, outcome, reason):
        event = {"tested": tested, "result": outcome, "reason": reason,
                 "lower": self.lower, "upper": self.upper, "next": self.target}
        self.events.append(event)
        print("FOODRADAR_THREAD_PROBE=" + json.dumps(event), flush=True)

    def _choose(self, now):
        if self.upper is None:
            self.target = min(MAX_THREADS, max(MIN_THREADS, self.lower * 2))
        elif self.lower == 0:
            self.target = max(MIN_THREADS, self.upper // 2)
        elif self.upper - self.lower > 1:
            self.target = (self.lower + self.upper) // 2
        else:
            self.target = self.lower
        if (self.lower == MAX_THREADS or
                (self.lower and self.upper is not None and self.upper-self.lower == 1)):
            self.next_probe = now + REPROBE_SECONDS

    def observe(self, now, snapshot, saturated, active_count=None):
        if not self.baseline:
            self.reset(now, snapshot)
            self.saturated = saturated
            return
        throttled = snapshot["rate_limit_events"] > self.baseline["rate_limit_events"]
        cooling = snapshot["cooldown_remaining_seconds"] > 0
        if throttled:
            # Multiple responses from the same in-flight cohort are one failed
            # probe, not repeated halving of a setting not yet tested.
            if not self.settling:
                tested = self.target
                self.upper = min(self.upper, tested) if self.upper is not None else tested
                if self.lower >= tested:
                    self.lower = 0
                    self.best = None
                    self.best_rate = 0.0
                self._choose(now)
                self.settling = True
                self._log(tested, "failed", "upstream_429")
            self.reset(now, snapshot)
            return
        if cooling or (self.settling and active_count is not None and active_count > self.target):
            self.reset(now, snapshot)
            return
        if self.settling:
            self.settling = False
            self.reset(now, snapshot)
            self.saturated = saturated
            return

        # Accumulate *busy* intervals; queue handoffs and the tail do not
        # erase already collected evidence or count as failed probes.
        if self.saturated:
            self.busy_seconds += max(0.0, now-self.last_tick)
            self.busy_successes += max(0, snapshot["successes"]-self.baseline["successes"])
        self.last_tick = now
        self.baseline = dict(snapshot)
        self.saturated = saturated
        if not saturated or self.busy_seconds < self.window_seconds or self.busy_successes < self.minimum_successes:
            return

        tested = self.target
        rate = self.busy_successes / max(self.busy_seconds, 1e-9)
        self.samples.append({"threads": tested, "successes": self.busy_successes,
                             "seconds": round(self.busy_seconds, 3), "rate": round(rate, 4),
                             "saturated_seconds": round(self.busy_seconds, 3),
                             "limiter_interval": snapshot["interval_seconds"]})
        # No arbitrary 5% throughput improvement requirement. The API pacing
        # ceiling can mask increases in available parallelism.
        if self.upper == tested:
            # At the minimum after a throttle, or a later re-probe of the old
            # boundary. Fresh successful evidence supersedes that boundary.
            self.upper = None
        self.lower = max(self.lower, tested)
        self.best, self.best_rate = self.lower, rate
        converged = self.lower == MAX_THREADS or (self.upper is not None and self.upper-self.lower == 1)
        if converged and tested == self.lower:
            if self.next_probe and now >= self.next_probe and self.lower < MAX_THREADS:
                self.target = self.upper
                self.next_probe = 0.0
            elif not self.next_probe:
                self.next_probe = now + REPROBE_SECONDS
        else:
            self._choose(now)
        if tested != self.target or len(self.samples) == 1:
            self._log(tested, "passed", "stable_busy_window")
        self.reset(now, snapshot)

    def resume(self, prior, now, snapshot):
        target = prior.get("target")
        lower = prior.get("lower", prior.get("optimal_threads") or 0)
        upper = prior.get("upper")
        best = prior.get("optimal_threads")
        rate = prior.get("best_rate", 0.0)
        delay = prior.get("probe_delay", 0.0)
        if (not valid_threads(target) or type(lower) is not int or not 0 <= lower <= MAX_THREADS
                or (upper is not None and (not valid_threads(upper) or upper <= lower))
                or best != (lower or None) or type(rate) not in (int, float) or not 0 <= rate < 1e6
                or type(delay) not in (int, float) or not 0 <= delay <= REPROBE_SECONDS):
            raise ValueError("Invalid saved thread learning")
        self.target, self.lower, self.upper, self.best, self.best_rate = target, lower, upper, best, rate
        self.next_probe = now + delay if delay else 0.0
        # New runner/IP: only current-process observations can confirm a
        # candidate again. Historic samples are merged in worker_reports.
        self.settling = False
        self.reset(now, snapshot)

    def report(self, now=None):
        if now is None:
            now = time.monotonic()
        return {"initial_threads": self.initial, "optimal_threads": self.best,
                "samples": self.samples, "events": self.events, "target": self.target,
                "lower": self.lower, "upper": self.upper, "best_rate": self.best_rate,
                "probe_delay": min(REPROBE_SECONDS, max(0, self.next_probe-now))}

def run_adaptive(items, capture, limiter, initial, on_result, clock=time.monotonic):
    ctrl = AdaptiveThreads(initial)
    pending = deque(items)
    active = {}
    ctrl.reset(clock(), limiter.snapshot())
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        while active or pending:
            snap = limiter.snapshot()
            ctrl.observe(clock(), snap, len(active) >= ctrl.target, len(active))
            if snap["cooldown_remaining_seconds"] <= 0:
                while pending and len(active) < ctrl.target:
                    item = pending.popleft()
                    active[pool.submit(capture, item)] = item
            if not active:
                if pending:
                    time.sleep(min(1, max(.05, snap["cooldown_remaining_seconds"])))
                continue
            done, _ = wait(active, timeout=.25, return_when=FIRST_COMPLETED)
            ctrl.observe(clock(), limiter.snapshot(), len(active) >= ctrl.target, len(active))
            for future in done:
                active.pop(future)
                on_result(future.result())
    return ctrl.report()

def maximum_best(reports):
    values = [r.get("optimal_threads") for r in reports]
    values = [v for v in values if valid_threads(v)]
    return max(values, default=None)
