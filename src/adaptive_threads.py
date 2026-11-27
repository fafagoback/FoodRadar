"""Learn parallel store slots from observed request successes and upstream 429.
No fixed thread ceiling, time window, store count or throughput gate.
Only real work supplies evidence; cooldown and in-flight work are respected.
"""
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
import json
import time

MIN_THREADS = 1
DEFAULT_THREADS = 4


def valid_threads(value):
    return type(value) is int and value >= MIN_THREADS


def completed(snapshot):
    # Older diagnostic callers have only request-success counters. Live
    # whole-store runners always pass completed_stores explicitly.
    return int(snapshot.get("completed_stores", snapshot.get("successes", 0)))


@dataclass
class AdaptiveThreads:
    initial: int
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

    def _choose(self):
        if self.upper is None:
            self.target = max(MIN_THREADS, self.lower * 2)
        elif self.lower == 0:
            self.target = max(MIN_THREADS, self.upper // 2)
        elif self.upper - self.lower > 1:
            self.target = (self.lower + self.upper) // 2
        else:
            self.target = self.lower

    def _reject(self, tested, reason):
        self.upper = min(self.upper, tested) if self.upper is not None else tested
        if self.lower >= tested:
            self.lower = 0
            self.best = None
            self.best_rate = 0.0
        self._choose()
        self._log(tested, "failed", reason)

    def observe(self, now, snapshot, saturated, active_count=None):
        if not self.baseline:
            self.reset(now, snapshot)
            self.saturated = saturated
            return
        throttled = snapshot["rate_limit_events"] > self.baseline["rate_limit_events"]
        cooling = snapshot["cooldown_remaining_seconds"] > 0
        if throttled:
            if not self.settling:
                tested = min(self.target, max(1, active_count)) if active_count is not None else self.target
                self._reject(tested, "upstream_429")
                self.settling = True
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

        # A successful HTTP response during an observed full allocation is
        # evidence for this tested count. No fixed sample/time or throughput gate.
        # A partially allocated target is not credited just because it was set.
        successes = max(0, snapshot.get("successes", 0) - self.baseline.get("successes", 0))
        was_saturated = self.saturated
        self.last_tick = now
        self.baseline = dict(snapshot)
        self.saturated = saturated
        if not (was_saturated or saturated) or not successes:
            return
        tested = self.target
        if self.best == tested and self.upper is not None and self.upper - self.lower <= 1:
            return
        self.samples.append({"threads": tested, "successful_requests": successes,
                             "rate_limit_events": 0})
        self.lower = max(self.lower, tested)
        self.best = self.lower
        self._choose()
        self._log(tested, "passed", "observed_work_without_429")
        self.reset(now, snapshot)

    def resume(self, prior, now, snapshot):
        target = prior.get("target")
        lower = prior.get("lower", prior.get("optimal_threads") or 0)
        upper = prior.get("upper")
        best = prior.get("optimal_threads")
        rate = prior.get("best_rate", 0.0)
        if (not valid_threads(target) or type(lower) is not int or lower < 0
                or (upper is not None and (not valid_threads(upper) or upper <= lower))
                or best != (lower or None)
                or type(rate) not in (int, float) or not 0 <= rate < float("inf")):
            raise ValueError("Invalid saved thread learning")
        self.target, self.lower, self.upper, self.best = target, lower, upper, best
        # Earlier saves measured API successes/s rather than completed stores/s.
        self.best_rate = 0.0
        self.settling = False
        self.reset(now, snapshot)

    def report(self, now=None):
        return {"initial_threads": self.initial, "optimal_threads": self.best,
                "throughput_unit": "429_free_parallel_slots",
                "samples": self.samples, "events": self.events, "target": self.target,
                "lower": self.lower, "upper": self.upper, "best_rate": self.best_rate}


def run_adaptive(items, capture, limiter, initial, on_result, clock=time.monotonic):
    ctrl = AdaptiveThreads(initial)
    pending = deque(items)
    active = {}
    count = 0
    def snap():
        return dict(limiter.snapshot(), completed_stores=count)
    ctrl.reset(clock(), snap())
    # The workload bounds the number of useful workers; there is no invented
    # fixed upper thread count and ThreadPoolExecutor creates threads lazily.
    with ThreadPoolExecutor(max_workers=max(1, len(pending))) as pool:
        while active or pending:
            current = snap()
            ctrl.observe(clock(), current, len(active) >= ctrl.target, len(active))
            if current["cooldown_remaining_seconds"] <= 0:
                while pending and len(active) < ctrl.target:
                    item = pending.popleft()
                    active[pool.submit(capture, item)] = item
            if not active:
                if pending:
                    time.sleep(min(1, max(.05, current["cooldown_remaining_seconds"])))
                continue
            done, _ = wait(active, timeout=.25, return_when=FIRST_COMPLETED)
            ctrl.observe(clock(), snap(), len(active) >= ctrl.target, len(active))
            for future in done:
                active.pop(future)
                result = future.result()
                count += 1
                on_result(result)
    return ctrl.report()


def maximum_best(reports):
    values = [r.get("optimal_threads") for r in reports]
    values = [v for v in values if valid_threads(v)]
    return max(values, default=None)
