"""Measure whole-store throughput and learn parallel store slots without a fixed cap.

Each slot works on one complete store. A finished slot is immediately reused;
no other store has to finish first. The learning objective is completed stores
per measured busy second, not successful HTTP requests. An observed 429 or a
non-improving throughput probe brackets the search and triggers integer bisection.
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
                self._reject(self.target, "upstream_429")
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

        if self.saturated:
            self.busy_seconds += max(0.0, now - self.last_tick)
            self.busy_successes += max(0, completed(snapshot) - completed(self.baseline))
        self.last_tick = now
        self.baseline = dict(snapshot)
        self.saturated = saturated
        # The sample size follows the tested worker count: each busy slot
        # needs a chance to complete at least one whole store.
        if not saturated or self.busy_seconds <= 0 or self.busy_successes < self.target:
            return

        tested = self.target
        rate = self.busy_successes / self.busy_seconds
        self.samples.append({"threads": tested, "completed_stores": self.busy_successes,
                             "seconds": round(self.busy_seconds, 3),
                             "stores_per_second": round(rate, 6),
                             "saturated_seconds": round(self.busy_seconds, 3),
                             "limiter_interval": snapshot["interval_seconds"]})
        # Do not increase merely because the API answered successfully.
        # A tie or slowdown means extra slots did not finish stores faster.
        if self.lower and tested > self.lower and rate <= self.best_rate:
            self._reject(tested, "throughput_not_improved")
        else:
            if self.upper == tested:
                self.upper = None
            self.lower = max(self.lower, tested)
            self.best = self.lower
            self.best_rate = rate
            self._choose()
            self._log(tested, "passed", "higher_completed_store_throughput")
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
        self.best_rate = rate if prior.get("throughput_unit") == "stores_per_second" else 0.0
        self.settling = False
        self.reset(now, snapshot)

    def report(self, now=None):
        return {"initial_threads": self.initial, "optimal_threads": self.best,
                "throughput_unit": "stores_per_second",
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
