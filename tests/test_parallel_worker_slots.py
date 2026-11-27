"""Verify every idle worker slot is refilled independently at learned sizes."""
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from batch_crawl import capture_worker
from crawl_queue import initial_state


class ParallelWorkerSlotTests(unittest.TestCase):
    def run_slots(self, parallel):
        ids = [f"s{i:02d}" for i in range(parallel + 2)]
        state = initial_state("run", "epoch", "sha", ids)
        all_started = threading.Barrier(parallel, timeout=15)
        next_started = threading.Event()
        long_running_saw_next = threading.Event()
        long_sid = ids[parallel - 1]
        next_sid = ids[parallel]
        started = []
        lock = threading.Lock()

        class Queue:
            def __init__(self):
                self.pending = ids.copy()
                self.claimed = set()
                self.reported = set()
                self.lock = threading.Lock()
                self.max_claim_slots = 0

            def read(self):
                return state

            def exchange(self, owner, worker, slots, stores, sections):
                with self.lock:
                    assert not sections
                    assert 0 <= slots <= 12, "Supabase exchange allows at most 12 slots"
                    self.max_claim_slots = max(slots, self.max_claim_slots)
                    for sid in stores:
                        assert sid in self.claimed and sid not in self.reported
                        self.reported.add(sid)
                    jobs = []
                    while slots and self.pending:
                        sid = self.pending.pop(0)
                        self.claimed.add(sid)
                        jobs.append(("store", sid, None, {"token": sid * 16, "worker": 0}))
                        slots -= 1
                    return jobs, not self.pending and len(self.reported) == len(ids)

        q = Queue()
        def capture(item, raw, session_factory=None):
            sid = item["store_uuid"]
            with lock:
                started.append(sid)
            if sid in ids[:parallel]:
                all_started.wait()
            if sid == long_sid:
                if next_started.wait(10):
                    long_running_saw_next.set()
            if sid == next_sid:
                next_started.set()
            return {"store_uuid": sid}

        snapshot = {"successes": 0, "rate_limit_events": 0,
                    "cooldown_remaining_seconds": 0, "interval_seconds": 0.18}
        with tempfile.TemporaryDirectory() as tmp, \
             patch("batch_crawl.shared_identity", return_value=(
                 {"run_id":"run","source_sha":"sha"}, "epoch", ids)), \
             patch("batch_crawl.verify_queue"), \
             patch("batch_crawl.load_shared_discovery", return_value=(
                 {sid: {"store_uuid": sid} for sid in ids}, 1, "epoch", None)), \
             patch("batch_crawl.validate_task"), \
             patch("batch_crawl.public_ip", return_value="test"), \
             patch("batch_crawl.TransportPool"), \
             patch("batch_crawl.global_limiter") as limiter, \
             patch.dict("os.environ", {"FOODRADAR_INITIAL_THREADS": str(parallel)}):
            limiter.snapshot.return_value = snapshot
            summary = capture_worker({}, 0, Path(tmp)/"discovery.json", queue=q,
                                     root=Path(tmp), capture=capture)
            saved = json.loads((Path(tmp)/"data/worker-batch-0/batch.json").read_text())
        self.assertTrue(next_started.is_set())
        self.assertTrue(long_running_saw_next.is_set(),
                        f"{parallel} slots did not refill while a slower task continued")
        self.assertEqual(set(started), set(ids))
        self.assertEqual(len(started), len(ids))
        self.assertEqual(set(saved["records"]), set(ids))
        self.assertEqual(summary["completed"], len(ids))
        self.assertEqual(summary["thread_learning"]["initial_threads"], parallel)
        self.assertLessEqual(q.max_claim_slots, 12)

    def test_independent_refill_for_dynamic_worker_counts(self):
        for parallel in (4, 8, 16, 32, 64, 96):
            with self.subTest(worker_threads=parallel):
                self.run_slots(parallel)

    def test_reject_invalid_worker_thread_limit(self):
        from adaptive_threads import valid_threads
        self.assertFalse(valid_threads(0))
        self.assertTrue(valid_threads(65))
        self.assertTrue(valid_threads(128))


if __name__ == "__main__":
    unittest.main()
