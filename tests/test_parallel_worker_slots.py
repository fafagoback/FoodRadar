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
    def test_next_store_starts_while_other_store_is_still_running(self):
        ids = ["a", "b", "c", "d", "e"]
        state = initial_state("run", "epoch", "sha", ids)
        e_started = threading.Event()
        d_saw_e = threading.Event()
        started = []
        lock = threading.Lock()

        class Queue:
            def __init__(self):
                self.pending = ids.copy()
                self.claimed = []
                self.reported = set()
                self.lock = threading.Lock()

            def read(self):
                return state

            def exchange(self, owner, worker, slots, stores, sections):
                with self.lock:
                    assert not sections
                    for sid in stores:
                        assert sid in self.claimed and sid not in self.reported
                        self.reported.add(sid)
                    jobs = []
                    while slots and self.pending:
                        sid = self.pending.pop(0)
                        self.claimed.append(sid)
                        jobs.append(("store", sid, None, {"token": sid * 32, "worker": 0}))
                        slots -= 1
                    return jobs, not self.pending and len(self.reported) == len(ids)

        q = Queue()

        def capture(item, raw, session_factory=None):
            sid = item["store_uuid"]
            with lock:
                started.append(sid)
            if sid == "d":
                if e_started.wait(4):
                    d_saw_e.set()
            if sid == "e":
                e_started.set()
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
             patch.dict("os.environ", {"FOODRADAR_INITIAL_THREADS": "4"}):
            limiter.snapshot.return_value = snapshot
            summary = capture_worker({}, 0, Path(tmp)/"discovery.json", queue=q,
                                     root=Path(tmp), capture=capture)
            saved = json.loads((Path(tmp)/"data/worker-batch-0/batch.json").read_text())

        self.assertTrue(e_started.is_set())
        self.assertTrue(d_saw_e.is_set(), "Store d must still be active when e starts")
        self.assertEqual(set(started), set(ids))
        self.assertEqual(len(started), len(ids))
        self.assertEqual(set(saved["records"]), set(ids))
        self.assertEqual(summary["completed"], len(ids))
        self.assertEqual(summary["thread_learning"]["initial_threads"], 4)


if __name__ == "__main__":
    unittest.main()
