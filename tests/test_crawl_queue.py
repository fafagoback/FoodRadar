import concurrent.futures
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from crawl_queue import GitQueue, initial_state, validate_state, artifact_name


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = self.root / "remote.git"
        subprocess.run(["git", "init", "--bare", str(self.remote)],
                       capture_output=True, check=True)

    def client(self, n, ttl=180):
        p = self.root / ("worker-" + str(n))
        subprocess.run(["git", "init", str(p)], capture_output=True, check=True)
        subprocess.run(["git", "-C", str(p), "remote", "add", "origin", str(self.remote)],
                       capture_output=True, check=True)
        return GitQueue(p, "123", lease_seconds=ttl)

    def test_fifteen_contending_workers_never_duplicate_task(self):
        clients = [self.client(i) for i in range(15)]
        ids = ["shop-%02d" % n for n in range(45)]
        clients[0].initialize(initial_state("123", "epoch", "sha", ids))
        def worker(i):
            q = clients[i]
            accepted = []
            while True:
                claims = q.claim("worker-" + str(i), i, 2)
                if not claims:
                    return accepted
                for sid, lease in claims:
                    self.assertTrue(q.finish(sid, lease["token"], int(sid[-2:]) + 1))
                    accepted.append(sid)
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as pool:
            accepted = sum(pool.map(worker, range(15)), [])
        self.assertEqual(sorted(accepted), sorted(ids))
        state = clients[0].read()
        self.assertFalse(state["pending"])
        self.assertFalse(state["leased"])
        self.assertEqual(set(state["completed"]), set(ids))

    def test_saved_result_survives_crashed_worker_and_stale_token_is_fenced(self):
        a, b = self.client(0, ttl=.15), self.client(1)
        a.initialize(initial_state("123", "epoch", "sha", ["a", "b"]))
        claims = dict(a.claim("dead-worker", 0, 2))
        state = a.read()
        uploaded = {artifact_name(state, "a", claims["a"]): 101}
        time.sleep(.25)
        b.recover_expired(uploaded.get)
        state = b.read()
        self.assertEqual(state["completed"]["a"]["artifact_id"], 101)
        self.assertEqual(state["pending"], ["b"])
        [(sid, lease)] = b.claim("new-worker", 1, 1)
        self.assertEqual(sid, "b")
        self.assertFalse(a.finish("b", claims["b"]["token"], 102))
        self.assertTrue(b.finish("b", lease["token"], 103))
        self.assertEqual(b.read()["completed"]["b"]["artifact_id"], 103)

    def test_heartbeat_and_storage_failure_do_not_reassign(self):
        a = self.client(0)
        a.initialize(initial_state("123", "epoch", "sha", ["x"]))
        a.claim("live", 0, 1)
        self.assertEqual(a.renew("live"), 1)
        self.assertFalse(a.claim("other", 1, 1))
        def broken(_):
            raise RuntimeError("storage unavailable")
        a.recover_expired(broken)
        self.assertEqual(len(a.read()["leased"]), 1)

    def test_bad_roster_is_rejected(self):
        s = initial_state("123", "epoch", "sha", ["a"])
        s["pending"].append("unexpected")
        with self.assertRaises(ValueError):
            validate_state(s)


if __name__ == "__main__":
    unittest.main()
