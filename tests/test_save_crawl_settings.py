"""Supabase is the sole rolling setting; no samples must leave it untouched."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import save_crawl_settings as saver

class SaveCrawlSettingsTests(unittest.TestCase):
    def exercise(self, values, initial):
        with tempfile.TemporaryDirectory() as tmp:
            files = []
            for i, value in enumerate(values):
                path = Path(tmp) / str(i)
                path.write_text(json.dumps({"shard_index":i,"run_id":"run","source_sha":"sha",
                    "stores":[{"success":i != 0}],"thread_learning":{"optimal_threads":value}}))
                files.append(path)
            state = {"optimal_threads":initial}
            queue = Mock()
            def rpc(op, **args):
                if op == "threads_write":
                    state.update(args)
                elif op != "threads_read":
                    raise AssertionError(op)
                return dict(state)
            queue._rpc.side_effect = rpc
            with patch.dict(os.environ, {"GITHUB_RUN_ID":"run","GITHUB_SHA":"sha",
                    "GITHUB_REPOSITORY":"owner/repo","FOODRADAR_BATCH_COUNT":"15"}), \
                 patch.object(Path,"glob",return_value=files), \
                 patch.object(saver,"settings_queue",return_value=queue):
                saver.main()
            return state["optimal_threads"], queue

    def test_maximum_updates_supabase_and_can_decrease(self):
        for initial in (4,32):
            value, q = self.exercise([16]+[4]*13+[None], initial)
            self.assertEqual(value,16)
            q._rpc.assert_any_call("threads_write",optimal_threads=16)
            self.assertEqual(q._rpc.call_count,3)

    def test_no_sample_keeps_current_value(self):
        value,q=self.exercise([None]*15,32)
        self.assertEqual(value,32)
        q._rpc.assert_not_called()

    def test_invalid_setting_fails_not_falls_back(self):
        q=Mock()
        q._rpc.return_value={"optimal_threads":0}
        with self.assertRaises(ValueError):
            saver.read_setting(q)

if __name__ == "__main__":
    unittest.main()
