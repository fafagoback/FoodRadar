import sys
import unittest
from pathlib import Path
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src"))
from crawl_queue import GitQueue

class BatchCrawlTests(unittest.TestCase):
    def test_one_artifact_commits_multiple_stores(self):
        state = {"completed": {}, "leased": {"a":{"owner":"w","worker":0,"token":"a"},
                 "b":{"owner":"w","worker":0,"token":"b"}}}
        q = GitQueue(ROOT, "test")
        with patch.object(q, "transact", side_effect=lambda edit: edit(state)[1]):
            self.assertEqual(q.finish_batch("w", {"a":"a","b":"b"}, 42, "batch"), 2)
            self.assertEqual(q.finish_batch("w", {"a":"a","b":"b"}, 42, "batch"), 2)
            self.assertEqual(state["leased"], {})
            self.assertEqual({v["artifact_id"] for v in state["completed"].values()}, {42})
            with self.assertRaises(ValueError):
                q.finish_batch("w", {"a":"bad"}, 42, "batch")
    def test_reset_releases_only_unfinished(self):
        state = {"completed":{"c":{}}, "leased":{"a":{}}, "pending":["b"]}
        q = GitQueue(ROOT, "test")
        with patch.object(q, "transact", side_effect=lambda edit: edit(state)[1]):
            self.assertEqual(q.reset_terminated_wave(), 1)
        self.assertEqual(state["pending"], ["a","b"])
        self.assertEqual(state["completed"], {"c":{}})
    def test_workflow_has_one_worker_upload_and_no_old_cleanup_job(self):
        text = (ROOT/".github/workflows/crawl.yml").read_text()
        first = text.split("  crawl_shard:",1)[1].split("  plan_recovery:",1)[0]
        self.assertEqual(first.count("uses: actions/upload-artifact@v4"),1)
        self.assertLess(first.index("python src/batch_crawl.py worker"), first.index("uses: actions/upload-artifact@v4"))
        self.assertNotIn("prepare_run:",text)
        self.assertNotIn("plan_wave_02:",text)
        self.assertIn("30 10,22 * * *",text)
        self.assertIn("python src/batch_crawl.py finalize",first)
