import sys
import unittest
from pathlib import Path
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src"))
from crawl_queue import GitQueue, initial_state, validate_state, validate_batch_group

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
    def test_multiple_native_metadata_refreshes_keep_distinct_raw_bodies(self):
        from batch_crawl import section_refresh_name
        first = section_refresh_name(6, 1, 1, 200)
        second = section_refresh_name(6, 1, 2, 200)
        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith("refresh/"))
        self.assertTrue(first.endswith(".body"))

    def test_heaviest_store_claimed_before_lexical_first(self):
        state = initial_state("r", "e", "sha", ["a", "z"], {"a": 100, "z": 40000})
        q = GitQueue(ROOT, "r")
        with patch.object(q, "transact", side_effect=lambda edit: edit(state)[1]):
            picked = q.claim("owner", 0, 1)
        self.assertEqual(picked[0][0], "z")
        validate_state(state)

    def test_category_batch_claim_fills_only_available_thread_slots(self):
        state = initial_state("r", "e", "sha", ["a"])
        q = GitQueue(ROOT, "r")
        with patch.object(q, "transact", side_effect=lambda edit: edit(state)[1]):
            _, lease = q.claim("original", 0, 1)[0]
            q.register_catalog("a", lease["token"], ["s0", "s1", "s2", "s3"])
            batch = q.claim_catalog_batch("helper", 1, "a", 2)
            self.assertEqual([row[1] for row in batch], ["0", "1"])
            self.assertEqual(state["catalog_groups"]["a"]["pending"], ["2", "3"])
            self.assertEqual({row[2]["worker"] for row in batch}, {1})

    def test_native_sections_fanout_finish_without_intermediate_artifacts(self):
        state = initial_state("r", "e", "sha", ["a"], {"a": 30000})
        q = GitQueue(ROOT, "r")
        with patch.object(q, "transact", side_effect=lambda edit: edit(state)[1]):
            sid, store_lease = q.claim("owner1", 0, 1)[0]
            self.assertEqual(sid, "a")
            self.assertEqual(q.register_catalog("a", store_lease["token"], ["cat0", "cat1"]), 2)
            first = q.claim_catalog("owner2", 1, "a")
            second = q.claim_catalog("owner3", 2, "a")
            self.assertEqual({first[1], second[1]}, {"0", "1"})
            q.mark_local_catalog_batch("owner2", {"a":{first[1]:first[2]["token"]}})
            q.mark_local_catalog_batch("owner3", {"a":{second[1]:second[2]["token"]}})
            self.assertEqual(state["catalog_groups"]["a"]["completed"], {})
            q.mark_local_batch("owner1", {"a":store_lease["token"]})
            self.assertEqual(state["leased"], {})
            q.finish_catalog_batch("owner2", {"a":{first[1]:first[2]["token"]}}, 123, "worker1")
            q.finish_catalog_batch("owner3", {"a":{second[1]:second[2]["token"]}}, 124, "worker2")
            q.finish_batch("owner1", {"a":store_lease["token"]}, 122, "worker0")
            self.assertEqual(len(state["catalog_groups"]["a"]["completed"]), 2)
            validate_batch_group("a", state["catalog_groups"]["a"])
            validate_state(state)

    def test_failed_wave_requeues_only_not_durable_native_sections(self):
        state = initial_state("r", "e", "sha", ["a"])
        q = GitQueue(ROOT, "r")
        with patch.object(q, "transact", side_effect=lambda edit: edit(state)[1]):
            sid, lease = q.claim("owner", 0, 1)[0]
            q.register_catalog(sid, lease["token"], ["s0", "s1"])
            first = q.claim_catalog("other", 1, sid)
            second = q.claim_catalog("other", 1, sid)
            q.mark_local_catalog_batch("other", {sid: {first[1]:first[2]["token"]}})
            q.finish_catalog_batch("other", {sid: {first[1]:first[2]["token"]}}, 99, "batch")
            self.assertEqual(q.reset_terminated_wave(), 2)
        group = state["catalog_groups"]["a"]
        self.assertEqual(group["pending"], [second[1]])
        self.assertEqual(set(group["completed"]), {first[1]})
        self.assertEqual(state["pending"], ["a"])

    def test_workflow_has_one_worker_upload_and_no_old_cleanup_job(self):
        text = (ROOT/".github/workflows/crawl.yml").read_text()
        first = text.split("  crawl_shard:",1)[1].split("  plan_recovery:",1)[0]
        self.assertEqual(first.count("uses: actions/upload-artifact@v4"),1)
        self.assertLess(first.index("python src/batch_crawl.py worker"), first.index("uses: actions/upload-artifact@v4"))
        self.assertNotIn("prepare_run:",text)
        self.assertNotIn("plan_wave_02:",text)
        self.assertIn("30 10,22 * * *",text)
        self.assertIn("python src/batch_crawl.py finalize",first)
