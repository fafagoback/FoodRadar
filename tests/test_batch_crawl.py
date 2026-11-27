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

    def test_late_worker_joins_existing_native_catalog_group(self):
        from batch_crawl import assert_supported_catalog_groups
        assert_supported_catalog_groups({"catalog_groups":{"store":{"format":"batch-section-v2"}}})
        with self.assertRaises(ValueError):
            assert_supported_catalog_groups({"catalog_groups":{"store":{"format":"old"}}})

    def test_new_stores_and_existing_categories_share_work_with_bounded_groups(self):
        from crawl_queue import prefer_new_store
        state = {"pending":["new"], "leased":{},
                 "catalog_groups":{"old":{"format":"batch-section-v2",
                     "pending":["0","1"],"leased":{"2":{}}, "sections":["a","b","c"]}}}
        self.assertTrue(prefer_new_store(state))
        state["leased"] = {str(i):{} for i in range(5)}
        self.assertFalse(prefer_new_store(state))
        state["leased"] = {}
        state["catalog_groups"] = {
            str(i): {"format":"batch-section-v2","pending":["0"],
                     "leased":{}, "sections":["a"]} for i in range(16)}
        self.assertFalse(prefer_new_store(state))
        state["catalog_groups"] = {}
        self.assertTrue(prefer_new_store(state))

    def test_cross_worker_split_only_for_high_cost_or_very_wide_catalog(self):
        from batch_crawl import should_split_catalog
        self.assertTrue(should_split_catalog(24, 208306))
        self.assertTrue(should_split_catalog(8, 61000))
        self.assertTrue(should_split_catalog(40, 400))
        self.assertFalse(should_split_catalog(19, 18404))
        self.assertFalse(should_split_catalog(33, 52994))
        self.assertTrue(should_split_catalog(7, 200000))
        self.assertTrue(should_split_catalog(1, 200000))
        self.assertTrue(should_split_catalog(1, 400, 81))
        self.assertFalse(should_split_catalog(0, 200000))

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

    def test_store_only_worker_reports_before_claiming_next_store(self):
        import json
        import tempfile
        from batch_crawl import capture_worker
        from unittest.mock import Mock
        state = initial_state("r", "epoch", "sha", ["a", "b"])
        class Queue:
            def __init__(self):
                self.claimed = []
                self.reported = []
            def read(self):
                return state
            def exchange(self, owner, worker, slots, stores, sections):
                self_outer.assertLessEqual(slots, 4)
                self_outer.assertEqual(sections, {})
                self.reported.extend(stores)
                if slots and len(self.claimed) < 2:
                    self_outer.assertTrue(set(self.reported).issubset(set(self.claimed)))
                    sid = ["a", "b"][len(self.claimed)]
                    self.claimed.append(sid)
                    return [("store", sid, None, {"token": ("a" if sid=="a" else "b")*32, "worker": 0})], False
                return [], len(self.reported)==2
        self_outer = self
        q = Queue()
        def capture(item, raw, session_factory=None):
            self.assertIn(item["store_uuid"], q.claimed)
            return {"store_uuid":item["store_uuid"]}
        with tempfile.TemporaryDirectory() as tmp, patch("batch_crawl.shared_identity", return_value=({"run_id":"r","source_sha":"sha"}, "epoch", ["a","b"])), patch("batch_crawl.verify_queue"), patch("batch_crawl.load_shared_discovery", return_value=({"a":{"store_uuid":"a"},"b":{"store_uuid":"b"}},1,"epoch",None)), patch("batch_crawl.validate_task"), patch("batch_crawl.public_ip",return_value="local"), patch("batch_crawl.TransportPool"), patch("batch_crawl.global_limiter") as limiter:
            limiter.snapshot.return_value={"successes":0,"rate_limit_events":0,"cooldown_remaining_seconds":0,"interval_seconds":0.18}
            summary = capture_worker({},0,Path(tmp)/"discovery.json",queue=q,root=Path(tmp),capture=capture)
            batch = json.loads((Path(tmp)/"data/worker-batch-0/batch.json").read_text())
            self.assertEqual(list(batch["records"]),["a","b"])
            self.assertEqual(batch["sections"],{})
            self.assertEqual(summary["completed"],2)
            self.assertCountEqual(q.reported,["a","b"])

