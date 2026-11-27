"""Prevent mixing old HF Raw batches with newer crawler artifacts."""
from __future__ import annotations
import sys, unittest
from unittest.mock import patch
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
from db_stage_plan import assert_batch_provenance,source_artifacts
from db_stage_worker import assert_capture_batch

class SourceBatchTests(unittest.TestCase):
    def setUp(self):
        self.run={"path":".github/workflows/crawl.yml",
                  "created_at":"2026-11-26T19:46:30Z",
                  "updated_at":"2026-11-27T03:33:35Z"}
    def test_source_run_matches_its_own_raw_batch(self):
        self.assertEqual(assert_batch_provenance(self.run,"20261127071423"),
                         "20261127071423")
    def test_previous_day_batch_rejected_before_pool_is_seeded(self):
        with self.assertRaisesRegex(ValueError,"OUTSIDE"):
            assert_batch_provenance(self.run,"20261126072649")
    def test_store_cannot_be_assigned_to_earlier_raw_batch(self):
        current={"captured_at":"2026-11-27T06:50:31.700000+08:00"}
        self.assertIsNone(assert_capture_batch(current,"20261127071423"))
        with self.assertRaisesRegex(RuntimeError,"earlier Raw batch"):
            assert_capture_batch(current,"20261126072649")

    @patch("db_stage_plan.github_get")
    def test_empty_worker_manifest_artifact_is_valid(self, get):
        # Real FR #502 produced three ~900-byte artifacts for workers that
        # claimed zero shops; they are valid and must not block 416 good shops.
        get.side_effect=[
            {"status":"completed","conclusion":"success","head_branch":"main"},
            {"artifacts":[{"name":"fr-worker-r-00","size_in_bytes":924,"expired":False},
                          {"name":"fr-worker-r-01","size_in_bytes":1048576,"expired":False}]}]
        _,artifacts=source_artifacts("46023983182","token","fafagoback/FoodRadar")
        self.assertEqual(artifacts,["fr-worker-r-00","fr-worker-r-01"])
    @patch("db_stage_plan.github_get")
    def test_true_empty_or_missing_artifact_is_rejected(self,get):
        get.side_effect=[
            {"status":"completed","conclusion":"success","head_branch":"main"},
            {"artifacts":[{"name":"fr-worker-r-00","size_in_bytes":0,"expired":False}]}]
        with self.assertRaisesRegex(RuntimeError,"Empty source artifact"):
            source_artifacts("46023983182","token","fafagoback/FoodRadar")

    def test_unrelated_workflow_rejected(self):
        self.run["path"]=".github/workflows/db-stage.yml"
        with self.assertRaisesRegex(ValueError,"crawler workflow"):
            assert_batch_provenance(self.run,"20261127071423")

if __name__=="__main__":
    unittest.main()
