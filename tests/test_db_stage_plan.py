"""Prevent mixing old HF Raw batches with newer crawler artifacts."""
from __future__ import annotations
import sys, unittest, os
from unittest.mock import patch, MagicMock
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

    @patch.dict(os.environ, {"FR_DB_SAME_RUN":"1","GITHUB_RUN_ID":"555","GITHUB_SHA":"a"*40})
    @patch("db_stage_plan.github_get")
    def test_same_run_stage16_may_seed_before_crawler_action_finishes(self,get):
        # The completed stage-16 receipt is verified before the entire Action completes.
        get.side_effect=[
            {"id":555,"status":"in_progress","head_branch":"main",
             "head_sha":"a"*40,"conclusion":None},
            {"artifacts":[{"name":"fr-worker-test-01","size_in_bytes":924,"expired":False}]}]
        run,names=source_artifacts("555","token","fafagoback/FoodRadar")
        self.assertEqual(run["status"],"in_progress")
        self.assertEqual(names,["fr-worker-test-01"])

    @patch.dict(os.environ, {"FR_DB_SAME_RUN":"1","GITHUB_RUN_ID":"555","GITHUB_SHA":"a"*40})
    @patch("db_stage_plan.github_get")
    def test_foreign_active_run_cannot_seed_same_run_pool(self,get):
        get.return_value={"id":556,"status":"in_progress","head_branch":"main",
                          "head_sha":"a"*40,"conclusion":None}
        with self.assertRaisesRegex(RuntimeError,"Source crawl"):
            source_artifacts("556","token","fafagoback/FoodRadar")

    @patch.dict(os.environ, {"FR_DB_REPLAY_SOURCE":"46076775133"})
    @patch("db_stage_plan.github_get")
    def test_failed_run_replay_requires_15_successful_crawlers_and_stage16(self,get):
        run={"status":"completed","conclusion":"failure","head_branch":"main","head_sha":"a"*40}
        artifact={"artifacts":[{"name":"fr-worker-abc-01","size_in_bytes":924,"expired":False}]}
        jobs=[{"name":f"爬蟲工作機 {i:02d}","conclusion":"success"} for i in range(1,16)]
        jobs += [{"name":"16｜核對15台來源、串流保存 HF Raw","conclusion":"success"},
                 {"name":"彙整本次繁體中文爬蟲報告","conclusion":"success"},
                 {"name":"00｜驗證 HF Raw，建立專屬 DB 共享工作池","conclusion":"success"}]
        get.side_effect=[run,artifact,{"total_count":len(jobs),"jobs":jobs}]
        _,names=source_artifacts("46076775133","token","fafagoback/FoodRadar")
        self.assertEqual(len(names),1)
        jobs[0]["conclusion"]="failure"
        get.side_effect=[run,artifact,{"total_count":len(jobs),"jobs":jobs}]
        with self.assertRaisesRegex(RuntimeError,"provenance"):
            source_artifacts("46076775133","token","fafagoback/FoodRadar")

    def test_unrelated_workflow_rejected(self):
        self.run["path"]=".github/workflows/db-stage.yml"
        with self.assertRaisesRegex(ValueError,"crawler workflow"):
            assert_batch_provenance(self.run,"20261127071423")

if __name__=="__main__":
    unittest.main()
