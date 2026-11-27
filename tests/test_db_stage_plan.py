"""Prevent mixing old HF Raw batches with newer crawler artifacts."""
from __future__ import annotations
import sys, unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
from db_stage_plan import assert_batch_provenance

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
    def test_unrelated_workflow_rejected(self):
        self.run["path"]=".github/workflows/db-stage.yml"
        with self.assertRaisesRegex(ValueError,"crawler workflow"):
            assert_batch_provenance(self.run,"20261127071423")

if __name__=="__main__":
    unittest.main()
