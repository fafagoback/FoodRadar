"""Ensure FR runs only on manual UE requests and DB never uses old static Pages.

No external network, no new UE calls. Read actual workflow YAML as data.
"""
import unittest
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]
def load(path):
    with (ROOT/path).open(encoding="utf-8") as f:
        return yaml.load(f,Loader=yaml.BaseLoader)

class PipelineWiringTests(unittest.TestCase):
    def setUp(self):
        self.crawl=load(".github/workflows/crawl.yml")
        self.db=load(".github/workflows/db-distributed.yml")
        self.release=load(".github/workflows/sqlite-release.yml")
        self.clean=load(".github/workflows/cleanup.yml")
    def test_crawler_has_no_automatic_trigger(self):
        triggers=self.crawl["on"]
        self.assertEqual(list(triggers),["workflow_dispatch"])
        self.assertNotIn("schedule",triggers)
        self.assertNotIn("push",triggers)
    def test_exact_auto_crawl_to_db_handoff(self):
        name=self.crawl["name"]
        self.assertEqual(self.db["on"]["workflow_run"]["workflows"],[name])
        resolve=self.db["jobs"]["resolve_source"]
        self.assertTrue(any(s.get("id")=="probe" for s in resolve["steps"]))
        self.assertIn("available",resolve["outputs"])
        self.assertIn("outputs.available",self.db["jobs"]["plan"]["if"])
        stream=(ROOT/"src/stream_ingest.py").read_text(encoding="utf-8")
        self.assertIn('fr-db-source.json',stream)
    def test_fifteen_db_workers_and_correct_final_step(self):
        m=self.db["jobs"]["workers"]["strategy"]
        self.assertEqual([int(x) for x in m["matrix"]["index"]],list(range(1,16)))
        self.assertEqual(m["max-parallel"],"15")
        steps=self.db["jobs"]["merge"]["steps"]
        cmds="\n".join(s.get("run","") for s in steps)
        self.assertIn("db_stage_merge.py",cmds)
        self.assertIn("sqlite-release.yml",cmds)
        self.assertNotIn("pages.yml",cmds)
    def test_raw_only_pipeline_does_not_build_old_pages_copy(self):
        crawl=(ROOT/".github/workflows/crawl.yml").read_text(encoding="utf-8")
        self.assertIn("FR_RAW_ONLY",crawl)
        self.assertIn("fr-db-source",crawl)
        self.assertNotIn('gh workflow run pages.yml',crawl)
        self.assertFalse((ROOT/".github/workflows/pages.yml").exists())
    def test_sqlite_production_is_verified_and_hf_retention_is_scheduled(self):
        release=self.release
        commands="\n".join(s.get("run","") for s in release["jobs"]["release"]["steps"])
        self.assertIn("test_relational_main_browser.cjs",commands)
        self.assertIn("activate_relational_sqlite.py",commands)
        self.assertIn("configure_sqlite_site.py",commands)
        self.assertIn("清理",self.clean["name"])
        self.assertEqual(self.clean["on"]["schedule"][0]["cron"],"0 19 * * *")
    def test_retention_only_removes_unused_previews(self):
        text=(ROOT/"scripts/cleanup_sqlite_previews.py").read_text(encoding="utf-8")
        self.assertIn('DB/current.json',text)
        self.assertIn('DB/preview.json',text)
        self.assertIn('CommitOperationDelete',text)
        self.assertIn('parent_commit=revision',text)
        self.assertIn('State/current',text)
if __name__=="__main__":
    unittest.main()
