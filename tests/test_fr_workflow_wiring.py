"""Three-entry FR Action contract: one crawl run owns all jobs and all gates."""
import unittest
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]
def load(name):
    return yaml.load((ROOT/".github/workflows"/name).read_text(encoding="utf-8"),Loader=yaml.BaseLoader)

class PipelineWiringTests(unittest.TestCase):
    def setUp(self):
        self.crawl=load("crawl.yml")
        self.pages=load("pages.yml")
        self.clean=load("fr-retention.yml")
    def test_exactly_three_workflow_files(self):
        names=sorted(p.name for p in (ROOT/".github/workflows").glob("*.yml"))
        self.assertEqual([n for n in names if n != "fr-integration-tests.yml"],["crawl.yml","fr-retention.yml","pages.yml"])
    def test_no_unintended_automatic_crawl(self):
        self.assertEqual(list(self.crawl["on"]),["workflow_dispatch"])
        self.assertEqual(self.crawl["on"]["workflow_dispatch"]["inputs"]["mode"]["default"],"crawl")
        source=(ROOT/".github/workflows/crawl.yml").read_text(encoding="utf-8")
        self.assertNotIn("FR_RECOVER_DB_504",source)
        self.assertIn("github.event_name == 'workflow_dispatch' && inputs.mode == 'crawl'",source)
        self.assertEqual(self.crawl["on"]["workflow_dispatch"]["inputs"]["mode"]["default"],"crawl")
    def test_consistent_production_workflow_names(self):
        for workflow, name in (
            (self.crawl, "FoodRadar 01｜資料爬取與更新"),
            (self.pages, "FoodRadar 02｜網站部署"),
            (self.clean, "FoodRadar 03｜儲存空間清理"),
        ):
            self.assertEqual(workflow["name"], name)
            self.assertTrue(workflow["run-name"].startswith(name + "｜#"))
            self.assertNotIn("push", workflow["on"])
        self.assertNotIn("PURGE_FR_67_RUNS", (ROOT/".github/workflows/fr-retention.yml").read_text(encoding="utf-8"))
        self.assertNotIn("FR_ONE_TIME_FULL_CRAWL", (ROOT/".github/workflows/crawl.yml").read_text(encoding="utf-8"))
    def test_one_run_contains_15_crawl_and_15_db_and_17(self):
        jobs=self.crawl["jobs"]
        self.assertEqual(jobs["crawl_shard"]["strategy"]["max-parallel"],"15")
        self.assertEqual(jobs["db_workers"]["strategy"]["matrix"]["index"],
                         [str(i) for i in range(1,16)])
        self.assertEqual(jobs["db_workers"]["strategy"]["max-parallel"],"15")
        self.assertEqual(jobs["db_plan"]["needs"],["ingest","report"])
        self.assertEqual(jobs["db_workers"]["env"]["GH_TOKEN"],"${{ github.token }}")
        self.assertIn("FR_DB_REPLAY_SOURCE",jobs["db_plan"]["env"])
        self.assertIn("FR_REPLAY_ONLY",jobs["final_gate"]["env"])
        self.assertIn("replay",self.crawl["on"]["workflow_dispatch"]["inputs"]["mode"]["options"])
        self.assertEqual(jobs["db_merge"]["needs"],["db_plan","db_workers"])
        self.assertIn("always()",jobs["db_merge"]["if"])
        self.assertIn("needs.db_workers.result == 'success'",jobs["db_merge"]["if"])
        self.assertIn("db_stage_merge.py","\n".join(s.get("run","") for s in jobs["db_merge"]["steps"]))
    def test_same_run_receipt_verified_before_pool(self):
        jobs=self.crawl["jobs"]
        self.assertIn("FR_DB_SAME_RUN",jobs["db_plan"]["env"])
        self.assertIn("github.run_id",jobs["db_plan"]["env"]["SOURCE_RUN"])
        self.assertIn("needs.ingest.outputs.batch_id",jobs["db_plan"]["env"]["BATCH"])
        text="\n".join(s.get("run","") for s in jobs["db_plan"]["steps"])
        self.assertIn("db_stage_source.py",text)
        self.assertIn("db_stage_hf.py",text)
        code=(ROOT/"scripts/db_stage_plan.py").read_text(encoding="utf-8")
        self.assertIn("FR_DB_SAME_RUN",code)
        self.assertIn("validate_receipt",code)
        self.assertIn('same_run',code)
    def test_publisher_and_cleanup_are_reusable_not_separate_runs(self):
        jobs=self.crawl["jobs"]
        self.assertEqual(jobs["publish"]["uses"],"./.github/workflows/pages.yml")
        self.assertEqual(jobs["cleanup"]["uses"],"./.github/workflows/fr-retention.yml")
        self.assertIn("workflow_call",self.pages["on"])
        self.assertIn("workflow_call",self.clean["on"])
        self.assertIn("workflow_dispatch",self.pages["on"])
        self.assertEqual(self.clean["on"]["schedule"][0]["cron"],"0 19 * * *")
        for name in ("crawl.yml","pages.yml","fr-retention.yml"):
            text=(ROOT/".github/workflows"/name).read_text(encoding="utf-8")
            self.assertNotIn("gh workflow run ",text)
    def test_final_gate_rejects_skips(self):
        gate=self.crawl["jobs"]["final_gate"]
        self.assertIn("always()",gate["if"])
        for need in ("ingest","db_plan","db_workers","db_merge","publish","cleanup"):
            self.assertIn(need,gate["needs"])
        command="\n".join(s.get("run","") for s in gate["steps"])
        for need in ("INGEST","DB_PLAN","DB_WORKERS","DB_MERGE","PUBLISH","CLEANUP"):
            self.assertIn(need,command)
        self.assertIn('!= success',command.replace('" != success', '!= success'))
    def test_browser_and_price_rules_unchanged(self):
        command="\n".join(s.get("run","") for s in self.pages["jobs"]["release"]["steps"])
        self.assertIn("test_relational_main_browser.cjs",command)
        self.assertIn("activate_relational_sqlite.py",command)
        self.assertIn("configure_sqlite_site.py",command)
        raw=(ROOT/"scripts/cleanup_sqlite_previews.py").read_text(encoding="utf-8")
        self.assertIn("DB/current.json",raw)
        self.assertIn("DB/preview.json",raw)
        self.assertIn("State/current",raw)
if __name__=="__main__":
    unittest.main()
