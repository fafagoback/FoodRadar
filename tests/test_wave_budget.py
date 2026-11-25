import concurrent.futures,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts")]
from artifact_budget import ArtifactBudget
from raw_capture import CheckpointStorageError
from plan_crawl_wave import plan
from crawl_queue import initial_state,validate_state

class WaveBudgetTest(unittest.TestCase):
    def test_450_limit_concurrent_no_write_past_cap(self):
        seen=[]
        def storage(op,name,folder):seen.append(name);return {"id":len(seen)}
        b=ArtifactBudget(storage)
        def upload(i):
            try:b("upload",str(i),None);return True
            except CheckpointStorageError:return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            result=list(pool.map(upload,range(900)))
        self.assertEqual(sum(result),450)
        self.assertEqual(len(seen),450)
        self.assertTrue(b.exhausted)

    def test_large_roster_15_slots_and_100k_durable_writes(self):
        # Synthetic capacity/fencing check, no upstream traffic and no claim
        # that 100k actual hosted artifacts have been uploaded.
        ids=[f"store-{i:06d}" for i in range(30000)]
        q=initial_state("run","epoch","sha",ids)
        self.assertEqual(len(validate_state(q)["pending"]),30000)
        remaining=30000;previous=0;names=[]
        for wave in range(15):
            planned=plan({"remaining":remaining,"completed":30000-remaining},names,"epoch",previous)
            self.assertEqual(planned["missing_count"],15 if remaining else 0)
            if not remaining:break
            previous=planned["progress"]
            count=min(remaining,15*150)
            for i in range(count):
                k=30000-remaining+i
                names.extend(f"fr-{kind}-epoch-{k}" for kind in ("small","page","task"))
            remaining-=count
        self.assertEqual(remaining,0)
        self.assertEqual(len(names),90000)
        self.assertEqual(plan({"remaining":0,"completed":30000},names,"epoch",previous)["missing_count"],0)

    def test_no_progress_does_not_loop(self):
        with self.assertRaises(RuntimeError):
            plan({"remaining":1,"completed":2},["fr-task-epoch-a"],"epoch",3)

if __name__=="__main__":unittest.main()
