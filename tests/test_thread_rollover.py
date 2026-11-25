import sys,tempfile,json,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from adaptive_threads import AdaptiveThreads
from dynamic_crawl import restore_worker_learning
class ThreadRollover(unittest.TestCase):
    def test_no_relearn_from_original_value_after_rollover(self):
        snapshot={"successes":0,"rate_limit_events":0,"cooldown_remaining_seconds":0,"interval_seconds":.18}
        original=AdaptiveThreads(4);original.reset(100,snapshot)
        original.target=12;original.lower=8;original.upper=16;original.best=8;original.best_rate=4.7
        report=original.report(120)
        next_worker=AdaptiveThreads(4)
        next_worker.resume(report,10,snapshot)
        self.assertEqual((next_worker.target,next_worker.lower,next_worker.upper,next_worker.best),(12,8,16,8))
        next_worker.observe(12,dict(snapshot,rate_limit_events=1,cooldown_remaining_seconds=30),True,12)
        self.assertEqual((next_worker.target,next_worker.lower,next_worker.upper),(10,8,12))
        third=AdaptiveThreads(4);third.resume(next_worker.report(13),100,snapshot)
        self.assertEqual((third.target,third.lower,third.upper),(10,8,12))

    def test_only_own_previous_wave(self):
        state={"run_id":"run","epoch":"epoch","source_sha":"sha"}
        summary={**state,"shard_index":3,"wave":1,"thread_learning":{"target":5}}
        def storage(op,name,folder):
            if name!="fr-worker-epoch-3-w01":return None
            (Path(folder)/"summary.json").write_text(json.dumps(summary))
            return {"id":1}
        self.assertEqual(restore_worker_learning(storage,state,3,3),{"target":5})
        self.assertIsNone(restore_worker_learning(storage,state,2,3))
        summary["source_sha"]="bad"
        with self.assertRaises(ValueError):restore_worker_learning(storage,state,3,3)
