import sys,tempfile,json,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from adaptive_threads import AdaptiveThreads
from dynamic_crawl import restore_worker_learning
class ThreadRollover(unittest.TestCase):
    def test_no_relearn_from_original_value_after_rollover(self):
        snapshot={"successes":0,"rate_limit_events":0,"cooldown_remaining_seconds":0,"interval_seconds":.18}
        original=AdaptiveThreads(2);original.reset(100,snapshot)
        original.target=5;original.best=6;original.best_rate=4.7
        original.next_probe=150;original.direction=-1
        report=original.report(120)
        next_worker=AdaptiveThreads(2)
        next_worker.resume(report,10,snapshot)
        self.assertEqual((next_worker.target,next_worker.best,next_worker.best_rate),(5,6,4.7))
        self.assertEqual(next_worker.next_probe,40)
        next_worker.observe(12,dict(snapshot,rate_limit_events=1),True)
        self.assertEqual(next_worker.target,2)
        self.assertIsNone(next_worker.best)
        third=AdaptiveThreads(2);third.resume(next_worker.report(13),100,snapshot)
        self.assertIsNone(third.best)
        self.assertEqual(third.target,2)

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
