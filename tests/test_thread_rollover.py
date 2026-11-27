import sys,tempfile,json,unittest,os
from unittest.mock import patch
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

    def test_resume_from_actual_recovery_artifact(self):
        from batch_crawl import capture_worker
        from crawl_queue import initial_state
        state=initial_state("run","epoch","sha",["store"])
        class Queue:
            def read(self):return state
            def exchange(self,owner,worker,slots,stores,sections):return [],True
        snap={"successes":0,"rate_limit_events":0,"cooldown_remaining_seconds":0,
              "interval_seconds":.18}
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            previous=root/"data"/"previous-worker-learning"
            previous.mkdir(parents=True)
            ctrl=AdaptiveThreads(4);ctrl.reset(0,snap)
            ctrl.target=12;ctrl.lower=8;ctrl.upper=16;ctrl.best=8
            (previous/"summary.json").write_text(json.dumps({
                "run_id":"run","source_sha":"sha","epoch":"epoch","shard_index":0,
                "wave":0,"thread_learning":ctrl.report()}))
            with patch.dict(os.environ,{"FOODRADAR_INITIAL_THREADS":"4",
                    "FR_CRAWL_WAVE":"1"}), \
                patch("batch_crawl.shared_identity",return_value=(
                    {"run_id":"run","source_sha":"sha"},"epoch",["store"])), \
                patch("batch_crawl.verify_queue"), \
                patch("batch_crawl.load_shared_discovery",return_value=(
                    {"store":{"store_uuid":"store"}},1,"epoch",None)), \
                patch("batch_crawl.public_ip",return_value="test"), \
                patch("batch_crawl.TransportPool"), \
                patch("batch_crawl.global_limiter") as limiter:
                limiter.snapshot.return_value=snap
                report=capture_worker({},0,root/"discovery.json",queue=Queue(),
                                      root=root)
            self.assertEqual((report["thread_learning"]["target"],
                              report["thread_learning"]["optimal_threads"]),(12,8))

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
