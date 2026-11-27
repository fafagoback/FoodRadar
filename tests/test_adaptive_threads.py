import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from adaptive_threads import AdaptiveThreads, maximum_best, run_adaptive, DEFAULT_THREADS, valid_threads

def snap(successes=0, throttles=0, cooldown=0, interval=.18, completed_stores=None):
    d = dict(successes=successes, rate_limit_events=throttles,
             cooldown_remaining_seconds=cooldown, interval_seconds=interval)
    if completed_stores is not None:
        d["completed_stores"] = completed_stores
    return d

class AdaptiveThreadTests(unittest.TestCase):
    def test_default_and_no_arbitrary_maximum(self):
        self.assertEqual(DEFAULT_THREADS,4)
        self.assertTrue(valid_threads(16))
        self.assertTrue(valid_threads(64))
        self.assertTrue(valid_threads(128))
        self.assertTrue(valid_threads(1024))
        self.assertFalse(valid_threads(0))
        self.assertFalse(valid_threads(True))

    def test_exponential_then_bisect_by_completed_store_throughput(self):
        c=AdaptiveThreads(4)
        now=0; stores=0
        c.reset(now,snap(completed_stores=0))
        def sample(count):
            nonlocal now,stores
            c.observe(now,snap(completed_stores=stores),True,c.target)
            now += 12
            stores += count
            c.observe(now,snap(completed_stores=stores),True,c.target)
        for amount,expected in ((12,8),(24,16),(48,32)):
            sample(amount)
            self.assertEqual(c.target,expected)
        sample(40)  # More HTTP threads but fewer completed stores/s.
        self.assertEqual((c.lower,c.upper,c.target),(16,32,24))
        sample(50)
        self.assertEqual((c.lower,c.upper,c.target),(24,32,28))
        sample(52)
        self.assertEqual((c.lower,c.upper,c.target),(28,32,30))
        self.assertTrue(any(e["reason"]=="throughput_not_improved" for e in c.events))
        self.assertEqual(c.report()["throughput_unit"],"stores_per_second")

    def test_upstream_429_bisects_without_fake_64_cap(self):
        c=AdaptiveThreads(128)
        c.reset(0,snap())
        c.observe(1,snap(0,1,30),True,128)
        self.assertEqual((c.target,c.upper),(64,128))
        c.observe(2,snap(0,2,30),True,128)
        self.assertEqual((c.target,c.upper),(64,128))
        c.observe(40,snap(0,2),False,0)
        c.observe(41,snap(0,2),True,64)
        c.observe(53,snap(72,2),True,64)
        self.assertEqual((c.best,c.target),(64,96))

    def test_no_work_not_failure_and_busy_samples_accumulate(self):
        c=AdaptiveThreads(4); c.reset(0,snap())
        c.observe(1,snap(),True,4)
        c.observe(6,snap(8),False,0)
        c.observe(100,snap(8),True,4)
        c.observe(106,snap(16),True,4)
        self.assertEqual((c.best,c.target),(4,8))
        c.observe(150,snap(20),False,0)
        self.assertEqual(c.best,4)
        self.assertIsNone(c.upper)

    def test_first_failure_halves_and_cooldown(self):
        c=AdaptiveThreads(4);c.reset(0,snap())
        c.observe(1,snap(0,1,30),True,4)
        self.assertEqual((c.target,c.upper),(2,4))
        c.observe(2,snap(0,2,30),True,4)
        self.assertEqual(c.target,2)
        c.observe(40,snap(0,2),False,0)
        c.observe(41,snap(0,2),True,2)
        c.observe(52,snap(20,2),True,2)
        self.assertEqual((c.best,c.target),(2,3))

    def test_maximum_best_stores_any_positive_observed_value(self):
        self.assertEqual(maximum_best([{"optimal_threads":4},{"optimal_threads":8},
                                       {"optimal_threads":128},{"optimal_threads":None},
                                       {"optimal_threads":True}]),128)
        self.assertIsNone(maximum_best([{},{ "optimal_threads":None},{"optimal_threads":0}]))

    def test_resume_legacy_api_rate_is_not_mistaken_for_store_throughput(self):
        original=AdaptiveThreads(4);original.reset(100,snap())
        original.target=12;original.lower=8;original.upper=16;original.best=8;original.best_rate=4.7
        doc=original.report()
        resumed=AdaptiveThreads(4);resumed.resume(doc,10,snap())
        self.assertEqual((resumed.target,resumed.lower,resumed.upper,resumed.best),(12,8,16,8))
        legacy={k:v for k,v in doc.items() if k!="throughput_unit"}
        other=AdaptiveThreads(4);other.resume(legacy,10,snap())
        self.assertEqual(other.best_rate,0.0)

    def test_every_item_once(self):
        class Limiter:
            def snapshot(self):return snap()
        got=[]
        run_adaptive(list(range(20)),lambda x:x,Limiter(),4,got.append)
        self.assertEqual(sorted(got),list(range(20)))

if __name__=="__main__":unittest.main()
