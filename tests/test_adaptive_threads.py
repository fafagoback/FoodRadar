import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from adaptive_threads import AdaptiveThreads, maximum_best, run_adaptive, DEFAULT_THREADS, MAX_THREADS, valid_threads

def snap(successes=0, throttles=0, cooldown=0, interval=.18):
    return dict(successes=successes, rate_limit_events=throttles,
                cooldown_remaining_seconds=cooldown, interval_seconds=interval)

class AdaptiveThreadTests(unittest.TestCase):
    def test_default_and_limit(self):
        self.assertEqual(DEFAULT_THREADS,4)
        self.assertTrue(valid_threads(16))
        self.assertTrue(valid_threads(MAX_THREADS))
        self.assertFalse(valid_threads(MAX_THREADS+1))

    def test_exponential_and_bisection(self):
        c=AdaptiveThreads(2); c.reset(0,snap())
        t=s=events=0
        def passed():
            nonlocal t,s
            c.observe(t,snap(s,events),True,c.target)
            t+=12;s+=48
            c.observe(t,snap(s,events),True,c.target)
        def failed():
            nonlocal t,events
            t+=1;events+=1
            c.observe(t,snap(s,events,30),True,c.target)
            t+=31
            c.observe(t,snap(s,events),False,0)
        for expected in (4,8,16):
            passed(); self.assertEqual(c.target,expected)
        for expected in (12,10,9):
            failed(); self.assertEqual(c.target,expected)
        passed()
        self.assertEqual((c.best,c.lower,c.upper,c.target),(9,9,10,9))

    def test_initial_failure_halves_and_cooldown_coalesces(self):
        c=AdaptiveThreads(4); c.reset(0,snap())
        c.observe(1,snap(0,1,30),True,4)
        self.assertEqual((c.target,c.upper),(2,4))
        c.observe(2,snap(0,2,30),True,4)
        self.assertEqual((c.target,c.upper),(2,4))
        c.observe(40,snap(0,2),False,0)
        c.observe(41,snap(0,2),True,2)
        c.observe(52,snap(20,2),True,2)
        self.assertEqual((c.best,c.target),(2,3))

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

    def test_minimum_failure_retries_only_after_cooldown(self):
        c=AdaptiveThreads(1);c.reset(0,snap())
        c.observe(1,snap(0,1,30),True,1)
        self.assertEqual(c.target,1);self.assertIsNone(c.best)
        c.observe(35,snap(0,1),False,0)
        c.observe(36,snap(0,1),True,1)
        c.observe(50,snap(16,1),True,1)
        self.assertEqual((c.best,c.target),(1,2))

    def test_maximum_and_ignore_invalid_or_insufficient_samples(self):
        self.assertEqual(maximum_best([{"optimal_threads":4},{"optimal_threads":8},
                                       {"optimal_threads":16},{"optimal_threads":None},
                                       {"optimal_threads":65},{"optimal_threads":True}]),16)
        self.assertIsNone(maximum_best([{},{"optimal_threads":None},{"optimal_threads":0}]))

    def test_every_item_once(self):
        class Limiter:
            def snapshot(self):return snap()
        got=[]
        run_adaptive(list(range(20)),lambda x:x,Limiter(),4,got.append)
        self.assertEqual(sorted(got),list(range(20)))

if __name__=="__main__":unittest.main()
