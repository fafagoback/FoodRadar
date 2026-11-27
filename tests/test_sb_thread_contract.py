"""Production settings and store-only queue contract."""
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

class SupabaseThreadContract(unittest.TestCase):
    def test_no_hardcoded_production_initial(self):
        src=(ROOT/"src"/"batch_crawl.py").read_text()
        self.assertIn('initial = int(os.environ["FOODRADAR_INITIAL_THREADS"])',src)
        self.assertIn('os.getenv("FR_QUEUE_BACKEND") == "sb"',src)

    def test_native_categories_not_enqueued(self):
        src=(ROOT/"src"/"sb_task_pool.py").read_text()
        self.assertNotIn('self._rpc("split"',src)
        self.assertIn('Only whole-store tasks are accepted',
                      (ROOT/"src"/"batch_crawl.py").read_text())
if __name__=="__main__":unittest.main()
