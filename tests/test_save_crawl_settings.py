"""Thread settings remain learnable when a known store gap is quarantined."""
import base64
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import save_crawl_settings as saver

class SaveCrawlSettingsTests(unittest.TestCase):
    def test_valid_worker_average_survives_isolated_store_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            files = []
            for i in range(15):
                path = root / str(i)
                path.write_text(json.dumps({
                    "shard_index": i, "run_id": "run", "source_sha": "sha",
                    "stores": [{"success": i != 0}],
                    "thread_learning": {"optimal_threads": 16 if i < 2 else None},
                }))
                files.append(path)
            original = {"sha": "old", "content": base64.b64encode(
                b'{"optimal_threads": 2}').decode()}
            with patch.dict(os.environ, {"GITHUB_RUN_ID":"run",
                        "GITHUB_SHA":"sha", "GITHUB_REPOSITORY":"owner/repo",
                        "FOODRADAR_BATCH_COUNT":"15"}), \
                 patch.object(Path, "glob", return_value=files), \
                 patch.object(saver, "gh", side_effect=[json.dumps(original), "{}"]) as gh:
                saver.main()
            self.assertEqual(gh.call_count, 2)
            args = gh.call_args.args
            content = next(x[8:] for x in args if x.startswith("content="))
            self.assertEqual(json.loads(base64.b64decode(content)), {"optimal_threads": 16})

if __name__ == "__main__":
    unittest.main()
