"""HF SQL pruning must preserve live DB/current and optional preview."""
import sys,unittest,json
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from cleanup_sqlite_previews import referenced,obsolete,CANDIDATE

LIVE="DB/previews/20261127143027/37fc05196e158fb6/catalog.db"
TEST="DB/previews/20261126143027/aaaaaaaaaaaaaaaa/catalog.db"
OLD="DB/previews/20261125143027/bbbbbbbbbbbbbbbb/catalog.db"
def pointer(path):return json.dumps({"format":"foodradar-relational-overlay-v9","path":path,"size":12345}).encode()
class Tests(unittest.TestCase):
    def setUp(self):
        self.files={LIVE,TEST,OLD,"DB/current.json","DB/preview.json","State/current/serving.compact.db"}
        self.rows={"DB/current.json":pointer(LIVE),"DB/preview.json":pointer(TEST)}
    def test_only_unused_preview_removed(self):
        saved=referenced(self.files,lambda p:self.rows[p])
        self.assertEqual(saved,{LIVE,TEST})
        self.assertEqual(obsolete(self.files,saved),[OLD])
        self.assertIn("State/current/serving.compact.db",self.files)
    def test_missing_active_pointer_fails_closed(self):
        with self.assertRaises(ValueError):referenced(self.files-{"DB/current.json"},lambda p:self.rows[p])
    def test_missing_pointer_target_fails_closed(self):
        self.rows["DB/current.json"]=pointer(OLD)
        with self.assertRaises(ValueError):referenced(self.files-{OLD},lambda p:self.rows[p])
    def test_malformed_pointer_fails_closed(self):
        self.rows["DB/current.json"]=pointer("../State/current/serving.compact.db")
        with self.assertRaises(ValueError):referenced(self.files,lambda p:self.rows[p])
    def test_unrelated_raw_is_never_selected(self):
        paths=self.files|{"Raw/20261127143027/capture.tar.gz","DB/current.json"}
        self.assertEqual(obsolete(paths,{LIVE,TEST}),[OLD])
if __name__=="__main__":unittest.main()
