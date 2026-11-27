"""Prevent mismatching a successful crawler with older/other Raw."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from db_stage_source import validate_receipt
class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.value={"source_run_id":"45951424825","source_sha":"a"*40,
                    "batch_id":"20261127143027","stores":523}
    def test_exact_valid_receipt(self):
        self.assertEqual(validate_receipt(self.value,"45951424825","a"*40)["stores"],523)
    def test_other_run_cannot_steal_batch(self):
        with self.assertRaises(ValueError):validate_receipt(self.value,"45939507529","a"*40)
    def test_other_sha_rejected(self):
        with self.assertRaises(ValueError):validate_receipt(self.value,"45951424825","b"*40)
    def test_fake_batch_rejected(self):
        self.value["batch_id"]="20261131143027"
        with self.assertRaises(ValueError):validate_receipt(self.value,"45951424825","a"*40)
    def test_zero_stores_rejected(self):
        self.value["stores"]=0
        with self.assertRaises(ValueError):validate_receipt(self.value,"45951424825","a"*40)
if __name__=="__main__":unittest.main()
