"""No silent historical DB rebuild after storage-only sharding optimization."""
from __future__ import annotations
import sys,unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from state_checkpoint import (
    compatible_direct_contract,direct_contract_digest,REVIEWED_RESHARD_DIRECT_DIGESTS)

class CompactMigrationTests(unittest.TestCase):
    def test_only_exact_historical_fingerprints_are_reused(self):
        actual=direct_contract_digest(ROOT)
        self.assertIn(actual,REVIEWED_RESHARD_DIRECT_DIGESTS)
        for predecessor in REVIEWED_RESHARD_DIRECT_DIGESTS[actual]:
            self.assertTrue(compatible_direct_contract(
                {"format":"foodradar-checkpoint-v4","contract_digest":predecessor},ROOT))
        self.assertFalse(compatible_direct_contract(
            {"format":"foodradar-checkpoint-v4","contract_digest":"0"*64},ROOT))

if __name__=="__main__":
    unittest.main()
