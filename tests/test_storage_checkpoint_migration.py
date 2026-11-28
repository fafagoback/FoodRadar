"""Exact migration tests: don't replay the full Raw when only compact layout changes."""
import sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import state_checkpoint

OLD="b97d6422936d64504a7e909f1570d3f63570f73aecb1316f6cb14cd3d8d82e64"
NEW="ac9e84a16e1eac1db68430cd36746f0420b07422374d85469c5954a57d50a183"

class StorageMigrationTests(unittest.TestCase):
    def test_exact_predecessor_is_compatible(self):
        with patch.object(state_checkpoint,"direct_contract_digest",return_value=NEW):
            self.assertTrue(state_checkpoint.compatible_direct_contract(
                {"format":"foodradar-checkpoint-v4","contract_digest":OLD}))
    def test_current_checkpoint_is_compatible(self):
        with patch.object(state_checkpoint,"direct_contract_digest",return_value=NEW):
            self.assertTrue(state_checkpoint.compatible_direct_contract(
                {"format":"foodradar-checkpoint-v4","contract_digest":NEW}))
    def test_wrong_format_does_not_use_storage_migration(self):
        with patch.object(state_checkpoint,"direct_contract_digest",return_value=NEW):
            self.assertFalse(state_checkpoint.compatible_direct_contract(
                {"format":"not-a-real-checkpoint","contract_digest":OLD}))
if __name__=="__main__":unittest.main()
