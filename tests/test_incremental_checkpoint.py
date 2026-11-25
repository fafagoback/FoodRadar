"""Offline integrity tests for disposable state checkpoints."""
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from state_checkpoint import restore, save, manifest_digest, CONTRACTS, compatible_contract


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.src=self.root/'src.db'
        self.target=self.root/'target.db'
        self.cache=self.root/'cache'
        self.config={'hf_repo_id':'hub-google/FoodRadar','hf_folder':'Raw'}
        with sqlite3.connect(self.src) as db:
            db.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)')
            db.execute("INSERT INTO metadata VALUES('latest_batch','20261112063000')")
            db.execute('CREATE TABLE crawl_batches(batch_id TEXT)')
            db.execute("INSERT INTO crawl_batches VALUES('20261112063000')")
            db.execute('CREATE TABLE products(name TEXT)')
            db.execute("INSERT INTO products VALUES('test item')")
        self.rows=[('Raw/20261112063000/manifest.json',
                    {'batch_id':'20261112063000'},
                    hashlib.sha256(b'original').hexdigest())]
        import state_checkpoint
        self.mock=patch.object(state_checkpoint,'contract_digest',return_value='valid-contract')
        self.mock.start()
        self.addCleanup(self.mock.stop)
        index=self.root/'raw-index.json'
        index.write_text(json.dumps({
            'batch_id':'20261112063000','repo_id':'hub-google/FoodRadar',
            'folder':'Raw','verified_batches':1,'manifest_count':1,
            'manifest_digest':manifest_digest(self.rows),
            'contract_digest':'valid-contract'}))
        save(self.src,index,cache=self.cache,compact=False)

    def test_valid_cache_preserves_product(self):
        self.assertEqual(restore(self.target,self.rows,self.config,self.cache),
                         ('20261112063000',1))
        with sqlite3.connect(self.target) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM products').fetchone()[0],1)

    def test_appended_batch_uses_verified_prefix(self):
        rows=self.rows+[('Raw/20261112183000/manifest.json',{},'f'*64)]
        self.assertEqual(restore(self.target,rows,self.config,self.cache),('20261112063000',1))

    def test_projection_changes_do_not_invalidate_observations(self):
        self.assertNotIn("src/verified_weekly.py", CONTRACTS)
        self.assertNotIn("src/price_oscillation.py", CONTRACTS)
        self.assertIn("src/serving_state.py", CONTRACTS)
        self.assertIn("src/price_observation.py", CONTRACTS)
        self.assertIn("src/raw_capture.py", CONTRACTS)

    def test_changed_observation_contract_rejects_cache(self):
        with patch("state_checkpoint.contract_digest", return_value="new-observation-contract"):
            self.assertIsNone(restore(self.target,self.rows,self.config,self.cache))

    def test_legacy_cache_requires_reviewed_exact_contract(self):
        def historical(root, ref, files):
            return "valid-contract" if tuple(files) == CONTRACTS else "reviewed-legacy"
        with patch("state_checkpoint._historical_digest", side_effect=historical):
            self.assertTrue(compatible_contract(
                {"format":"foodradar-checkpoint-v1","contract_digest":"reviewed-legacy"},self.root))
            self.assertFalse(compatible_contract(
                {"format":"foodradar-checkpoint-v1","contract_digest":"unknown"},self.root))
        with patch("state_checkpoint._historical_digest", return_value="different"):
            self.assertFalse(compatible_contract(
                {"format":"foodradar-checkpoint-v1","contract_digest":"reviewed-legacy"},self.root))

    def test_old_manifest_changed_falls_back(self):
        changed=[(self.rows[0][0],self.rows[0][1],'e'*64)]
        self.assertIsNone(restore(self.target,changed,self.config,self.cache))

    def test_corrupt_db_falls_back(self):
        (self.cache/'serving.db').write_bytes(b'corrupt')
        self.assertIsNone(restore(self.target,self.rows,self.config,self.cache))

    def test_different_repository_falls_back(self):
        self.assertIsNone(restore(self.target,self.rows,
                          {'hf_repo_id':'wrong/repo','hf_folder':'Raw'},self.cache))


class DirectCaptureMigrationTests(unittest.TestCase):
    def test_reviewed_capture_upgrade_only(self):
        from state_checkpoint import compatible_direct_contract, DIRECT_CAPTURE_MIGRATIONS, DIRECT_CONTRACTS
        old,new=DIRECT_CAPTURE_MIGRATIONS[0]
        root=ROOT
        def historical(path, ref, files):
            if tuple(files)==DIRECT_CONTRACTS:
                if ref==old: return "reviewed-old"
                if ref==new: return "reviewed-new"
            return "other"
        with patch("state_checkpoint._historical_digest",side_effect=historical):
            with patch("state_checkpoint.direct_contract_digest",return_value="reviewed-new"):
                self.assertTrue(compatible_direct_contract({"format":"foodradar-checkpoint-v4","contract_digest":"reviewed-old"},root))
                self.assertFalse(compatible_direct_contract({"format":"foodradar-checkpoint-v4","contract_digest":"unknown"},root))
            with patch("state_checkpoint.direct_contract_digest",return_value="unreviewed-code"):
                self.assertFalse(compatible_direct_contract({"format":"foodradar-checkpoint-v4","contract_digest":"reviewed-old"},root))

    def test_real_layout_migration_exact_contract(self):
        from state_checkpoint import (compatible_direct_contract, CODEC_UPGRADE, CODEC_OLD_LAYOUTS,
            CODEC_OLD_CONTRACTS, DIRECT_CONTRACTS, PREVIOUS_LAYOUT_CONTRACTS,
            DIRECT_LAYOUT_CONTRACTS, _historical_digest,direct_contract_digest)
        current=_historical_digest(ROOT,CODEC_UPGRADE,DIRECT_LAYOUT_CONTRACTS)
        self.assertEqual(direct_contract_digest(ROOT),current)
        for old,files in [(r,PREVIOUS_LAYOUT_CONTRACTS) for r in CODEC_OLD_LAYOUTS]+[(r,DIRECT_CONTRACTS) for r in CODEC_OLD_CONTRACTS]:
            previous=_historical_digest(ROOT,old,files)
            self.assertTrue(compatible_direct_contract({"format":"foodradar-checkpoint-v4","contract_digest":previous},ROOT))
        self.assertFalse(compatible_direct_contract({"format":"foodradar-checkpoint-v4","contract_digest":"unknown"},ROOT))

    def test_real_reviewed_contract_digests(self):
        import subprocess
        from state_checkpoint import compatible_direct_contract, DIRECT_CAPTURE_MIGRATIONS, DIRECT_CONTRACTS, _historical_digest, direct_contract_digest
        old,new=DIRECT_CAPTURE_MIGRATIONS[0]
        try:
            previous=_historical_digest(ROOT,old,DIRECT_CONTRACTS)
            reviewed=_historical_digest(ROOT,new,DIRECT_CONTRACTS)
        except (OSError,subprocess.SubprocessError):
            self.skipTest("Historical commits not available in shallow checkout")
        if direct_contract_digest(ROOT)!=reviewed:
            self.skipTest("Current code contract is not the reviewed capture upgrade")
        self.assertNotEqual(previous,reviewed)
        self.assertTrue(compatible_direct_contract({"format":"foodradar-checkpoint-v4","contract_digest":previous},ROOT))



if __name__=='__main__':
    unittest.main()
