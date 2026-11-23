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
from state_checkpoint import restore, save, manifest_digest


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
        save(self.src,index,cache=self.cache)

    def test_valid_cache_preserves_product(self):
        self.assertEqual(restore(self.target,self.rows,self.config,self.cache),
                         ('20261112063000',1))
        with sqlite3.connect(self.target) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM products').fetchone()[0],1)

    def test_appended_batch_uses_verified_prefix(self):
        rows=self.rows+[('Raw/20261112183000/manifest.json',{},'f'*64)]
        self.assertEqual(restore(self.target,rows,self.config,self.cache),('20261112063000',1))

    def test_old_manifest_changed_falls_back(self):
        changed=[(self.rows[0][0],self.rows[0][1],'e'*64)]
        self.assertIsNone(restore(self.target,changed,self.config,self.cache))

    def test_corrupt_db_falls_back(self):
        (self.cache/'serving.db').write_bytes(b'corrupt')
        self.assertIsNone(restore(self.target,self.rows,self.config,self.cache))

    def test_different_repository_falls_back(self):
        self.assertIsNone(restore(self.target,self.rows,
                          {'hf_repo_id':'wrong/repo','hf_folder':'Raw'},self.cache))


if __name__=='__main__':
    unittest.main()
