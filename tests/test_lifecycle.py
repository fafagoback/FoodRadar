import contextlib
import gzip
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
sys.path.insert(0,str(ROOT/'scripts'))
from serving_state import apply_snapshot, ServingStateCache
from test_packed_updates import document
from build_web_db import build_web_db
import pipeline

SID='ffffffff-ffff-4fff-8fff-ffffffffffff'
PID='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'

class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.row_factory=sqlite3.Row
        self.addCleanup(self.conn.close)
    def observe(self,batch,price=100,opened=True,present=True,cache=None):
        doc=document(SID,[PID] if present else [])
        doc['isOpen']=opened
        if present:
            doc['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price']=price
        apply_snapshot(self.conn,[doc],batch,state_cache=cache)
        return dict(self.conn.execute('SELECT * FROM products').fetchone())
    def test_last_valid_price_ignores_closed_unknown_and_zero(self):
        self.observe('20260101063000',20)
        for day,opened,price in [(2,False,1),(3,None,999),(4,'false',1),(5,True,0)]:
            row=self.observe(f'202601{day:02d}063000',price,opened)
            self.assertEqual(json.loads(row['recent_prices']),[20])
            self.assertEqual(row['is_price_deal'],0)
        row=self.observe('20260106063000',14)
        self.assertEqual((row['reference_price'],row['discount_pct'],row['discount_amount'],row['is_price_deal']),(20,30,6,1))
        row=self.observe('20260107063000',10)
        self.assertEqual(row['reference_price'],14)
        self.assertEqual(row['is_price_deal'],0)
        row=self.observe('20260108063000',14)
        self.assertEqual(row['reference_price'],10)
        changes=self.conn.execute("SELECT old_state FROM events WHERE event_type='PRICE_CHANGED' ORDER BY id").fetchall()
        self.assertEqual(json.loads(changes[0][0])['price'],20)
    def test_inactive_short_and_exact_60_day_relist_with_cache(self):
        first=self.observe('20260101063000')
        cache=ServingStateCache(self.conn)
        for day in (2,3,4):
            row=self.observe(f'202601{day:02d}063000',present=False,cache=cache)
        self.assertEqual(row['status'],'inactive')
        short=self.observe('20260105063000',cache=cache)
        self.assertEqual(short['listing_started_at'],first['first_seen'])
        for day in (6,7,8):
            self.observe(f'202601{day:02d}063000',present=False,cache=cache)
        long=self.observe('20260306063000',cache=cache)
        self.assertEqual(long['first_seen'],first['first_seen'])
        self.assertEqual(long['listing_started_at'],'2026-03-06T06:30:00+08:00')
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM events WHERE event_type='SEASONAL_RELIST'").fetchone()[0],1)
        import msgpack, zstandard
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(__import__('io').StringIO()):
            source=Path(tmp)/'state.db'
            with contextlib.closing(sqlite3.connect(source)) as dest:
                self.conn.backup(dest)
            build_web_db(source,Path(tmp)/'packed.db')
            with contextlib.closing(sqlite3.connect(Path(tmp)/'packed.db')) as packed:
                refs=[]
                for payload, in packed.execute('SELECT payload FROM search_buckets'):
                    refs.extend(msgpack.unpackb(zstandard.ZstdDecompressor().decompress(payload),raw=False).get('f:new',[]))
                self.assertEqual(len(refs),1)
    def test_unknown_closed_and_absent_store_do_not_count_missing(self):
        self.observe('20260101063000')
        for day,opened in [(2,False),(3,None)]:
            self.assertEqual(self.observe(f'202601{day:02d}063000',opened=opened,present=False)['missing_streak'],0)
        apply_snapshot(self.conn,[],'20260104063000')
        self.assertEqual(self.conn.execute('SELECT missing_streak FROM products').fetchone()[0],0)
    def test_permanent_checkpoint_restores_without_raw(self):
        self.observe('20260101063000')
        for day in (2,3,4):
            self.observe(f'202601{day:02d}063000',present=False)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            database=root/'original.db'
            with contextlib.closing(sqlite3.connect(database)) as dest:
                self.conn.backup(dest)
            packed=root/'state.gz'
            packed.write_bytes(gzip.compress(database.read_bytes()))
            pointer=root/'pointer.json'
            pointer.write_text(json.dumps({'batch_id':'20260104063000','state_path':'FoodRadar/state/state.gz','sha256':hashlib.sha256(packed.read_bytes()).hexdigest()}))
            paths={'FoodRadar/state/latest.json':pointer,'FoodRadar/state/state.gz':packed}
            with patch.object(pipeline,'ROOT',root),patch('huggingface_hub.HfApi') as api,patch('huggingface_hub.hf_hub_download',side_effect=lambda repo,path,**kwargs:str(paths[path])):
                api.return_value.list_repo_files.return_value=list(paths)
                pipeline.restore_history({'hf_folder':'FoodRadar','hf_repo_id':'test/repo'})
            with contextlib.closing(sqlite3.connect(root/'data/20260104063000/serving.db')) as restored:
                restored.row_factory=sqlite3.Row
                row=dict(restored.execute('SELECT * FROM products').fetchone())
                self.assertEqual(row['status'],'inactive')
                self.assertEqual(row['first_seen'],row['listing_started_at'])
                doc=document(SID,[PID])
                apply_snapshot(restored,[doc],'20260302063000')
                row=dict(restored.execute('SELECT * FROM products').fetchone())
                self.assertEqual(row['first_seen'],'2026-01-01T06:30:00+08:00')
                self.assertEqual(row['listing_started_at'],'2026-03-02T06:30:00+08:00')

if __name__=='__main__': unittest.main()
