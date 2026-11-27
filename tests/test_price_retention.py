"""Prove physical expiry, exact boundary, reload and subsequent update."""
import contextlib,io,sys,tempfile,unittest
from datetime import datetime
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts"),str(ROOT/"tests")]
from compact_direct import CompactStore,CompactCollection,open_collection
from test_compact_direct import doc,SIDS
from price_retention import prune_price_history,TW,_remap,_runs

class PriceRetentionTests(unittest.TestCase):
    def test_sparse_dictionary_remap(self):
        self.assertEqual(_remap(0b11111,_runs([0,2,4])),0b111)
        self.assertEqual(_remap(0b10010,_runs([0,2,4])),0b100)
        self.assertEqual(_remap(0b111,[]),0)

    def test_db_boundary_and_incremental(self):
        with tempfile.TemporaryDirectory() as folder,contextlib.redirect_stdout(io.StringIO()):
            path=Path(folder)/"state.db"
            state=CompactCollection([CompactStore(path,empty=True)])
            for batch,price in [("20260927065959",100),("20260927070000",120),("20261126063000",80)]:
                state.apply_batch([doc(SIDS[0],price)],batch)
            first=state.products(SIDS[0])[0]["first_seen"]
            now=datetime(2026,11,26,7,tzinfo=TW)
            self.assertEqual(prune_price_history(state,now),1)
            self.assertEqual(prune_price_history(state,now),0)
            self.assertEqual(state.stats()["observations"],2)
            self.assertEqual([q[0] for q in state.shop(SIDS[0])[2]],["20260927070000","20261126063000"])
            self.assertEqual(state.products(SIDS[0])[0]["first_seen"],first)
            self.assertEqual(len(state.parts[0].batches),2)
            state.close()
            state=open_collection([path])
            try:
                self.assertEqual(state.stats()["observations"],2)
                state.apply_batch([doc(SIDS[0],70)],"20261127063000")
                self.assertEqual(state.stats()["observations"],3)
                self.assertEqual(prune_price_history(state,datetime(2026,11,27,7,tzinfo=TW)),1)
                self.assertEqual(state.stats()["observations"],2)
                self.assertEqual(state.products(SIDS[0])[0]["first_seen"],first)
            finally:
                state.close()
