import copy,json,sqlite3,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts")]
from compact_direct import CompactStore,CompactCollection
from compact_templates import Templates,choose_pool,KEY
from compact_repack import build,digest
from test_compact_direct import doc,SIDS

class SharedCatalogTests(unittest.TestCase):
    def test_cross_store_exact_content_not_price(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);original=root/"original.db";target=root/"target.db"
            state=CompactCollection([CompactStore(original,empty=True)])
            batch="20261125133219"
            state.apply_batch([doc(SIDS[0],100),doc(SIDS[1],150)],batch)
            before=[state.shop(sid,derive_history=True) for sid in state.ids()]
            state.close()
            info=build(original,target,512,share=True)
            self.assertEqual(info["shared_groups"],1)
            self.assertEqual(info["cross_shop_rows"],2)
            self.assertEqual(digest(original),digest(target))
            pool=CompactCollection([CompactStore(target)])
            legacy=CompactCollection([CompactStore(original)])
            try:
                self.assertEqual([pool.shop(sid,derive_history=True) for sid in pool.ids()],before)
                self.assertEqual(len(pool.parts[0].templates.pool),1)
                old_second=pool.shop(SIDS[1])
                for c in (pool,legacy):
                    c.apply_batch([doc(SIDS[0],120)],"20261126133219",[SIDS[1]])
                    c.refresh_weekly("20261126133219")
                self.assertEqual(pool.shop(SIDS[1]),old_second)
                self.assertEqual(list(pool.shops()),list(legacy.shops()))
                self.assertEqual(pool.stats(),legacy.stats())
            finally:pool.close();legacy.close()

    def test_no_pool_for_same_store_only(self):
        codec=Templates({"__product_columns":["product_name","category","description","price"]})
        pool,stats=choose_pool(codec,[[[1,2,3,100],[1,2,3,200]],[[4,2,3,100]]])
        self.assertFalse(pool)
        self.assertEqual(stats["shared_groups"],0)
