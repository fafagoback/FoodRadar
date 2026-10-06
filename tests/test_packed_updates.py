import contextlib
import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
from serving_state import apply_snapshot
from build_web_db import build_web_db
from verify_packed_local import verify
from publish_turso_delta import plan_table


def document(sid,pids):
    return {'@type':'Restaurant','@id':f'https://www.ubereats.com/tw/store/test/{sid}',
        'store_uuid':sid,'name':'測試餐廳','isOpen':True,
        'address':{'streetAddress':'台北市測試路1號'},
        'hasMenu':{'hasMenuSection':[{'name':'飯','hasMenuItem':[
            {'identifier':pid,'name':'雞肉飯','offers':{'price':100}} for pid in pids]}]}}


class PackedUpdateTests(unittest.TestCase):
    def test_daily_observation_does_not_rewrite_current_bundles_or_indexes(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), contextlib.ExitStack() as stack:
            root = Path(tmp)
            conn = sqlite3.connect(root/'state.db')
            conn.row_factory = sqlite3.Row
            stack.callback(conn.close)
            sid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            pid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            docs=[document(sid,[pid])]
            apply_snapshot(conn,docs,'20261006063000')
            build_web_db(root/'state.db',root/'a.db')
            apply_snapshot(conn,docs,'20261007063000')
            build_web_db(root/'state.db',root/'b.db')
            for table in ('store_directory','store_bundles','search_buckets'):
                with contextlib.closing(sqlite3.connect(root/'a.db')) as a, contextlib.closing(sqlite3.connect(root/'b.db')) as b:
                    self.assertEqual(a.execute(f'SELECT * FROM {table}').fetchall(),b.execute(f'SELECT * FROM {table}').fetchall(),table)
            # New identities sort before the old UUID but must append to IDs,
            # and a new product must leave the existing product ref unchanged.
            new_sid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            new_pid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            apply_snapshot(conn,[document(sid,[pid,new_pid]),document(new_sid,[new_pid])],'20261008063000')
            build_web_db(root/'state.db',root/'c.db')
            report = verify(root/'c.db')
            self.assertEqual(report['products'],3)
            with contextlib.closing(sqlite3.connect(root/'c.db')) as c:
                self.assertEqual(c.execute('SELECT store_id FROM store_directory WHERE store_uuid=?',(sid,)).fetchone()[0],0)


if __name__ == '__main__':
    unittest.main()
