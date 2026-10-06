import contextlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from publish_turso_delta import Remote, publish
from prototype_pack_db import SCHEMA


class SQLiteRemote:
    """Use real SQLite transactions to exercise the publisher's SQL and rollback."""
    def __init__(self):
        self.conn = sqlite3.connect(':memory:', isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.fail = False

    def execute(self, statements):
        results = []
        for sql,args,read in statements:
            if self.fail and sql.startswith('INSERT INTO store_bundles'):
                raise RuntimeError('Simulated write failure')
            cursor = self.conn.execute(sql,args)
            results.append([dict(r) for r in cursor.fetchall()] if read else [])
        return results

    def query(self, sql,args=()):
        return self.execute([(sql,args,True)])[0]


class DeltaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'packed.db'
        self.local = sqlite3.connect(self.path)
        self.addCleanup(self.local.close)
        self.local.executescript(SCHEMA)
        self.local.executemany('INSERT INTO metadata VALUES(?,?)', [
            ('application', '"FoodRadar"'), ('schema_version', '1'), ('latest_batch', '"20261006090919"')])
        self.local.execute("INSERT INTO store_bundles VALUES(0,0,'msgpack+zstd',10,'first',X'1234')")
        self.local.execute("INSERT INTO search_buckets VALUES(0,'msgpack+zstd',10,'index',X'1234')")
        self.local.commit()
        self.remote = SQLiteRemote()
        self.addCleanup(self.remote.conn.close)

    def test_initial_publish_then_unchanged_run_has_zero_writes(self):
        report = publish(self.path,self.remote,True)
        self.assertTrue(report['applied'])
        again = publish(self.path,self.remote,True)
        self.assertEqual(again['delta_bytes'],0)
        self.assertTrue(all(s['upsert']==s['delete']==0 for s in again['tables'].values()))

    def test_only_changed_bundle_is_written(self):
        publish(self.path,self.remote,True)
        self.local.execute("UPDATE store_bundles SET checksum='price-change',payload=X'5678'")
        self.local.commit()
        report = publish(self.path,self.remote,True)
        self.assertEqual(report['tables']['store_bundles']['upsert'],1)
        self.assertEqual(report['tables']['search_buckets']['upsert'],0)
        self.assertEqual(report['tables']['metadata']['upsert'],0)

    def test_failure_keeps_old_release_and_rolls_back(self):
        publish(self.path,self.remote,True)
        self.local.execute("UPDATE store_bundles SET checksum='new'")
        self.local.execute("UPDATE metadata SET value='\"20261007090919\"' WHERE key='latest_batch'")
        self.local.commit()
        self.remote.fail=True
        with self.assertRaises(RuntimeError):
            publish(self.path,self.remote,True)
        self.assertEqual(self.remote.query('SELECT checksum FROM store_bundles')[0]['checksum'],'first')
        self.assertEqual(json.loads(self.remote.query("SELECT value FROM metadata WHERE key='latest_batch'")[0]['value']),'20261006090919')

    def test_original_database_url_is_rejected(self):
        with self.assertRaises(ValueError):
            Remote('https://ubereats-packed-v1-eeetchen.aws-ap-northeast-1.turso.io','unused')


if __name__ == '__main__':
    unittest.main()
