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

    def test_empty_six_table_foodradar_schema_can_initialize(self):
        # A prior DB cleanup may leave schemas but zero application rows.
        self.remote.conn.executescript(SCHEMA)
        result = publish(self.path, self.remote, True)
        self.assertTrue(result['applied'])
        self.assertTrue(result['initialization'])
        self.assertEqual(
            self.remote.query("SELECT value FROM metadata WHERE key='application'")[0]['value'],
            '"FoodRadar"')

    def test_missing_owner_with_existing_rows_never_writes(self):
        self.remote.conn.executescript(SCHEMA)
        self.remote.query(
            "INSERT INTO store_bundles VALUES(0,0,'msgpack+zstd',10,'previous',X'1234')")
        with self.assertRaisesRegex(ValueError, 'partially populated'):
            publish(self.path, self.remote, True)
        self.assertEqual(self.remote.query("SELECT COUNT(*) AS count FROM metadata")[0]['count'], 0)
        self.assertEqual(self.remote.query("SELECT checksum FROM store_bundles")[0]['checksum'], 'previous')

    def test_missing_owner_with_unknown_table_never_writes(self):
        self.remote.conn.executescript(SCHEMA)
        self.remote.query('CREATE TABLE third_party(id INTEGER)')
        with self.assertRaisesRegex(ValueError, 'unidentified'):
            publish(self.path, self.remote, True)
        self.assertEqual(self.remote.query("SELECT COUNT(*) AS count FROM metadata")[0]['count'], 0)

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

    def test_incomplete_hf_replay_cannot_mass_delete_existing_stores(self):
        self.local.executemany(
            'INSERT INTO store_directory(store_id,store_uuid,name,chunk_count) VALUES(?,?,?,?)',
            [(i,f'uuid-{i}','Store',1) for i in range(100)])
        self.local.commit()
        publish(self.path,self.remote,True)
        self.local.execute('DELETE FROM store_directory WHERE store_id>=10')
        self.local.commit()
        with self.assertRaisesRegex(ValueError,'Unsafe store shrink'):
            publish(self.path,self.remote,True)
        self.assertEqual(self.remote.query('SELECT COUNT(*) AS count FROM store_directory')[0]['count'],100)

    def test_packed_directory_id_swap_preserves_uuid_uniqueness(self):
        self.local.executemany(
            'INSERT INTO store_directory(store_id,store_uuid,name,chunk_count) VALUES(?,?,?,?)',
            [(0,'uuid-a','A',1),(1,'uuid-b','B',1)])
        self.local.commit()
        publish(self.path,self.remote,True)
        # Repacking can swap the position-based IDs of two known stores.
        self.local.execute('UPDATE store_directory SET store_id=-1 WHERE store_id=0')
        self.local.execute('UPDATE store_directory SET store_id=0 WHERE store_id=1')
        self.local.execute('UPDATE store_directory SET store_id=1 WHERE store_id=-1')
        self.local.commit()
        report=publish(self.path,self.remote,True)
        self.assertTrue(report['applied'])
        self.assertEqual(report['tables']['store_directory']['upsert'],2)
        self.assertEqual(report['tables']['store_directory']['delete'],2)
        self.assertEqual([(r['store_id'],r['store_uuid']) for r in self.remote.query(
            'SELECT store_id,store_uuid FROM store_directory ORDER BY store_id')],
            [(0,'uuid-b'),(1,'uuid-a')])

    def test_rebuilt_single_store_reuses_previous_uuid_with_new_id(self):
        self.local.execute(
            "INSERT INTO store_directory(store_id,store_uuid,name,chunk_count) VALUES(1,'uuid-a','A',1)")
        self.local.commit()
        publish(self.path,self.remote,True)
        self.local.execute("UPDATE store_directory SET store_id=0 WHERE store_uuid='uuid-a'")
        self.local.commit()
        report=publish(self.path,self.remote,True)
        self.assertTrue(report['applied'])
        self.assertEqual(report['tables']['store_directory']['delete'],1)
        self.assertEqual([(r['store_id'],r['store_uuid']) for r in self.remote.query(
            'SELECT store_id,store_uuid FROM store_directory')],[(0,'uuid-a')])

    def test_original_database_url_is_rejected(self):
        with self.assertRaises(ValueError):
            Remote('https://ubereats-packed-v1-eeetchen.aws-ap-northeast-1.turso.io','unused')

    def test_named_directory_comparison_ignores_response_column_order(self):
        from publish_turso_delta import plan_table
        row = {'store_id':1,'store_uuid':'uuid','name':'Test','rating':4.5}
        changed,deleted = plan_table('store_directory',[row],{(1,):dict(reversed(list(row.items())))})
        self.assertEqual((changed,deleted),([],[]))

    def test_float_transport_rounding_does_not_trigger_daily_writes(self):
        from publish_turso_delta import plan_table
        row={'store_id':1,'latitude':25.133664341011666,'rating':4.5}
        published={(1,):{**row,'latitude':25.133664341011663}}
        self.assertEqual(plan_table('store_directory',[row],published),([],[]))
        published[(1,)]['rating']=4.4
        self.assertEqual(len(plan_table('store_directory',[row],published)[0]),1)

    def test_verified_full_raw_replay_proof_is_required_for_deliberate_shrink(self):
        from publish_turso_delta import verified_rebuild_evidence
        import tempfile
        local={'latest_batch':'"20261027144628"',
               'source_counts':json.dumps({'stores':546,'products':22357,'crawl_batches':49})}
        prior={'source_counts':json.dumps({'stores':2183,'products':109382,'crawl_batches':49})}
        self.assertFalse(verified_rebuild_evidence(None,local,prior))
        with tempfile.TemporaryDirectory() as temp:
            evidence=Path(temp)/'proof.json'
            proof={'format':'foodradar-full-raw-replay-v2','batch_id':'20261027144628',
                   'verified_manifest_count':49,'manifest_digest':'a'*64,
                   'source_counts':{'stores':546,'products':22357,'crawl_batches':49}}
            evidence.write_text(json.dumps(proof))
            self.assertTrue(verified_rebuild_evidence(evidence,local,prior))
            proof['verified_manifest_count']=48
            evidence.write_text(json.dumps(proof))
            with self.assertRaisesRegex(ValueError,'Full Raw replay proof'):
                verified_rebuild_evidence(evidence,local,prior)

if __name__ == '__main__':
    unittest.main()
