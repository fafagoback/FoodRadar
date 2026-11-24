"""Roundtrip + next batch must reproduce live classification without old events."""
import contextlib
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"tests"))
from compact_checkpoint import create,expand
from test_packed_updates import apply_snapshot,document

SID="ffffffff-ffff-4fff-8fff-ffffffffffff"
PID="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

class CompactIncrementalTests(unittest.TestCase):
    def test_new_batch_matches_original_state_and_prices(self):
        with tempfile.TemporaryDirectory() as d,contextlib.redirect_stdout(io.StringIO()):
            root=Path(d)
            first=root/"first.db"
            with sqlite3.connect(first) as original:
                original.row_factory=sqlite3.Row
                for batch,price in [("20261121133219",100),("20261122133219",150)]:
                    doc=document(SID,[PID])
                    doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["offers"]["price"]=price
                    apply_snapshot(original,[doc],batch)
            compact=root/"compact.db"
            create(first,compact)
            resumed=root/"resumed.db"
            expand(compact,resumed)
            for path in (first,resumed):
                with sqlite3.connect(path) as conn:
                    conn.row_factory=sqlite3.Row
                    doc=document(SID,[PID])
                    doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["offers"]["price"]=100
                    apply_snapshot(conn,[doc],"20261123133219")
            with sqlite3.connect(first) as a,sqlite3.connect(resumed) as b:
                tables=[
                    ("stores","store_uuid","price_evidence_source_hash"),
                    ("products","store_uuid,product_uuid","price_evidence_source_hash"),
                    ("product_price_observations","batch_id,store_uuid,product_uuid","source_hash")
                ]
                for table,order,excluded in tables:
                    columns=[r[1] for r in a.execute(f"PRAGMA table_info({table})") if r[1]!=excluded]
                    sql=f"SELECT {','.join(columns)} FROM {table} ORDER BY {order}"
                    rows1=a.execute(sql).fetchall()
                    rows2=b.execute(sql).fetchall()
                    if table=="products":
                        j=columns.index("menu_quote_history")
                        rows1=[tuple(json.loads(v) if k==j else v for k,v in enumerate(row)) for row in rows1]
                        rows2=[tuple(json.loads(v) if k==j else v for k,v in enumerate(row)) for row in rows2]
                    self.assertEqual(rows1,rows2,table)
