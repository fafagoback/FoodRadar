"""Real-checkpoint round-trip: exact product fields and verified price amounts."""
from pathlib import Path
import json
import sqlite3
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from compact_checkpoint import create,expand

source=Path(sys.argv[1])
with tempfile.TemporaryDirectory() as temp:
    compact=Path(temp)/"state.compact.db"
    expanded=Path(temp)/"expanded.db"
    started=time.perf_counter()
    stats=create(source,compact)
    assert compact.stat().st_size < 20_000_000,stats
    reconstructed=expand(compact,expanded)
    assert all(stats[k]==reconstructed[k] for k in ("stores","products"))
    assert stats["observations"]==reconstructed["observations"]
    with sqlite3.connect(source) as a, sqlite3.connect(expanded) as b:
        for table in ("stores","products","crawl_batches","metadata"):
            columns=[r[1] for r in a.execute(f"PRAGMA table_info({table})")]
            if table=="products": columns=[c for c in columns if c!="price_evidence_source_hash"]
            columns=",".join(columns)
            original=list(a.execute(f"SELECT {columns} FROM {table} ORDER BY 1,2"))
            rebuilt=list(b.execute(f"SELECT {columns} FROM {table} ORDER BY 1,2"))
            assert original==rebuilt,(table,len(original),len(rebuilt),next(((x,y) for x,y in zip(original,rebuilt) if x!=y),None))
        fields=("batch_id","store_uuid","product_uuid","observed_at","menu_price_cents","promotion_type",
                "buy_quantity","free_quantity","received_quantity","original_total_cents","payable_total_cents")
        sql="SELECT "+",".join(fields)+" FROM product_price_observations ORDER BY batch_id,store_uuid,product_uuid"
        original=a.execute(sql)
        rebuilt=b.execute(sql)
        checked=0
        while True:
            batchA=original.fetchmany(10000)
            batchB=rebuilt.fetchmany(10000)
            assert batchA==batchB,(checked,batchA[:1],batchB[:1])
            checked+=len(batchA)
            if not batchA:break
        print("DB_COMPACT_ROUNDTRIP="+json.dumps({**stats,"verified_price_rows":checked,
            "seconds":round(time.perf_counter()-started,2)},ensure_ascii=False),flush=True)
