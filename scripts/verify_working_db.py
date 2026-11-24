"""Real production checkpoint benchmark; no Raw changes or deployment."""
import json,sqlite3,sys,time,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts")]
from compact_checkpoint import expand,create
from state_checkpoint import compatible_contract
from verified_weekly import update_verified_weekly
from audit_deal_pipeline import report as audit
from build_web_db import build_web_db
from build_static_catalog import build,verify
cache=Path("data/state-checkpoint")
meta=json.loads((cache/"manifest.json").read_text())
assert compatible_contract(meta,ROOT)
source=cache/"serving.compact.db"
root=Path("data/working-benchmark");root.mkdir(parents=True,exist_ok=True)
result={}
manifests=[]
for name,lean in (("old",False),("new",True)):
    started=time.perf_counter()
    dbpath=root/(name+".db")
    counts=expand(source,dbpath,lean=lean)
    restored=time.perf_counter()-started
    with sqlite3.connect(dbpath) as db:
        t=time.perf_counter()
        weekly=update_verified_weekly(db,meta["batch_id"])
        weekly_seconds=time.perf_counter()-t
        assert db.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        sizes=dict(db.execute("SELECT name,sum(pgsize) FROM dbstat GROUP BY name ORDER BY 2 DESC"))
        fields=("stores","products","product_price_observations")
        digests={}
        for table,order in zip(fields,("store_uuid","store_uuid,product_uuid","batch_id,store_uuid,product_uuid")):
            h=hashlib.sha256()
            for row in db.execute(f"SELECT * FROM {table} ORDER BY {order}"):
                h.update(json.dumps(row,ensure_ascii=False,separators=(",",":")).encode())
            digests[table]=h.hexdigest()
    checked=audit(dbpath)
    packed=root/(name+"-packed.db")
    build_web_db(dbpath,packed,static_only=True)
    static=root/(name+"-site")
    manifest=build(packed,dbpath,static);verify(static)
    manifests.append(manifest)
    result[name]={"working_bytes":dbpath.stat().st_size,"counts":counts,"restore_seconds":round(restored,3),
       "weekly_seconds":round(weekly_seconds,3),"weekly":weekly,"digests":digests,"audit":checked,
       "static_bytes":manifest["bytes"],"static_counts":manifest["counts"],
       "objects":sizes,"total_seconds":round(time.perf_counter()-started,3)}
assert result["old"]["digests"]==result["new"]["digests"]
assert result["old"]["weekly"]==result["new"]["weekly"]
assert result["old"]["audit"]==result["new"]["audit"]
assert manifests[0]["files"]==manifests[1]["files"],"Web payload differs"
assert result["new"]["working_bytes"]<result["old"]["working_bytes"]
repacked=root/"repacked.db"; info=create(root/"new.db",repacked)
assert info["bytes"]<20_000_000
result["repacked_bytes"]=info["bytes"]
result["static_payloads_identical"]=True
print("WORKING_DB_BENCHMARK="+json.dumps(result,ensure_ascii=False),flush=True)
Path("working-db-report.json").write_text(json.dumps(result,ensure_ascii=False,indent=2))
