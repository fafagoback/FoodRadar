"""Verify against the exact previously published source artifact; no large DB."""
import json,sys,time,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts")]
from compact_direct import open_collection
from prototype_pack_db import build as pack
from build_static_catalog import build as publish,verify
from audit_deal_pipeline import report
from audit_store_identity import audit_stores
cache=Path("data/state-checkpoint")
meta=json.loads((cache/"manifest.json").read_text())
reference=json.loads(Path("reference-static/current.json").read_text())
paths=[cache/p["path"] for p in meta["parts"]] if meta.get("parts") else [cache/"serving.compact.db"]
for i,path in enumerate(paths):
    with path.open("rb") as f:
        expected=meta["parts"][i]["sha256"] if meta.get("parts") else meta["compact_sha256"]
        assert hashlib.file_digest(f,"sha256").hexdigest()==expected
started=time.perf_counter()
state=open_collection(paths)
try:
    assert state.stats()=={k:meta[k] for k in ("stores","products","observations")}
    weekly=state.refresh_weekly(meta["batch_id"])
    audit=report(direct=state)
    assert not audit["errors"]
    identity=audit_stores(direct=state)
    assert identity["total_store_rows"]==meta["stores"]
    projection=pack(None,None,static_only=True,direct=state)
    result=publish(None,None,Path("data/direct-static"),direct=state,projection=projection)
    verify(Path("data/direct-static"))
    assert reference["batch"]==result["batch"]
    for key in ("files","summary","counts","data_revision"):
        assert reference[key]==result[key],(key,"previous website differs")
    stats=state.metrics()
    assert stats["compact_bytes"]<20_000_000
    assert not list(Path("data").rglob("serving.db"))
    assert not list(Path("data").rglob("packed-serving.db"))
    result={"counts":state.stats(),"metrics":stats,"weekly":weekly,
            "audit":audit,"site_bytes":result["bytes"],"web_payloads_identical":True,
            "projection_compressed_bytes":projection.validate()["bytes"],
            "seconds":round(time.perf_counter()-started,2)}
    print("DIRECT_COMPACT_REAL_VERIFIED="+json.dumps(result,ensure_ascii=False),flush=True)
    Path("direct-compact-report.json").write_text(json.dumps(result,ensure_ascii=False,indent=2))
finally:state.close()
