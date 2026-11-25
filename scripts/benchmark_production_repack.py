"""Read-only real-checkpoint benchmark of the production repack path."""
import hashlib,json,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from compact_repack import select,digest
p=ROOT/"data/state-checkpoint"
m=json.loads((p/"manifest.json").read_text())
parts=m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]
results=[]
for part in parts:
    source=p/part["path"]
    assert hashlib.sha256(source.read_bytes()).hexdigest()==part["sha256"]
    with tempfile.TemporaryDirectory() as temp:
        chosen,report=select(source,temp)
        if chosen:
            assert digest(chosen)==digest(source)
        results.append(report)
        print("FR_PRODUCTION_REPACK="+json.dumps(report),flush=True)
    assert hashlib.sha256(source.read_bytes()).hexdigest()==part["sha256"]
print("FR_PRODUCTION_REPACK_TOTAL="+json.dumps({"batch":m["batch_id"],
      "before_bytes":sum(r["before_bytes"] for r in results),
      "after_bytes":sum(r["after_bytes"] for r in results),"parts":len(results)}),flush=True)
