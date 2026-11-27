"""Build a FR-only SQL overlay on a verified compact checkpoint without UE calls."""
import argparse
import hashlib
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
sys.path.insert(0,str(ROOT/"src"))
from build_relational_db import load_checkpoint
from relational_overlay import build

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--checkpoint",default="data/state-checkpoint")
    p.add_argument("--output",required=True)
    p.add_argument("--report")
    a=p.parse_args()
    state,_=load_checkpoint(Path(a.checkpoint))
    try: data=build(state,a.output)
    finally:state.close()
    h=hashlib.sha256()
    with Path(a.output).open("rb") as fp:
        for part in iter(lambda:fp.read(1024*1024),b""):h.update(part)
    data["sha256"]=h.hexdigest()
    if a.report:
        Path(a.report).write_text(json.dumps(data,ensure_ascii=False,indent=2))
    print("FR_RELATIONAL_OVERLAY="+json.dumps(data,ensure_ascii=False),flush=True)

if __name__=="__main__":main()
