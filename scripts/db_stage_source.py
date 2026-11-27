"""Validate the exact Raw batch artifact created by completed crawler stage 16."""
from __future__ import annotations
import argparse
import json
from datetime import datetime
from pathlib import Path

def validate_receipt(value, run_id, source_sha):
    if not isinstance(value,dict):
        raise ValueError("Invalid crawler receipt")
    batch=value.get("batch_id")
    if (value.get("source_run_id")!=str(run_id) or value.get("source_sha")!=source_sha or
        not isinstance(batch,str) or len(batch)!=14 or not batch.isdecimal() or
        type(value.get("stores")) is not int or value["stores"]<=0):
        raise ValueError("Raw source Run/SHA/batch/roster does not match completed crawler")
    datetime.strptime(batch,"%Y%m%d%H%M%S")
    return {"source_run_id":str(run_id),"batch_id":batch,"stores":value["stores"]}

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--path",required=True)
    p.add_argument("--source-run",required=True)
    p.add_argument("--source-sha",required=True)
    p.add_argument("--output",required=True)
    args=p.parse_args()
    if (not args.source_run.isdecimal() or len(args.source_sha)!=40 or
        any(c not in "0123456789abcdef" for c in args.source_sha)):
        raise ValueError("Invalid workflow_run provenance")
    receipt=validate_receipt(json.loads(Path(args.path).read_text(encoding="utf-8")),
                             args.source_run,args.source_sha)
    with Path(args.output).open("a",encoding="utf-8") as out:
        for key in ("source_run_id","batch_id"):
            out.write(key+"="+receipt[key]+"\n")
    print("FR_DB_WORKFLOW_RUN_SOURCE="+json.dumps(receipt),flush=True)
