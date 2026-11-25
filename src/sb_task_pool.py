"""Run-scoped Supabase task pool. No raw bodies or thread-learning state."""
from __future__ import annotations

import datetime
import json
import os
import random
import threading
import time
import uuid
import urllib.request
import urllib.parse
import urllib.error
from crawl_queue import initial_state, validate_state, validate_batch_group

ENDPOINT = "https://unapiwajmgqqtdkixaab.supabase.co/functions/v1/fr-task-pool"
AUDIENCE = "foodradar:task-pool"

class SupabaseQueue:
    def __init__(self, repo, run_id, attempt=1):
        self.run_id = str(run_id)
        self.attempt = int(attempt)
        self.lock = threading.RLock()
        self.token = None
        self.token_until = 0
        self.calls = 0
        self.rate_limits = 0
        self.retry_seconds = 0.0

    def _oidc(self):
        if self.token is None or time.monotonic() >= self.token_until:
            url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
            url += ("&" if "?" in url else "?") + urllib.parse.urlencode({"audience": AUDIENCE})
            req = urllib.request.Request(url,headers={
                "Authorization":"Bearer "+os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
            with urllib.request.urlopen(req,timeout=15) as res:
                self.token = json.load(res)["value"]
            self.token_until = time.monotonic()+180
        return self.token

    def _rpc(self, op, **args):
        # Exactly one coordinator request at a time per process; retries retain
        # the same request_id and completion fencing tokens.
        body = json.dumps({"op":op,"args":args},separators=(",",":")).encode()
        with self.lock:
            for attempt in range(5):
                retry_after = 0
                try:
                    req = urllib.request.Request(ENDPOINT,data=body,headers={
                        "Content-Type":"application/json","X-FR-Workflow-Token":self._oidc()})
                    self.calls += 1
                    with urllib.request.urlopen(req,timeout=20) as res:
                        return json.load(res)
                except urllib.error.HTTPError as exc:
                    if exc.code == 401 and attempt == 0:
                        self.token = None
                        continue
                    if exc.code == 429:
                        self.rate_limits += 1
                        try:
                            retry_after = float(exc.headers.get("Retry-After","0"))
                        except ValueError:
                            retry_after = 0
                    elif exc.code not in (502,503,504,500,408):
                        raise RuntimeError(f"SB pool {op} HTTP {exc.code}") from None
                except (OSError, TimeoutError):
                    pass
                if attempt == 4:
                    break
                delay = max(retry_after,min(20,2**attempt)+random.random())
                self.retry_seconds += delay
                time.sleep(delay)
            raise RuntimeError("SB pool "+op+" exhausted retries")

    @staticmethod
    def _records(stores,sections):
        return ([{"task_key":sid,"token":token} for sid,token in stores.items()]
                + [{"task_key":sid+":section:"+key,"token":token}
                   for sid,keys in sections.items() for key,token in keys.items()])

    @staticmethod
    def _lease(row):
        result = row.get("result") or {}
        out = {"owner":result.get("owner"),"worker":row["worker_id"]-1,
               "token":row["lease_token"].replace("-",""),"attempt":row["retry_count"]}
        expiry = row.get("lease_expires_at")
        if expiry:
            out["expires"] = datetime.datetime.fromisoformat(expiry.replace("Z","+00:00")).timestamp()
        for key in ("artifact_id","batch_name"):
            if key in result:
                out[key] = result[key]
        return out

    def initialize(self,state):
        ids = state["tasks"]
        for i in range(0,len(ids),1000):
            self._rpc("seed",epoch=state["epoch"],source_sha=state["source_sha"],
                      rows=[{"sid":sid,"weight":state.get("weights",{}).get(sid,400)}
                            for sid in ids[i:i+1000]])
        actual=self.read()
        for key in ("run_id","source_sha","epoch","tasks","weights"):
            if actual[key]!=state[key]:
                raise ValueError("SB task pool initialization mismatch: "+key)
        return actual

    def read(self):
        rows=[]; after=0
        while True:
            page=self._rpc("read",after=after)
            rows.extend(page)
            if len(page)<1000:
                break
            after=page[-1]["id"]
        stores=[r for r in rows if r["task_type"]=="store"]
        if not stores:
            raise ValueError("SB task pool missing or already cleared")
        first=stores[0]["payload"]
        ids=[r["task_key"] for r in stores]
        state=initial_state(self.run_id,first["epoch"],first["source_sha"],ids,
                            {r["task_key"]:r["priority"] for r in stores})
        state["pending"]=[]; state["catalog_groups"]={}
        def place(target,key,row):
            status=row["status"]
            if status in ("pending","retry"):
                target["pending"].append(key)
            elif status in ("running","local_done","completed"):
                target["leased" if status=="running" else status][key]=self._lease(row)
            else:
                raise ValueError("Unexpected SB task status "+status)
        for row in stores:
            if any(row["payload"][k]!=first[k] for k in ("epoch","source_sha")):
                raise ValueError("Mixed SB run identity")
            place(state,row["task_key"],row)
        for row in sorted((r for r in rows if r["task_type"]=="section"),
                          key=lambda r:(r["payload"]["sid"],tuple(map(int,r["payload"]["key"].split("."))))):
            p=row["payload"]; sid=p["sid"]; key=p["key"]
            if sid not in state["tasks"]:
                raise ValueError("Orphan native category")
            group=state["catalog_groups"].setdefault(sid,{"format":"batch-section-v2",
                "sections":[],"pending":[],"leased":{},"local_done":{},"completed":{},"attempts":{},"partitions":{}})
            if "." in key:
                group["partitions"][key]={"section_uuid":p["section_uuid"],"sub_path":p["sub_path"]}
            else:
                if int(key)!=len(group["sections"]):
                    raise ValueError("Native category roster gap")
                group["sections"].append(p["section_uuid"])
            group["attempts"][key]=row["retry_count"]
            place(group,key,row)
        for sid,group in state["catalog_groups"].items():
            validate_batch_group(sid,group)
        return validate_state(state)

    def exchange(self,owner,worker,slots,stores,sections):
        records=self._records(stores,sections)
        result=self._rpc("exchange",worker=worker+1,owner=owner,slots=slots,
                         finished=records,request_id=uuid.uuid4().hex)
        if result["finished"]!=len(records):
            raise RuntimeError("SB completion count mismatch")
        jobs=[]
        for row in result["claimed"]:
            p=row["payload"]; lease=self._lease(row)
            if row["task_type"]=="section":
                lease["section_uuid"]=p["section_uuid"]
                lease["sub_path"]=p.get("sub_path",[])
                jobs.append(("section",p["sid"],p["key"],lease))
            elif row["task_type"]=="store":
                jobs.append(("store",p["sid"],None,lease))
            else:
                raise ValueError("Unsupported SB task type")
        if len(jobs)>slots:
            raise ValueError("Claim exceeds available slots")
        return jobs,bool(result["drained"])

    def register_catalog(self,sid,token,sections,weights=None):
        return self._rpc("split",sid=sid,token=token,sections=sections,weights=weights or {})["sections"]

    def register_children(self,sid,key,token,children):
        children=sorted(set(children))
        return self._rpc("split",sid=sid,key=key,token=token,sections=children)["sections"]

    def finalize_batch(self,owner,stores,sections,artifact_id,batch_name):
        records=self._records(stores,sections)
        for i in range(0,len(records),1000):
            chunk=records[i:i+1000]
            if self._rpc("finalize",owner=owner,finished=chunk,artifact_id=artifact_id,
                         batch_name=batch_name)["completed"]!=len(chunk):
                raise RuntimeError("Artifact completion count mismatch")
        return len(stores),sum(map(len,sections.values()))

    def finish_batch(self,owner,records,artifact_id,batch_name):
        return self.finalize_batch(owner,records,{},artifact_id,batch_name)[0]

    def finish_section_batch(self,owner,records,artifact_id,batch_name):
        return self.finalize_batch(owner,{},records,artifact_id,batch_name)[1]

    def reset_terminated_wave(self):
        return self._rpc("reset")["reset"]

    def cleanup(self):
        return self._rpc("cleanup")["deleted"]

    def purge(self):
        return self._rpc("purge")["deleted"]

    def metrics(self):
        return {"rpc_calls":self.calls,"http_429":self.rate_limits,
                "retry_seconds":round(self.retry_seconds,2)}
