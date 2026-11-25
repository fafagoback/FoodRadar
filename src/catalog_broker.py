"""Work stealing for actual native category partitions and immutable page resume.

The store owner retains its ordinary store lease. Independent native categories
are additional tasks in the same Git CAS lane; any of the fifteen workers can
claim them. No historical work estimates, external queue or database service.
Only verified results/HTTP bytes live in Actions artifacts, never in Git.
"""
from __future__ import annotations

import hashlib
import json
import random
import re
import shutil
import tempfile
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests

from catalog_complete import CATALOG_API, _catalog_entries, _catalog_skus, _section_ids, capture_catalog
from menu_core import attach_delivery_location, global_limiter, response_retry_after
from raw_capture import capture_response, TW, CheckpointStorageError

MIN_SECTIONS = 8


def wire(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf8")


def sha(blob):
    return hashlib.sha256(blob).hexdigest()


def checks(raw):
    return {p.relative_to(raw).as_posix(): sha(p.read_bytes()) for p in sorted(raw.rglob("*.body"))}


def copy_raw(source, dest):
    for relative, digest in checks(source).items():
        file = dest / relative
        file.parent.mkdir(parents=True, exist_ok=True)
        if file.exists():
            if sha(file.read_bytes()) != digest:
                raise ValueError("Conflicting immutable HTTP response")
        else:
            shutil.copyfile(source / relative, file)


def validate_group(sid, group):
    keys = set(map(str, range(len(group["sections"]))))
    p, l, c = set(group["pending"]), set(group["leased"]), set(group["completed"])
    if len(p) != len(group["pending"]) or p & l or p & c or l & c or p | l | c != keys:
        raise ValueError("Invalid catalog partition roster")
    if len(set(group["sections"])) != len(group["sections"]) or not group["sections"]:
        raise ValueError("Duplicate native category")
    if type(group["plan_id"]) is not int or group["plan_id"] <= 0:
        raise ValueError("Missing immutable catalog plan")
    if not re.fullmatch(r"[0-9a-f]{64}", group["plan_sha"]):
        raise ValueError("Invalid catalog plan digest")
    for key, row in {**group["leased"], **group["completed"]}.items():
        if not 0 <= row["worker"] < 15 or not re.fullmatch("[a-f0-9]{32}", row["token"]):
            raise ValueError("Invalid category task lease")
        if key in c and (type(row.get("artifact_id")) is not int or row["artifact_id"] <= 0):
            raise ValueError("Missing immutable category result")
    return group


class PageJournal:
    """Replay the exact successful native page of this capture, not cached prices.

    Persist every successful native page before advancing, including terminal
    single-page categories. Failures, invalid paging, and independent promotion
    rechecks never use these checkpoints.
    """
    def __init__(self, session, storage, prefix, recover=False, save_terminal=False):
        self.session = session
        self.storage = storage
        self.prefix = prefix
        self.recover = recover
        self.save_terminal = save_terminal
        self.replayed = 0
        self.saved = 0
        self.local = {}

    def _read(self, folder, expected):
        doc = json.loads((folder / "page.json").read_text())
        body = (folder / "response.body").read_bytes()
        if doc != {"request": expected, "sha256": sha(body), "status": 200}:
            raise ValueError("Page checkpoint provenance mismatch")
        data, _ = _catalog_entries(json.loads(body))
        offset = expected["pagingInfo"]["offset"]
        p = data["pagingInfo"]
        if p["hasMore"] and (type(p.get("offset")) is not int or p["offset"] <= offset):
            raise ValueError("Page checkpoint has stalled pagination")
        response = requests.Response()
        response.status_code = 200
        response._content = body
        return response

    def post(self, url, *, json, headers, timeout):
        if url != CATALOG_API:
            return self.session.post(url, json=json, headers=headers, timeout=timeout)
        key = sha(wire(json))
        if key in self.local:
            return self.local[key]
        name = self.prefix + "-" + key
        if self.recover:
            with tempfile.TemporaryDirectory() as temp:
                try:
                    found = self.storage("recover", name, temp)
                except Exception as exc:
                    raise CheckpointStorageError("Could not verify saved native catalog page") from exc
                if found is not None:
                    self.replayed += 1
                    response = self._read(Path(temp), json)
                    self.local[key] = response
                    return response
        response = self.session.post(url, json=json, headers=headers, timeout=timeout)
        if response.status_code != 200:
            return response
        try:
            data, _ = _catalog_entries(response.json())
        except (ValueError, TypeError, KeyError):
            return response
        paging = data["pagingInfo"]
        offset = json["pagingInfo"]["offset"]
        if paging["hasMore"] and (type(paging.get("offset")) is not int or paging["offset"] <= offset):
            return response
        if not self.save_terminal and offset == 0 and not paging["hasMore"]:
            return response
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            body = response.content
            (folder / "response.body").write_bytes(body)
            (folder / "page.json").write_bytes(wire({"request": json, "sha256": sha(body), "status": 200}))
            try:
                self.storage("upload", name, folder)
            except Exception as exc:
                raise CheckpointStorageError("Could not durably save native catalog page") from exc
            self.saved += 1
        self.local[key] = response
        return response


class CatalogBroker:
    def __init__(self, queue, identity, discovered, owner, worker, storage, session_factory):
        self.queue, self.identity, self.discovered = queue, identity, discovered
        self.owner, self.worker, self.storage = owner, worker, storage
        self.session_factory = session_factory
        self.metrics = {"partitions": 0, "remote_partitions": 0, "saved_pages": 0, "resumed_pages": 0}
        self.metrics_lock = threading.Lock()

    def _lane(self, sid):
        with self.queue.lock:
            self.queue._ensure()
            return self.queue.lanes[self.queue.lane_of[sid]]

    def _edit(self, sid, op):
        return self._lane(sid).transact(op)

    def _group(self, sid):
        g = self._lane(sid).read().get("catalog_groups", {}).get(sid)
        return validate_group(sid, g) if g else None

    def _name(self, kind, sid, suffix=""):
        return "fr-" + kind + "-" + self.identity["epoch"] + "-" + sid + suffix

    def _identity(self, sid):
        return {k:self.identity[k] for k in ("run_id", "source_sha", "epoch")} | {"store_uuid": sid}

    def _read_plan(self, folder, sid):
        p = json.loads((folder / "plan.json").read_text())
        if p["identity"] != self._identity(sid) or p["item"] != self.discovered[sid]:
            raise ValueError("Catalog plan belongs to another store/run")
        actual = checks(folder / "raw")
        if actual != p["responses"] or p["response_path"] not in actual:
            raise ValueError("Catalog plan original HTTP checksum mismatch")
        doc = json.loads((folder / "raw" / p["response_path"]).read_bytes())
        data = doc.get("data")
        if (not isinstance(data, dict) or doc.get("status") == "failure"
                or p["sections"] != _section_ids(data)
                or (data.get("catalogSectionPagingInfo") or {}).get("hasMore") is True):
            raise ValueError("Catalog plan native category evidence mismatch")
        return p, data

    def _register(self, sid, plan, artifact_id):
        expected = {"plan_id": artifact_id, "plan_sha": sha(wire(plan)),
                    "sections": plan["sections"], "origin_worker": plan["worker"]}
        def edit(state):
            if sid not in state["tasks"]:
                raise ValueError("Unrecognized store")
            groups = state.setdefault("catalog_groups", {})
            old = groups.get(sid)
            if old is not None:
                if any(old[k] != v for k,v in expected.items()):
                    raise ValueError("Catalog plan cannot be replaced")
                return False, old
            group = expected | {"pending": list(map(str,range(len(plan["sections"])))),
                                "leased": {}, "completed": {}, "attempts": {}}
            groups[sid] = group
            return True, validate_group(sid, group)
        return self._edit(sid, edit)

    def _load_plan(self, sid, folder, group=None):
        found = self.storage("recover", self._name("plan",sid), folder)
        if found is None:
            if group:
                raise RuntimeError("Registered catalog plan artifact missing")
            return None
        plan, data = self._read_plan(folder,sid)
        if group and (found["id"] != group["plan_id"] or sha(wire(plan)) != group["plan_sha"]):
            raise ValueError("Catalog plan artifact identity changed")
        return plan,data,found["id"]

    def dispatch(self, item, raw, response_path, data, headers, attempt):
        sid = item["store_uuid"]
        sections = _section_ids(data)
        if not sections or (data.get("catalogSectionPagingInfo") or {}).get("hasMore") is True:
            return None
        plan={"identity":self._identity(sid), "item":item, "sections":sections,
              "headers":headers, "store_attempt":attempt, "response_path":response_path,
              "worker":self.worker, "responses":checks(raw),
              "captured_at":datetime.now(TW).isoformat()}
        # Small stores keep one owner; combine the plan with the first valid
        # page to avoid a separate metadata upload. Every valid page can be
        # recovered by the next owner of this exact run, even terminal pages.
        if len(sections) < MIN_SECTIONS:
            return self._run_small(sid, raw, plan, data)
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp)
            copy_raw(raw,folder/"raw")
            (folder/"plan.json").write_bytes(wire(plan))
            self._read_plan(folder,sid)
            saved=self.storage("upload",self._name("plan",sid),folder)
            self._register(sid,plan,saved["id"])
        return self.finish_catalog(sid,raw)

    def resume(self, sid, raw, attempt):
        # On normal first claims avoid an artifact lookup on every small store.
        if attempt <= 1:
            return None
        group=self._group(sid)
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp)
            loaded=self._load_plan(sid,folder,group)
            if loaded is None:
                return self._resume_small(sid,raw)
            plan,data,artifact_id=loaded
            self._register(sid,plan,artifact_id)
            copy_raw(folder/"raw",raw)
        catalog=self.finish_catalog(sid,raw)
        return {"store_uuid":sid,"store_url":self.discovered[sid]["store_url"],
                "success":True,"response_path":plan["response_path"],
                "captured_at":datetime.now(TW).isoformat(),**catalog}

    def _resume_small(self,sid,raw):
        with tempfile.TemporaryDirectory() as temp:
            first=Path(temp)
            try:
                found=self.storage("recover",self._name("small",sid),first)
            except Exception as exc:
                raise CheckpointStorageError("Cannot verify small store checkpoint") from exc
            if found is None:
                return None
            plan,data=self._read_plan(first,sid)
            if not 0 < len(plan["sections"]) < MIN_SECTIONS:
                raise ValueError("Small store plan has invalid category roster")
            copy_raw(first/"raw",raw)
            catalog=self._run_small(sid,raw,plan,data,first=first,first_id=found["id"])
            return {"store_uuid":sid,"store_url":plan["item"]["store_url"],"success":True,
                    "response_path":plan["response_path"],"captured_at":datetime.now(TW).isoformat(),**catalog}

    def _run_small(self,sid,raw,plan,data,first=None,first_id=None):
        # The first immutable page artifact includes the original native base.
        # Only metadata in the plan is copied; later raw files are separate.
        prefix=self._name("page",sid,"-small-"+sha(wire(plan))[:16])
        first_key=None
        if first is not None:
            page=json.loads((first/"page.json").read_text())
            first_key=prefix+"-"+sha(wire(page["request"]))
        def storage(op,name,folder):
            nonlocal first_key,first_id
            folder=Path(folder)
            if op=="recover" and name==first_key and first is not None:
                for file in ("page.json","response.body"):
                    shutil.copyfile(first/file,folder/file)
                return {"id":first_id}
            if op=="upload" and first_key is None:
                folder.joinpath("plan.json").write_bytes(wire(plan))
                for path,digest in plan["responses"].items():
                    source=raw/path
                    if sha(source.read_bytes())!=digest:
                        raise ValueError("Original small store metadata changed")
                    dest=folder/"raw"/path
                    dest.parent.mkdir(parents=True,exist_ok=True)
                    shutil.copyfile(source,dest)
                self._read_plan(folder,sid)
                saved=self.storage("upload",self._name("small",sid),folder)
                first_key=name
                return saved
            return self.storage(op,name,folder)
        session=self.session_factory()
        try:
            if not attach_delivery_location(session,plan["item"]):
                raise ValueError("Missing native delivery location")
            journal=PageJournal(session,storage,prefix,recover=first is not None,save_terminal=True)
            refresh=self._refresh(session,sid,raw,plan,"small")
            result=capture_catalog(journal,sid,data,raw,plan["headers"],capture_response,
                store_attempt=plan["store_attempt"],refresh_native_metadata=refresh)
        finally:
            session.close()
        with self.metrics_lock:
            self.metrics["saved_pages"]+=journal.saved
            self.metrics["resumed_pages"]+=journal.replayed
        print("FR_SMALL_STORE_PAGES="+json.dumps({"store":sid,"saved":journal.saved,
              "resumed":journal.replayed}),flush=True)
        return result

    def _refresh(self,session,sid,raw,plan,key):
        refresh_number=0
        def refresh():
            nonlocal refresh_number
            refresh_number+=1
            global_limiter.wait()
            response=session.post("https://www.ubereats.com/api/getStoreV1",
                json={"storeUuid":sid,"sfNuggetCount":24},headers=plan["headers"],timeout=12)
            capture_response(raw,f"section-refresh/{sid}-{key}-{refresh_number:02d}-{response.status_code}.body",response)
            body=response.json()
            data=body.get("data")
            if response.status_code==429 or (body.get("status")=="failure" and isinstance(data,dict) and str(data.get("code"))=="429"):
                global_limiter.trigger_backoff(30,"Native category metadata refresh 429",
                    retry_after=response_retry_after(response))
            if response.status_code!=200 or body.get("status")=="failure" or not isinstance(data,dict):
                raise RuntimeError(f"Native metadata refresh unavailable HTTP {response.status_code}")
            if data.get("storeUuid") not in (None,sid):
                raise ValueError("Native metadata refresh changed store")
            global_limiter.record_success()
            return data
        return refresh

    def claim(self, sid):
        token=uuid.uuid4().hex
        def edit(state):
            group=state.get("catalog_groups",{}).get(sid)
            if not group:
                return False,None
            for key,row in group["leased"].items():
                if row["token"]==token:
                    return False,(sid,key,row)
            if not group["pending"]:
                return False,None
            key=group["pending"].pop(0)
            group["attempts"][key]=group["attempts"].get(key,0)+1
            row={"owner":self.owner,"worker":self.worker,"token":token,
                 "expires":time.time()+self.queue.root.lease_seconds,
                 "attempt":group["attempts"][key]}
            group["leased"][key]=row
            validate_group(sid,group)
            return True,(sid,key,row)
        result=self._edit(sid,edit)
        if result:
            with self.queue.lock:
                self.queue.owner_lanes.setdefault(self.owner,set()).add(self.queue.lane_of[sid])
        return result

    def claim_any(self,state):
        groups=[sid for sid,g in state.get("catalog_groups",{}).items() if g["pending"]]
        random.shuffle(groups)
        for sid in groups:
            result=self.claim(sid)
            if result:
                return result
        return None

    def _result_name(self,sid,key,lease):
        return self._name("section",sid,"-"+key+"-"+lease["token"])

    def _read_result(self,folder,sid,key,lease,group):
        doc=json.loads((folder/"result.json").read_text())
        if (doc["identity"]!=self._identity(sid) or doc["key"]!=key
                or doc["plan_sha"]!=group["plan_sha"]
                or doc["worker"]!=lease["worker"] or doc["token"]!=lease["token"]
                or doc["responses"]!=checks(folder/"raw")):
            raise ValueError("Catalog category result provenance mismatch")
        result=doc["result"]
        if result.get("catalog_complete") is True:
            reports=result["catalog_section_reports"]
            if (len(reports)!=1 or reports[0]["section_uuid"]!=group["sections"][int(key)]
                    or reports[0]["terminal_has_more"] is not False
                    or any(p["path"] not in doc["responses"] for p in result["catalog_responses"])):
                raise ValueError("Incomplete or mismatched native category")
        elif not result.get("error"):
            raise ValueError("Missing native category failure evidence")
        return result

    def _finish(self,sid,key,lease,artifact_id=None,expired=False):
        def edit(state):
            group=state["catalog_groups"][sid]
            old=group["completed"].get(key)
            if old:
                return False,old["token"]==lease["token"] and old["artifact_id"]==artifact_id
            row=group["leased"].get(key)
            if not row or row["token"]!=lease["token"]:
                return False,False
            if expired and row["expires"]>time.time():
                return False,False
            del group["leased"][key]
            if artifact_id is None:
                group["pending"].append(key)
            else:
                group["completed"][key]=row | {"artifact_id":artifact_id}
            validate_group(sid,group)
            return True,True
        return self._edit(sid,edit)

    def recover_expired(self,sid,group):
        for key,row in group["leased"].items():
            if row["expires"]>time.time():
                continue
            with tempfile.TemporaryDirectory() as temp:
                folder=Path(temp)
                saved=self.storage("recover",self._result_name(sid,key,row),folder)
                if saved is not None:
                    self._read_result(folder,sid,key,row,group)
                self._finish(sid,key,row,saved["id"] if saved else None,expired=True)

    def run_partition(self,claim):
        if getattr(self.storage,'exhausted',False):
            raise CheckpointStorageError('Physical job must roll over')
        sid,key,lease=claim
        group=self._group(sid)
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp);plan_dir=base/"plan";out=base/"result"
            plan_dir.mkdir();out.mkdir();raw=out/"raw";raw.mkdir()
            plan,data,_=self._load_plan(sid,plan_dir,group)
            session=self.session_factory()
            if not attach_delivery_location(session,plan["item"]):
                session.close()
                raise ValueError("Missing native delivery location")
            journal=PageJournal(session,self.storage,
                self._name("page",sid,"-"+key+"-"+group["plan_sha"][:16]),
                recover=lease["attempt"]>1,save_terminal=True)
            refresh=self._refresh(session,sid,raw,plan,key)
            try:
                result=capture_catalog(journal,sid,data,raw,plan["headers"],capture_response,
                    store_attempt=plan["store_attempt"],section_indices=[int(key)],refresh_native_metadata=refresh)
            except CheckpointStorageError:
                raise
            except (requests.RequestException,ValueError,RuntimeError,TypeError) as exc:
                result={"catalog_complete":False,"error":type(exc).__name__+": "+str(exc)}
            finally:
                session.close()
            doc={"identity":self._identity(sid),"key":key,"worker":lease["worker"],
                 "token":lease["token"],"plan_sha":group["plan_sha"],
                 "responses":checks(raw),"result":result}
            (out/"result.json").write_bytes(wire(doc))
            self._read_result(out,sid,key,lease,group)
            saved=self.storage("upload",self._result_name(sid,key,lease),out)
            self._finish(sid,key,lease,saved["id"])
            with self.metrics_lock:
                self.metrics["partitions"]+=1
                self.metrics["remote_partitions"]+=int(self.worker!=group["origin_worker"])
                self.metrics["saved_pages"]+=journal.saved
                self.metrics["resumed_pages"]+=journal.replayed
            print("FR_NATIVE_CATEGORY_TASK="+json.dumps({"store":sid,"section":key,
                "worker":self.worker,"origin_worker":group["origin_worker"],
                "saved_pages":journal.saved,"resumed_pages":journal.replayed}),flush=True)

    def finish_catalog(self,sid,raw):
        # Owner contributes work as well; never waits exclusively for helpers.
        while True:
            if getattr(self.storage,'exhausted',False):
                raise CheckpointStorageError('Physical job must roll over')
            group=self._group(sid)
            if group is None:
                raise RuntimeError("Catalog group disappeared")
            if not group["pending"] and not group["leased"]:
                break
            task=self.claim(sid) if group["pending"] else None
            if task:
                self.run_partition(task)
            else:
                self.recover_expired(sid,group)
                time.sleep(.5)
        reports=[];records=[];unique=set();seen=set();workers=set()
        errors=[]
        for key in sorted(group["completed"],key=int):
            row=group["completed"][key]
            with tempfile.TemporaryDirectory() as temp:
                folder=Path(temp)
                saved=self.storage("recover",self._result_name(sid,key,row),folder)
                if not saved or saved["id"]!=row["artifact_id"]:
                    raise RuntimeError("Missing durable native category result")
                result=self._read_result(folder,sid,key,row,group)
                copy_raw(folder/"raw",raw)
                workers.add(row["worker"])
                if result.get("catalog_complete") is not True:
                    errors.append(result["error"])
                    continue
                reports.extend(result["catalog_section_reports"])
                for page in result["catalog_responses"]:
                    if page["path"] in seen:
                        raise ValueError("Duplicate native HTTP page path")
                    seen.add(page["path"]);records.append(page)
                    unique.update(_catalog_skus([json.loads((raw/page["path"]).read_bytes())]))
        if errors:
            raise RuntimeError("; ".join(errors))
        if [p["section_uuid"] for p in reports]!=group["sections"]:
            raise ValueError("Native category coverage mismatch")
        return {"catalog_responses":records,"catalog_complete":True,
                "catalog_section_count":len(reports),"catalog_sections_finished":len(reports),
                "catalog_section_reports":reports,"catalog_unique_skus":len(unique),
                "catalog_reused_partitions":0,"catalog_independent_inventory_verified":False,
                "catalog_item_occurrences":sum(p["item_count"] for p in records),
                "catalog_worker_indices":sorted(workers)}
