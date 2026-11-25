import json,sys,tempfile,shutil,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from catalog_broker import PageJournal, validate_group, CatalogBroker, wire, sha
from catalog_complete import capture_catalog
from test_catalog_complete import Response, sample_item


class MemoryArtifacts:
    def __init__(self,folder):
        self.root=Path(folder); self.ids={}
    def __call__(self,op,name,folder):
        folder=Path(folder);target=self.root/name
        if op=="upload":
            if name in self.ids:raise ValueError("Duplicate fixture artifact")
            shutil.copytree(folder,target)
            self.ids[name]=len(self.ids)+1
            return {"id":self.ids[name]}
        if name not in self.ids:return None
        shutil.copytree(target,folder,dirs_exist_ok=True)
        return {"id":self.ids[name]}


class PageTests(unittest.TestCase):
    def test_saved_pages_replay_exact_bytes_without_api_request(self):
        class S:
            def __init__(self):self.calls=0
            def post(self,url,json,headers,timeout):
                self.calls+=1
                return Response({"status":"success","data":{"catalog":[],
                    "pagingInfo":{"hasMore":True,"offset":60}}})
        with tempfile.TemporaryDirectory() as t:
            storage=MemoryArtifacts(t);s=S()
            from catalog_complete import CATALOG_API
            payload={"pagingInfo":{"offset":0},"storeFilters":{"storeUuid":"a"}}
            first=PageJournal(s,storage,"page")
            a=first.post(CATALOG_API,json=payload,headers={},timeout=1)
            self.assertEqual(first.saved,1)
            second=PageJournal(s,storage,"page",recover=True)
            b=second.post(CATALOG_API,json=payload,headers={},timeout=1)
            self.assertEqual(s.calls,1)
            self.assertEqual(second.replayed,1)
            self.assertEqual(a.content,b.content)

    def test_one_native_section_not_entire_catalog(self):
        class S:
            def __init__(self):self.calls=[]
            def post(self,url,json,headers,timeout):
                sid=json["storeFilters"]["sectionUuids"][0]
                self.calls.append(sid)
                return Response({"data":{"catalog":[{"payload":{"standardItemsPayload":{
                    "catalogItems":[sample_item(sid,sid,100)]}}}],"pagingInfo":{"hasMore":False}}})
        s=S()
        with tempfile.TemporaryDirectory() as t:
            out=capture_catalog(s,"store",{"aisles":{"x":[{"catalogSectionUUID":str(i)} for i in range(10)]}},
                Path(t),{},lambda a,b,c:None,section_indices=[7])
        self.assertEqual(s.calls,["7"])
        self.assertEqual(out["catalog_section_count"],1)
        self.assertEqual(out["catalog_section_reports"][0]["section_uuid"],"7")


class SmallStoreResumeTests(unittest.TestCase):
    def test_terminal_single_page_is_durable(self):
        from catalog_complete import CATALOG_API
        class S:
            def __init__(self):self.calls=0
            def post(self,url,json,headers,timeout):
                self.calls+=1
                return Response({"data":{"catalog":[],"pagingInfo":{"hasMore":False}}})
        with tempfile.TemporaryDirectory() as t:
            storage=MemoryArtifacts(t);session=S()
            payload={"pagingInfo":{"offset":0},"storeFilters":{"storeUuid":"a"}}
            first=PageJournal(session,storage,"terminal",save_terminal=True)
            a=first.post(CATALOG_API,json=payload,headers={},timeout=1)
            self.assertEqual(first.saved,1)
            next_worker=PageJournal(session,storage,"terminal",recover=True,save_terminal=True)
            b=next_worker.post(CATALOG_API,json=payload,headers={},timeout=1)
            self.assertEqual(a.content,b.content)
            self.assertEqual(session.calls,1)
            self.assertEqual(next_worker.replayed,1)

    def test_one_small_store_replays_saved_page_on_another_worker(self):
        from catalog_complete import CATALOG_API
        class Crash(BaseException): pass
        sid="00000000-0000-4000-8000-000000000001"
        item={"store_uuid":sid,"store_url":"https://example.invalid/store/"+sid}
        base={"title":"small","aisles":{"x":[{"catalogSectionUUID":"section"}]}}
        calls=[]
        class Session:
            def __init__(self,worker):self.worker=worker
            def close(self):pass
            def post(self,url,json,headers,timeout):
                assert url==CATALOG_API
                offset=json["pagingInfo"]["offset"]
                calls.append((self.worker,offset))
                if self.worker==0 and offset==60:raise Crash()
                return Response({"data":{"catalog":[{"payload":{"standardItemsPayload":{
                    "catalogItems":[sample_item(str(offset),"Fixture",100)]}}}],
                    "pagingInfo":{"hasMore":offset==0,"offset":60 if offset==0 else 120}}})
        state={"run_id":"123","source_sha":"sha","epoch":"epoch"}
        with tempfile.TemporaryDirectory() as t,patch("catalog_broker.attach_delivery_location",return_value=True):
            root=Path(t);storage=MemoryArtifacts(root/"artifacts")
            a=CatalogBroker(None,state,{sid:item},"a",0,storage,lambda:Session(0))
            raw=root/"a";raw.mkdir();(raw/"base.body").write_text(json.dumps({"data":base}))
            with self.assertRaises(Crash):
                a.dispatch(item,raw,"base.body",base,{},1)
            b=CatalogBroker(None,state,{sid:item},"b",1,storage,lambda:Session(1))
            target=root/"b";target.mkdir()
            result=b._resume_small(sid,target)
            self.assertTrue(result["success"])
            self.assertEqual(len(result["catalog_responses"]),2)
            self.assertEqual(calls,[(0,0),(0,60),(1,60)])
            self.assertEqual(b.metrics["resumed_pages"],1)
            self.assertEqual((target/"base.body").read_bytes(),(raw/"base.body").read_bytes())

if __name__=="__main__":unittest.main()
