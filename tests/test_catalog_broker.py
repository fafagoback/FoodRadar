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

if __name__=="__main__":unittest.main()
