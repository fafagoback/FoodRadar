import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from native_partition_merge import merge_partition_tree
from crawl_queue import validate_batch_group
from catalog_complete import capture_catalog

def page(skus,more):
    return {"data":{"catalog":[{"payload":{"standardItemsPayload":{"catalogItems":[{"uuid":s} for s in skus]}}}],"pagingInfo":{"hasMore":more,"offset":1000 if more else 0}}}

class NativeTreeTests(unittest.TestCase):
    def test_capture_defers_children_without_crawling_them(self):
        from unittest.mock import patch
        native={"aisles":{"a":[{"catalogSectionUUID":"r","subsectionUuids":["a","b"]}]}}
        calls=[]
        class Response:
            status_code=200
            headers={}
            def json(self):
                return page(["sku"],True)
        class Session:
            def post(self,url,json,**kwargs):
                calls.append(json)
                return Response()
        children=[]
        with tempfile.TemporaryDirectory() as directory, patch("catalog_complete.global_limiter.wait"), patch("catalog_complete.global_limiter.record_success"):
            result=capture_catalog(Session(),"store",native,Path(directory),{},
                                   lambda *args:None,section_indices=[0],
                                   defer_children=lambda c:children.extend(c))
        self.assertEqual(children,["a","b"])
        self.assertEqual(len(calls),1)
        self.assertFalse(result["catalog_complete"])
        self.assertEqual(result["catalog_deferred_children"]["children"],children)

    def test_unexpected_large_store_promotes_only_remaining_roots(self):
        from unittest.mock import patch
        native={"aisles":{"a":[{"catalogSectionUUID":"small"},
                              {"catalogSectionUUID":"r","subsectionUuids":["a","b"]},
                              {"catalogSectionUUID":"later"}]}}
        class Response:
            status_code=200
            headers={}
            def __init__(self,large):self.large=large
            def json(self):return page(["sku"],self.large)
        calls=[]
        class Session:
            def post(self,url,json,**kwargs):
                root=json["storeFilters"]["sectionUuids"][0]
                calls.append(root)
                return Response(root=="r")
        pending=[]
        with tempfile.TemporaryDirectory() as directory, patch("catalog_complete.global_limiter.wait"), patch("catalog_complete.global_limiter.record_success"):
            result=capture_catalog(Session(),"store",native,Path(directory),{},
                    lambda *args:None,promote_store=lambda r:pending.extend(r))
        self.assertEqual(calls,["small","r"])
        self.assertEqual(pending,["r","later"])
        self.assertEqual(result["catalog_local_reports"][0]["section_uuid"],"small")
        self.assertTrue(result["catalog_deferred"])
        self.assertFalse(result["catalog_complete"])

    def test_nested_merge_preserves_parent_coverage(self):
        with tempfile.TemporaryDirectory() as t:
            raw=Path(t)
            group={"sections":["root"],"partitions":{"0.0":{"section_uuid":"root","sub_path":["child"]},
                   "0.1":{"section_uuid":"root","sub_path":["other"]},
                   "0.0.0":{"section_uuid":"root","sub_path":["child","leaf"]}}}
            docs={}
            def add(k,sub,skus,children=None):
                f=k+".body"; (raw/f).write_text(json.dumps(page(skus,children is not None)))
                record={"path":f,"section_uuid":"root","subsection_uuid":sub,"item_count":len(skus)}
                sec={"success":True,"catalog_complete":children is None,"catalog_responses":[] if children else [record]}
                if children is not None:
                    sec["catalog_deferred_children"]={"children":children,"preview_responses":[record]}
                else:
                    sec["catalog_section_reports"]=[{"section_uuid":"root","terminal_has_more":False}]
                docs[k]=(sec,{f:"hash"},raw,k+"/")
            add("0",None,["a","b"],["child","other"])
            add("0.0","child",["a"],["leaf"])
            add("0.0.0","leaf",["a","c"])
            add("0.1","other",["b"])
            pages,report=merge_partition_tree(group,"0",docs)
            self.assertEqual(report["unique_skus"],3)
            self.assertEqual(len(pages),2)
            (raw/"0.1.body").write_text(json.dumps(page(["z"],False)))
            with self.assertRaisesRegex(RuntimeError,"previously seen native parent SKUs"):
                merge_partition_tree(group,"0",docs)

    def test_nested_roster_validation(self):
        group={"format":"batch-section-v2","sections":["r"],"partitions":{
            "0.0":{"section_uuid":"r","sub_path":["c"]},
            "0.0.0":{"section_uuid":"r","sub_path":["c","g"]}},
            "pending":["0","0.0","0.0.0"],"leased":{},"local_done":{},"completed":{}}
        validate_batch_group("s",group)
        group["partitions"]["0.0.0"]["sub_path"]=["wrong","g"]
        with self.assertRaises(ValueError):
            validate_batch_group("s",group)
