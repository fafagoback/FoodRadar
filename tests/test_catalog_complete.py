"""Contract: never publish a partial store menu, and keep raw catalog bodies immutable."""
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from catalog_complete import capture_catalog, merge_catalog_pages, is_recoverable_catalog_failure
from menu_core import convert_api_data_to_schema
from verified_prices import quote_from_native_item


def sample_item(uuid, title, price):
    return {"uuid":uuid,"title":title,"price":price,
            "catalogItemAnalyticsData":{"promoType":"MULTI_SKU_FLAT"},
            "itemLevelPromotion":{"type":"multiSKUFlatPromotion","multiSKUFlatPromotion":{"promotionUuid":"cart-only"}},
            "itemThumbnailElements":[{"payload":{"labelPayload":{"label":{
                "richTextElements":[{"text":{"text":{"text":"NT$259"},
                    "predefinedDecorations":None}},
                    {"text":{"text":{"text":"NT$309"},
                    "predefinedDecorations":["STRIKE_THROUGH"]}}]}}}}]}


class Response:
    status_code = 200
    def __init__(self, obj):
        self.obj = obj
        self.content = json.dumps(obj, ensure_ascii=False).encode()
    def json(self):
        return self.obj


class Session:
    def __init__(self):
        self.offsets = []
    def post(self, url, json, headers, timeout):
        offset = json["pagingInfo"]["offset"]
        self.offsets.append(offset)
        if offset == 0:
            item = sample_item("first", "Häagen-Dazs 草莓冰淇淋", 25900)
        elif offset == 60:
            item = sample_item("second", "Häagen-Dazs 咖啡冰淇淋", 25900)
        else:
            raise ValueError("Unexpected page")
        return Response({"status":"success","data":{
            "catalog":[{"payload":{"standardItemsPayload":{
                "title":{"text":"哈根達斯"},"catalogItems":[item]}}}],
            "pagingInfo":{"hasMore":offset==0,"offset":60 if offset==0 else 120}}})


class CatalogCompleteness(unittest.TestCase):
    def test_partial_raw_only_allows_deterministic_catalog_gaps(self):
        for error in (
            "RuntimeError: native code 500",
            "Subsections for abc omitted 2 previously seen native parent SKUs",
            "Catalog abc reached deep-offset boundary; native subsection UUIDs unavailable, refusing partial menu",
        ):
            self.assertTrue(is_recoverable_catalog_failure(error), error)
        for error in (
            "HTTP 403 forbidden", "getStoreV1 timed out", "HTTP 429",
            "Missing delivery coordinate", "Catalog native subsection cycle",
            "invalid JSON", None, "",
        ):
            self.assertFalse(is_recoverable_catalog_failure(error), error)

    def test_collect_two_pages_and_merge(self):
        sid = "d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42"
        data = {"title":"北投店","catalogSectionsMap":{}, "aisles":{sid:[
            {"catalogSectionUUID":"brand-category"}]}}
        session = Session()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def capture(directory,name,response):
                out=directory/name
                out.parent.mkdir(parents=True,exist_ok=True)
                out.write_bytes(response.content)
            metadata = capture_catalog(session,sid,data,root,{},capture)
            self.assertTrue(metadata["catalog_complete"])
            self.assertEqual(session.offsets,[0,60])
            self.assertEqual(metadata["catalog_item_occurrences"],2)
            raw=[json.loads((root/p["path"]).read_text()) for p in metadata["catalog_responses"]]
            doc=convert_api_data_to_schema(merge_catalog_pages(data,raw),"https://example.com/store")
            names=[p["name"] for s in doc["hasMenu"]["hasMenuSection"] for p in s["hasMenuItem"]]
            self.assertEqual(len(names),2)
            self.assertTrue(all("Häagen-Dazs" in n for n in names))
            quotes=[p["verified_price"] for s in doc["hasMenu"]["hasMenuSection"] for p in s["hasMenuItem"]]
            self.assertTrue(all(q["status"]=="VERIFIED_MENU" for q in quotes))
            self.assertTrue(all(q["kind"]=="DIRECT_DISCOUNT" for q in quotes))
            self.assertEqual(quotes[0]["original_total_cents"],30900)
            self.assertEqual(quotes[0]["payable_total_cents"],25900)

    def test_no_pagination_metadata_rejected(self):
        class BadSession(Session):
            def post(self,url,json,headers,timeout):
                return Response({"data":{"catalog":[]}})
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError):
                capture_catalog(BadSession(),"uuid",{"aisles":{"uuid":[{"catalogSectionUUID":"s"}]}},
                                Path(tmp),{},lambda root,path,resp:None)

    def test_outer_store_retries_preserve_separate_immutable_page_files(self):
        sid = "d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42"
        initial = {"title":"北投店", "catalogSectionsMap":{}, "aisles": {
            sid:[{"catalogSectionUUID":"brand-category"}]}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def save(directory, name, response):
                target = directory / name
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise FileExistsError(name)
                target.write_bytes(response.content)
            first = capture_catalog(Session(), sid, initial, root, {}, save, store_attempt=1)
            second = capture_catalog(Session(), sid, initial, root, {}, save, store_attempt=2)
            first_paths = {x["path"] for x in first["catalog_responses"]}
            second_paths = {x["path"] for x in second["catalog_responses"]}
            self.assertTrue(first_paths.isdisjoint(second_paths))
            self.assertEqual(len(list(root.rglob("*.body"))), 4)

    def test_uber_null_terminal_page_is_valid_and_preserved_raw(self):
        # Real HF batch 20261109105106: three stores returned HTTP 200,
        # data.catalog=null, pagingInfo={"hasMore":false} on final page.
        sid = "a0b09a3a-4271-5a38-a5eb-fa53a160c51c"
        data = {"title":"test","aisles":{"0":[{"catalogSectionUUID":"section"}]}}
        class FinalPageSession(Session):
            def post(self, url, json, headers, timeout):
                offset=json["pagingInfo"]["offset"]
                self.offsets.append(offset)
                if offset == 0:
                    return Response({"data":{"catalog":[{
                        "payload":{"standardItemsPayload":{"catalogItems":[sample_item(
                            "kept", "Häagen-Dazs 草莓冰淇淋", 25900)]}}}],
                        "pagingInfo":{"hasMore":True,"offset":60}}})
                return Response({"data":{"catalog":None,
                    "pagingInfo":{"hasMore":False}}})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            session=FinalPageSession()
            def save(directory,name,response):
                path=directory/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(response.content)
            out=capture_catalog(session,sid,data,root,{},save)
            self.assertEqual(session.offsets,[0,60])
            self.assertEqual(len(out["catalog_responses"]),2)
            self.assertEqual(out["catalog_item_occurrences"],1)
            self.assertTrue(out["catalog_complete"])
            native=[json.loads((root/x["path"]).read_bytes()) for x in out["catalog_responses"]]
            doc=merge_catalog_pages({"catalogSectionsMap":{}},native)
            self.assertEqual(len(doc["catalogSectionsMap"]["v2_catalog"]),1)
            self.assertIsNone(native[1]["data"]["catalog"])

    def test_uber_null_intermediate_page_advances_offset(self):
        # Real HF batch 20261109105106: one store returned catalog=null
        # while pagingInfo.hasMore=true and offset advanced to 420.
        sid="f552a70c-1d18-5b45-b037-4d9d592d9f06"
        class GapSession(Session):
            def post(self,url,json,headers,timeout):
                off=json["pagingInfo"]["offset"]
                self.offsets.append(off)
                if off==0:
                    return Response({"data":{"catalog":[{
                        "payload":{"standardItemsPayload":{"catalogItems":[sample_item(
                            "one","test item",2500)]}}}],
                        "pagingInfo":{"hasMore":True,"offset":60}}})
                if off==60:
                    return Response({"data":{"catalog":None,
                        "pagingInfo":{"hasMore":True,"offset":120}}})
                return Response({"data":{"catalog":None,
                    "pagingInfo":{"hasMore":False}}})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def save(directory,name,response):
                path=directory/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(response.content)
            session=GapSession()
            result=capture_catalog(session,sid,
                {"aisles":{"0":[{"catalogSectionUUID":"section"}]}},
                root,{},save)
            self.assertEqual(session.offsets,[0,60,120])
            self.assertEqual([p["item_count"] for p in result["catalog_responses"]],[1,0,0])
            self.assertTrue(result["catalog_complete"])

    def test_invalid_null_paging_still_fails_closed(self):
        class BadNullSession(Session):
            def post(self,url,json,headers,timeout):
                return Response({"data":{"catalog":None,
                    "pagingInfo":{"hasMore":True,"offset":0}}})
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                capture_catalog(BadNullSession(),"uuid",
                    {"aisles":{"0":[{"catalogSectionUUID":"section"}]}},
                    Path(tmp),{},lambda a,b,c:None)

    def test_no_sections_does_not_erase_real_store_menu(self):
        data={"title":"Small cafe","catalogSectionsMap":{"a":[{"payload":{
            "standardItemsPayload":{"title":{"text":"drinks"},"catalogItems":[
                {"uuid":"x","title":"Coffee","price":6000}]}}}]}}
        with tempfile.TemporaryDirectory() as tmp:
            meta=capture_catalog(Session(),"uuid",data,Path(tmp),{},lambda a,b,c:None)
        self.assertTrue(meta["catalog_complete"])
        self.assertEqual(meta["catalog_responses"],[])
        self.assertEqual(merge_catalog_pages(data,[]),data)


    def test_distinct_catalog_categories_each_start_at_zero(self):
        """Grouped category requests may truncate; each category is independent."""
        sid="a4e2201d-5fc7-5a10-a41a-dfe58975a7a7"
        initial={"catalogSectionsMap":{},"aisles":{"cat":[
            {"catalogSectionUUID":"fresh"},
            {"catalogSectionUUID":"snacks"}]}}
        class CategorySession:
            def __init__(self):
                self.calls=[]
            def post(self,url,json,headers,timeout):
                cat=json["storeFilters"]["sectionUuids"]
                off=json["pagingInfo"]["offset"]
                self.calls.append((cat[0],off))
                self_item=sample_item(cat[0],cat[0]+"-product",20000)
                return Response({"data":{"catalog":[{
                    "payload":{"standardItemsPayload":{"catalogItems":[self_item]}}}],
                    "pagingInfo":{"hasMore":False}}})
        session=CategorySession()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def save(directory,name,response):
                out=directory/name
                out.parent.mkdir(parents=True,exist_ok=True)
                out.write_bytes(response.content)
            meta=capture_catalog(session,sid,initial,root,{},save)
            self.assertEqual(session.calls,[("fresh",0),("snacks",0)])
            self.assertEqual(meta["catalog_sections_finished"],2)
            self.assertEqual(len(meta["catalog_responses"]),2)
            self.assertEqual(len({x["path"] for x in meta["catalog_responses"]}),2)
            pages=[json.loads((root/x["path"]).read_bytes())
                   for x in meta["catalog_responses"]]
            merged=merge_catalog_pages(initial,pages)
            names=[x["title"]
                   for group in merged["catalogSectionsMap"]["v2_catalog"]
                   for x in group["payload"]["standardItemsPayload"]["catalogItems"]]
            self.assertEqual(set(names),{"fresh-product","snacks-product"})

    def test_failing_later_category_does_not_report_complete(self):
        sid="a4e2201d-5fc7-5a10-a41a-dfe58975a7a7"
        base={"catalogSectionsMap":{},"aisles":{"cat":[
            {"catalogSectionUUID":"fresh"},{"catalogSectionUUID":"snacks"}]}}
        class MixedSession:
            def post(self,url,json,headers,timeout):
                cat=json["storeFilters"]["sectionUuids"][0]
                if cat=="snacks":
                    return Response({"data":{"catalog":[]}})
                return Response({"data":{"catalog":[{
                    "payload":{"standardItemsPayload":{
                        "catalogItems":[sample_item("fresh-1","Fresh",10000)]}}}],
                    "pagingInfo":{"hasMore":False}}})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with self.assertRaises(RuntimeError):
                capture_catalog(MixedSession(),sid,base,root,{},lambda a,b,c:None)


    def test_deep_offset_failure_recovers_native_subsections_only(self):
        base={"aisles":{"store":[{"catalogSectionUUID":"parent"}]},
              "sections":[{"uuid":"parent","subsectionUuids":["child-a","child-b"]}],
              "catalogSectionsMap":{}}
        class FakeSession:
            def __init__(self):
                self.calls=[]
            def post(self,url,json,headers,timeout):
                sf=json["storeFilters"]
                sub=(sf["subsectionUuids"] or [None])[0]
                off=json["pagingInfo"]["offset"]
                self.calls.append((sub,off))
                if sub is None and off==1020:
                    return Response({"status":"failure","data":{"code":500,
                        "message":"status code error"}})
                if sub is None:
                    return Response({"status":"success","data":{"catalog":[{
                        "payload":{"standardItemsPayload":{"catalogItems":[
                            sample_item("old","Seen in parent",10000)]}}}],
                        "pagingInfo":{"hasMore":True,"offset":1020}}})
                items = ([sample_item("old","Seen in parent",10000)] if sub=="child-a"
                         else [sample_item("new","Only in child",20000)])
                return Response({"status":"success","data":{"catalog":[{
                    "payload":{"standardItemsPayload":{"catalogItems":items}}}],
                    "pagingInfo":{"hasMore":False}}})
        session=FakeSession()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def save(directory,name,response):
                path=directory/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(response.content)
            out=capture_catalog(session,"store",base,root,{},save)
            self.assertTrue(out["catalog_complete"])
            self.assertEqual(out["catalog_section_reports"][0]["mode"],
                             "native_children_before_deep_offset")
            self.assertEqual([x["subsection_uuid"]
                              for x in out["catalog_responses"]],
                             ["child-a","child-b"])
            self.assertEqual(session.calls,[
                (None,0),("child-a",0),("child-b",0)])
            self.assertEqual(len(list(root.rglob("*.body"))),3)
            pages=[json.loads((root/x["path"]).read_bytes())
                   for x in out["catalog_responses"]]
            self.assertEqual(_item_ids(merge_catalog_pages(base,pages)),
                             {"old","new"})

    def test_subsection_only_filter_recovers_native_parent_skus(self):
        """Real 2026-11-11 API: parent+child may return an empty catalog.

        Child-only (sectionUuids=null) is the verified productive request.
        Never mark an empty combined-filter response as a complete menu.
        """
        base = {
            "aisles": {"store": [{"catalogSectionUUID": "parent"}]},
            "sections": [{"uuid": "parent", "subsectionUuids": ["child-a"]}],
            "catalogSectionsMap": {},
        }
        class FilterSensitiveSession:
            def __init__(self):
                self.filters = []
            def post(self, url, json, headers, timeout):
                filt = json["storeFilters"]
                self.filters.append((filt["sectionUuids"], filt["subsectionUuids"],
                                     json["pagingInfo"]["offset"]))
                if filt["subsectionUuids"]:
                    items = ([sample_item("already-seen", "Retained", 9000),
                              sample_item("new-native", "Recovered", 11000)]
                             if filt["sectionUuids"] is None else [])
                    return Response({"status": "success", "data": {
                        "catalog": [{"payload": {"standardItemsPayload": {
                            "catalogItems": items}}}],
                        "pagingInfo": {"hasMore": False}}})
                if json["pagingInfo"]["offset"] >= 1020:
                    return Response({"status": "failure",
                                     "data": {"code": 500, "message": "status code error"}})
                return Response({"status": "success", "data": {
                    "catalog": [{"payload": {"standardItemsPayload": {
                        "catalogItems": [sample_item("already-seen", "Seen", 9000)]}}}],
                    "pagingInfo": {"hasMore": True, "offset": 1020}}})
        session = FilterSensitiveSession()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def save(directory, name, response):
                p = directory / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(response.content)
            out = capture_catalog(session, "store", base, root, {}, save)
            self.assertTrue(out["catalog_complete"])
            self.assertEqual(out["catalog_item_occurrences"], 2)
            self.assertIn((None, ["child-a"], 0), session.filters)
            self.assertNotIn((["parent"], ["child-a"], 0), session.filters)

    def test_deep_offset_fails_closed_without_subcategories(self):
        base={"aisles":{"store":[{"catalogSectionUUID":"parent"}]}}
        class FakeSession:
            def post(self,url,json,headers,timeout):
                off=json["pagingInfo"]["offset"]
                if off:
                    return Response({"status":"failure","data":{
                        "code":500,"message":"status code error"}})
                return Response({"status":"success","data":{"catalog":[{
                    "payload":{"standardItemsPayload":{
                        "catalogItems":[sample_item("old","Seen",10000)]}}}],
                    "pagingInfo":{"hasMore":True,"offset":1020}}})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def save(directory,name,response):
                path=directory/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(response.content)
            with self.assertRaisesRegex(RuntimeError,"native subsection UUIDs unavailable"):
                capture_catalog(FakeSession(),"store",base,root,{},save)



    def test_string_500_deep_offset_uses_native_subsections(self):
        """Regression: 2026-11-12 run #397 failed at offset 1020 and never fell back."""
        base = {"aisles": {"s": [{"catalogSectionUUID": "parent"}]},
                "sections": [{"uuid": "parent", "subsectionUuids": ["child"]}]}
        class Session:
            def __init__(self):
                self.calls = []
            def post(self, url, json, headers, timeout):
                child = (json["storeFilters"]["subsectionUuids"] or [None])[0]
                offset = json["pagingInfo"]["offset"]
                self.calls.append((child, offset))
                if child is None and offset >= 1020:
                    return Response({"status": "failure",
                                     "data": {"code": "500", "message": "internal error"}})
                return Response({"status": "success", "data": {
                    "catalog": [{"payload": {"standardItemsPayload": {"catalogItems": [
                        sample_item("sku", "Product", 9900)]}}}],
                    "pagingInfo": {"hasMore": child is None,
                                   "offset": 1020 if child is None else 0}}})
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def save(directory, name, response):
                file = directory / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_bytes(response.content)
            session = Session()
            meta = capture_catalog(session, "store", base, root, {}, save)
            self.assertTrue(meta["catalog_complete"])
            self.assertEqual(meta["catalog_section_reports"][0]["mode"],
                             "native_children_before_deep_offset")
            self.assertEqual(session.calls, [(None, 0), ("child", 0)])



    def test_missing_child_skus_must_fail_closed_without_offset_1020(self):
        base = {"aisles": {"s": [{"catalogSectionUUID": "parent"}]},
                "sections": [{"uuid": "parent", "subsectionUuids": ["child"]}]}
        class Session:
            def __init__(self):
                self.calls = []
            def post(self, url, json, headers, timeout):
                child = (json["storeFilters"]["subsectionUuids"] or [None])[0]
                offset = json["pagingInfo"]["offset"]
                self.calls.append((child, offset))
                if offset >= 1000:
                    raise AssertionError("Should never request offset >= 1000")
                item = sample_item("parent-only" if child is None else "child-only",
                                   "Verified item", 1000)
                return Response({"data": {"catalog": [{
                    "payload": {"standardItemsPayload": {"catalogItems": [item]}}}],
                    "pagingInfo": {"hasMore": child is None,
                                   "offset": 1020 if child is None else 0}}})
        session = Session()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "omitted 1"):
                capture_catalog(session, "store", base, Path(tmp), {},
                                lambda root, path, resp: None)
        self.assertEqual(session.calls, [(None, 0), ("child", 0)])

    def test_nested_native_children_avoid_deep_offsets(self):
        base = {"aisles": {"s": [{"catalogSectionUUID": "parent"}]},
                "sections": [{"uuid": "parent", "subsectionUuids": ["child"]},
                             {"uuid": "child", "subsectionUuids": ["grandchild"]}]}
        class Session:
            def __init__(self):
                self.calls = []
            def post(self, url, json, headers, timeout):
                part = (json["storeFilters"]["subsectionUuids"] or ["parent"])[0]
                offset = json["pagingInfo"]["offset"]
                self.calls.append((part, offset))
                if offset >= 1000:
                    raise AssertionError("Should never request >= 1000")
                return Response({"data": {"catalog": [{
                    "payload": {"standardItemsPayload": {"catalogItems": [
                        sample_item("common", "Common", 1000)]}}}],
                    "pagingInfo": {"hasMore": part != "grandchild",
                                   "offset": 1020 if part != "grandchild" else 0}}})
        with tempfile.TemporaryDirectory() as tmp:
            session = Session()
            out = capture_catalog(session, "store", base, Path(tmp), {},
                                  lambda root, path, resp: None)
        self.assertTrue(out["catalog_complete"])
        self.assertEqual(session.calls,
                         [("parent", 0), ("child", 0), ("grandchild", 0)])


    def test_native_item_subsections_when_store_sections_omit_parent(self):
        """Real Jinji #431347: aisles has section, getStoreV1.sections omits it."""
        parent = "parent"
        child = "native-child"
        base = {"aisles": {"store": [{"catalogSectionUUID": parent}]},
                "sections": [], "catalogSectionsMap": {}}
        class ItemSourcedSession:
            def __init__(self):
                self.calls = []
            def post(self, url, json, headers, timeout):
                section = (json["storeFilters"]["sectionUuids"] or [None])[0]
                subsection = (json["storeFilters"]["subsectionUuids"] or [None])[0]
                offset = json["pagingInfo"]["offset"]
                self.calls.append((section, subsection, offset))
                if offset >= 1000:
                    raise AssertionError("Forbidden deep offset request")
                if subsection == child:
                    ids, more = [("seen", child), ("new", child)], False
                else:
                    ids, more = [("seen", child)], True
                entries = [{
                    "catalogSectionUUID": child,
                    "payload": {"standardItemsPayload": {"catalogItems": [
                        {**sample_item(sid, sid, 10000), "subsectionUuid": sub}
                        for sid, sub in ids]}}}]
                return Response({"data": {
                    "catalog": entries,
                    "pagingInfo": {"hasMore": more, "offset": 1020 if more else 0}}})
        session = ItemSourcedSession()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def save(directory, name, response):
                file = directory / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_bytes(response.content)
            meta = capture_catalog(session, "store", base, root, {}, save)
            self.assertTrue(meta["catalog_complete"])
            self.assertEqual(meta["catalog_section_reports"][0]["mode"],
                             "native_children_before_deep_offset")
            self.assertEqual(session.calls,
                             [("parent", None, 0), (None, child, 0)])
            raw = [json.loads((root/p["path"]).read_bytes())
                   for p in meta["catalog_responses"]]
            self.assertEqual(_item_ids(merge_catalog_pages(base, raw)),
                             {"seen", "new"})

    def test_item_sourced_children_missing_parent_skus_still_rejected(self):
        """A real native child UUID does not justify hiding missing parent SKUs."""
        parent = "parent"
        child = "native-child"
        base = {"aisles": {"store": [{"catalogSectionUUID": parent}]},
                "sections": []}
        class BadPartition:
            def post(self, url, json, headers, timeout):
                subsection = (json["storeFilters"]["subsectionUuids"] or [None])[0]
                if subsection == child:
                    ids, more = ["other"], False
                else:
                    ids, more = ["parent-only"], True
                items = [{**sample_item(sid, sid, 10000),
                          "subsectionUuid": child} for sid in ids]
                return Response({"data": {
                    "catalog": [{
                        "catalogSectionUUID": child,
                        "payload": {"standardItemsPayload": {"catalogItems": items}}}],
                    "pagingInfo": {"hasMore": more, "offset": 1020 if more else 0}}})
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "omitted 1"):
                capture_catalog(BadPartition(), "store", base, Path(tmp), {},
                                lambda directory, name, response: None)


    def test_incomplete_store_child_list_recovers_extra_real_item_subsection(self):
        """A truncated getStoreV1 child list must not discard legitimate SKU groups."""
        base = {"aisles": {"store": [{"catalogSectionUUID": "parent"}]},
                "sections": [{"uuid": "parent", "subsectionUuids": ["child-a"]}],
                "catalogSectionsMap": {}}
        class SessionWithOmittedChild:
            def __init__(self):
                self.calls = []
            def post(self, url, json, headers, timeout):
                child = (json["storeFilters"]["subsectionUuids"] or [None])[0]
                off = json["pagingInfo"]["offset"]
                self.calls.append((child,off))
                if off >= 1000:
                    raise AssertionError("No deep page may be requested")
                if child == "child-a":
                    values, more = [("a", "child-a")], False
                elif child == "child-b":
                    values, more = [("b", "child-b")], False
                else:
                    values, more = [("a", "child-a"), ("b", "child-b")], True
                groups = [{"catalogSectionUUID": subsection, "payload": {
                    "standardItemsPayload": {"catalogItems": [
                        {**sample_item(sku, sku, 10000),
                         "subsectionUuid": subsection}]}}}
                    for sku, subsection in values]
                return Response({"status": "success", "data": {
                    "catalog": groups,
                    "pagingInfo": {"hasMore": more, "offset": 1020 if more else 0}}})
        session = SessionWithOmittedChild()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def save(directory, filename, response):
                target = directory / filename
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(response.content)
            result = capture_catalog(session, "store", base, root, {}, save)
            self.assertTrue(result["catalog_complete"])
            self.assertEqual(session.calls,
                             [(None, 0), ("child-a", 0), ("child-b", 0)])
            self.assertEqual(result["catalog_section_reports"][0]["subsections"], 2)
            raw = [json.loads((root / x["path"]).read_bytes())
                   for x in result["catalog_responses"]]
            self.assertEqual(_item_ids(merge_catalog_pages(base, raw)), {"a", "b"})


def _item_ids(native):
    ids=set()
    for group in native.get("catalogSectionsMap",{}).values():
        for section in group:
            for item in (((section.get("payload") or {})
                          .get("standardItemsPayload") or {}).get("catalogItems") or []):
                ids.add(item["uuid"])
    return ids


if __name__ == "__main__":
    unittest.main()
