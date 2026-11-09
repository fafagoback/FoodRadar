"""Contract: never publish a partial store menu, and keep raw catalog bodies immutable."""
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from catalog_complete import capture_catalog, merge_catalog_pages
from menu_core import convert_api_data_to_schema
from verified_prices import quote_from_native_item


def sample_item(uuid, title, price):
    return {"uuid":uuid,"title":title,"price":price,
            "catalogItemAnalyticsData":{"promoType":"MULTI_SKU_FLAT"},
            "itemLevelPromotion":{"multiSKUFlatPromotion":{"promotionUuid":"cart-only"}},
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

    def test_no_sections_does_not_erase_real_store_menu(self):
        data={"title":"Small cafe","catalogSectionsMap":{"a":[{"payload":{
            "standardItemsPayload":{"title":{"text":"drinks"},"catalogItems":[
                {"uuid":"x","title":"Coffee","price":6000}]}}}]}}
        with tempfile.TemporaryDirectory() as tmp:
            meta=capture_catalog(Session(),"uuid",data,Path(tmp),{},lambda a,b,c:None)
        self.assertTrue(meta["catalog_complete"])
        self.assertEqual(meta["catalog_responses"],[])
        self.assertEqual(merge_catalog_pages(data,[]),data)


if __name__ == "__main__":
    unittest.main()
