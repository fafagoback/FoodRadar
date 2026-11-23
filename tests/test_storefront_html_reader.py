"""Validation of independent public storefront HTML preview (not the API)."""
import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from storefront_html_reader import decode_embedded_json, extract_preview_html

STORE="d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42"

def page(items):
    payload={"queries":[{"queryKey":["getStoreV1",{"storeUuid":STORE}],
         "state":{"data":{"uuid":STORE,"catalogSectionsMap":{STORE:[
            {"payload":{"standardItemsPayload":{"catalogItems":items}}}]}}}}]}
    # Uber encodes JSON delimiter double quotes to unicode escapes in script.
    encoded=json.dumps(payload,ensure_ascii=False).replace('"',r"\u0022")
    return f'<html><script id="__REACT_QUERY_STATE__" type="application/json">{encoded}</script></html>'


class SafariStorefrontHtmlTests(unittest.TestCase):
    def test_real_products_and_cents_are_extracted_without_api(self):
        html=page([
          {"uuid":"sku1","title":"草莓冰淇淋","price":25900,"isAvailable":True},
          {"uuid":"sku2","title":"牛奶 936ml","price":9900,"isSoldOut":False},
          {"uuid":"sku1","title":"草莓冰淇淋","price":25900},
        ])
        data=extract_preview_html(html,http_status=200,final_url="https://www.ubereats.com/tw/store/test/uuid")
        self.assertEqual(data["count"],2)
        self.assertEqual(data["items"][0]["price_cents"],25900)
        self.assertEqual(data["items"][0]["sku"],"sku1")
        self.assertEqual(data["status"],"HTML_PREVIEW_PARTIAL")
        self.assertFalse(data["complete"])

    def test_missing_html_is_unknown_not_zero_products(self):
        result=extract_preview_html("<html>Waiting</html>",http_status=403)
        self.assertIsNone(result["count"])
        self.assertFalse(result["complete"])

    def test_challenge_redirect_is_not_product_page(self):
        result=extract_preview_html("<html>Challenge</html>",http_status=200,
             final_url="https://def.uber.com/en/challenge")
        self.assertEqual(result["status"],"WEB_HTML_BLOCKED")

    def test_no_priced_products_never_marks_full_success(self):
        result=extract_preview_html(page([]),http_status=200)
        self.assertIsNone(result["count"])
        self.assertFalse(result["complete"])

    def test_escaped_percent_encoded_json_inner_quote(self):
        text=r'{\u0022metaJson\u0022:\u0022{%5C\u0022@type%5C\u0022:%5C\u0022FAQPage%5C\u0022}\u0022}'
        value=decode_embedded_json(text)
        self.assertIn("FAQPage",value["metaJson"])


if __name__=="__main__":
    unittest.main()
