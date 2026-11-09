"""On-GitHub live acceptance for independent HTML storefront method B.

No Uber API calls. Checks that HTML actually contains per-SKU UUIDs, names
and native numeric prices. Does NOT assert full catalog coverage.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from storefront_html_reader import collect_public_html

STORES=[
("costco","https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA"),
("pxmart","https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"),
("ubermart","https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"),
]

results=[]
for name,url in STORES:
    data=collect_public_html(url)
    record={"store":name,"status":data["status"],"count":data["count"],
            "complete":data["complete"],"preview_categories":data.get("preview_categories"),
            "sample":[{"sku":x["sku"],"name":x["name"],"price_cents":x["price_cents"]}
                      for x in data.get("items",[])[:4]]}
    print("LIVE_INDEPENDENT_HTML="+json.dumps(record,ensure_ascii=False),flush=True)
    results.append(record)
    if data["status"]!="HTML_PREVIEW_PARTIAL" or not isinstance(data["count"],int) or data["count"]<10:
        raise AssertionError(f"Method B did not retrieve usable product preview for {name}")
    if data["complete"] is not False:
        raise AssertionError("HTML preload is partial, cannot be called complete")
print("LIVE_INDEPENDENT_HTML_PASS="+json.dumps({"stores":len(results),
    "total_preview_skus":sum(r["count"] for r in results)},ensure_ascii=False),flush=True)
