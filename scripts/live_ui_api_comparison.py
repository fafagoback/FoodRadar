"""Real same-run comparison between API (A) and rendered WebKit UI (B).

Independent inputs: A requests Uber API; B observes visible webpage card
text/UUIDs, never introspects APIs or hydration state. Does not modify DB.
"""
from __future__ import annotations
import json
import os
import tempfile
from pathlib import Path
from live_webkit_dom import capture_store
from daily_triple_audit import STORES, collect_api

report=[]
with tempfile.TemporaryDirectory(prefix="foodradar-compare-") as td:
    for slug,label,url,lat,lon in STORES:
        folder=Path(td)/slug
        folder.mkdir(parents=True,exist_ok=True)
        record={"store":label}
        try:
            a=collect_api(label,url,lat,lon,folder)
            record["api_count"]=a["count"]
            aa={x["sku"]:x for x in a["items"] if x.get("sku")}
        except Exception as e:
            record["api_error"]=type(e).__name__+":"+str(e)[:160]
            aa={}
        try:
            b=capture_store(url,limit_categories=int(os.environ.get("DOM_CATEGORY_TEST_LIMIT","2")),
                           scroll_steps=int(os.environ.get("DOM_SCROLL_TEST_LIMIT","4")))
            record["ui_count"]=b["count"]
            record["ui_categories_visited"]=b.get("categories_visited")
            record["ui_categories_total"]=b.get("categories_listed")
            record["ui_errors"]=b.get("errors")
            bb={x["sku"]:x for x in b["items"] if x.get("sku")}
        except Exception as e:
            record["ui_error"]=type(e).__name__+":"+str(e)[:160]
            bb={}
        intersection=set(aa)&set(bb)
        prices=[{"sku":sku,"api_cents":aa[sku].get("price_cents"),
                 "ui_cents":bb[sku].get("price_cents"),
                 "name":bb[sku]["name"][:110]}
                 for sku in sorted(intersection)
                 if isinstance(aa[sku].get("price_cents"),int) and
                    aa[sku]["price_cents"]!=bb[sku].get("price_cents")]
        record.update({"same_sku":len(intersection),"ui_only_count":len(set(bb)-set(aa)),
                       "ui_only_examples":[bb[x] for x in sorted(set(bb)-set(aa))[:5]],
                       "api_only_count":len(set(aa)-set(bb)),
                       "price_different_count":len(prices),
                       "price_different_examples":prices[:9],
                       "all_ui_categories_visited":bool(b.get("categories_visited")==b.get("categories_listed"))
                       if "ui_error" not in record else False})
        print("REAL_A_VS_B="+json.dumps(record,ensure_ascii=False),flush=True)
        report.append(record)
Path("audit-results/ui-vs-api-live-test.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf8")
if not all("api_count" in r and "ui_count" in r and r["ui_count"]>0 for r in report):
    raise SystemExit("At least one real method B store did not return visible cards")
