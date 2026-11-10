"""Four-store, two-method actual Uber Eats SKU reconciliation.

One GitHub job per store. A: native catalog API, independently paged per
category; B: observed visible WebKit DOM cards. No hydration or API reuse
in method B. Preserve partial evidence even on failure.
"""
import json
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright
from daily_triple_audit import collect_api, loc_cookie
from live_webkit_dom import scan_live_webpage

STORES = {
    "costco_beitou": (
        "Costco 好市多 北投店",
        "https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%8C%97%E6%8A%95%E5%BA%97/pOIgHV_HWhCkGt_liXWnpw",
        25.13184, 121.498632),
    "pxmart_beitou_mingde": (
        "全聯福利中心 北投明德",
        "https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA",
        25.115, 121.520),
    "ubermart_beitou": (
        "Uber Eats 優市 北投店",
        "https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg",
        25.123246847935267, 121.52951826298867),
    "carrefour_beitou_guangming": (
        "家樂福超市 北投光明店",
        "https://www.ubereats.com/tw/store/%E5%AE%B6%E6%A8%82%E7%A6%8F%E8%B6%85%E5%B8%82-%E5%8C%97%E6%8A%95%E5%85%89%E6%98%8E%E5%BA%97/d6zC-zVpUzikW3dTh2EYSQ",
        25.134, 121.500),
}
slug=os.environ.get("STORE_ID","costco_beitou")
if slug not in STORES:
    raise SystemExit("Unknown STORE_ID: "+slug)
name,url,lat,lon=STORES[slug]
output=Path("audit-results")/("crosscheck-"+slug+".json")
output.parent.mkdir(parents=True,exist_ok=True)
result={
    "store":name,"slug":slug,"store_url":url,
    "lat":lat,"lon":lon,"timestamp":datetime.now(timezone.utc).isoformat(),
    "method_a":"getStoreV1 and per-section getCatalogPresentationV2",
    "method_b":"actual rendered WebKit product cards only",
    "complete":False,
}
def save():
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf8")
def fail(phase,exc):
    result[phase+"_error"]=type(exc).__name__+": "+str(exc)[:500]
    print("STORE_ERROR="+json.dumps({"store":slug,"phase":phase,
           "error":result[phase+"_error"]},ensure_ascii=False),flush=True)
    save()
start=time.monotonic()
aa={}
with tempfile.TemporaryDirectory(prefix="foodradar-four-") as folder:
    try:
        a=collect_api(name,url,lat,lon,Path(folder))
        aa={x["sku"]:x for x in a["items"] if x.get("sku")}
        result["api"]={"count":len(aa),"pages":a["pages"],
                       "categories":a["categories"],
                       "pagination_complete":a["complete"]}
        save()
        print("STORE_METHOD_A="+json.dumps({
            "store":slug,**result["api"],"seconds":round(time.monotonic()-start,2)
        },ensure_ascii=False),flush=True)
    except Exception as exc:
        fail("api",exc)
    bb={}
    try:
        with sync_playwright() as pw:
            browser=pw.webkit.launch(headless=True)
            try:
                context=browser.new_context(locale="zh-TW",viewport={"width":1440,"height":980})
                context.add_cookies([{"name":"uev2.loc","value":loc_cookie(lat,lon),
                                     "domain":".ubereats.com","path":"/"}])
                page=context.new_page()
                response=page.goto(url,wait_until="domcontentloaded",timeout=40000)
                page.wait_for_timeout(1600)
                http=response.status if response else None
                title=page.title()
                text=page.evaluate("()=> (document.body?.innerText||'').slice(0,420)")
                result["ui_http_status"]=http
                result["ui_page_title"]=title
                if http in (401,403,429) or "正在執行安全驗證" in text:
                    raise RuntimeError("Store website security challenge (not a verified capture)")
                if not page.locator('[role="tab"]').count():
                    raise RuntimeError("No rendered category tabs on page")
                ui=scan_live_webpage(page,limit_categories=0,scroll_steps=42,stop_after_stale=6)
                bb={x["sku"]:x for x in ui["items"] if x.get("sku")}
                result["ui"]={
                    "count":len(bb),"categories_listed":ui["categories_listed"],
                    "categories_visited":ui["categories_visited"],
                    "category_stats":ui["category_stats"],"errors":ui["errors"],
                    "all_tabs_visited":ui["all_tabs_visited"],
                    "whole_catalog_proven_complete":False,
                    "scan_budgets_exhausted":ui.get("scan_budgets_exhausted",0),
                }
                save()
                print("STORE_METHOD_B="+json.dumps({
                    "store":slug,"count":len(bb),
                    "categories_listed":ui["categories_listed"],
                    "categories_visited":ui["categories_visited"],
                    "errors":ui["errors"],"seconds":round(time.monotonic()-start,2)
                },ensure_ascii=False),flush=True)
                context.close()
            finally:
                browser.close()
    except Exception as exc:
        fail("ui",exc)
    if aa and bb:
        common=set(aa)&set(bb)
        aonly=set(aa)-set(bb)
        bonly=set(bb)-set(aa)
        # Site prices are displayed in whole NT dollars; API can retain cents.
        prices=[]
        for sku in common:
            x=aa[sku].get("price_cents")
            y=bb[sku].get("price_cents")
            if isinstance(x,(int,float)) and math.isfinite(x) and isinstance(y,int):
                rounded=int(math.floor(x/100+0.5))*100
                if rounded!=y:
                    prices.append({"sku":sku,"api_cents":x,"api_display_cents":rounded,"ui_cents":y})
        result["comparison"]={
            "common_skus":len(common),
            "api_only_skus":len(aonly),
            "ui_only_skus":len(bonly),
            "api_only_examples":[{"sku":sku,**aa[sku]} for sku in sorted(aonly)[:10]],
            "ui_only_examples":[{"sku":sku,**bb[sku]} for sku in sorted(bonly)[:10]],
            "api_only_all_skus":sorted(aonly),
            "ui_only_all_skus":sorted(bonly),
            "display_price_mismatches":len(prices),
            "price_mismatch_examples":prices[:10],
        }
        result["capture_execution_complete"]=bool(
            result["api"]["pagination_complete"] and
            result["ui"]["all_tabs_visited"] and
            not result["ui"]["errors"])
        result["exact_sku_match"]=not aonly and not bonly
        result["browser_scan_budget_exhausted"]=bool(
            result["ui"]["scan_budgets_exhausted"])
        # Catalog inventory completeness is NOT proven by visiting every tab.
        result["complete"]=False
        result["status"]=("BOTH_METHODS_CAPTURED_DIFFERENCES_UNRESOLVED"
                          if result["capture_execution_complete"] else
                          "CAPTURE_INCOMPLETE")
    result["elapsed_seconds"]=round(time.monotonic()-start,2)
    save()
    print("FOUR_STORE_CROSSCHECK="+json.dumps({
        "store":slug,"api":result.get("api"),
        "ui":result.get("ui",{}).get("count"),
        "categories_visited":result.get("ui",{}).get("categories_visited"),
        "categories_listed":result.get("ui",{}).get("categories_listed"),
        "comparison":result.get("comparison"),
        "complete":result["complete"],
        "capture_execution_complete":result.get("capture_execution_complete",False),
        "status":result.get("status","CAPTURE_INCOMPLETE"),
        "browser_scan_budget_exhausted":result.get("browser_scan_budget_exhausted"),
        "seconds":result["elapsed_seconds"],
        "errors":[result.get("api_error"),result.get("ui_error"),
                  result.get("ui",{}).get("errors")],
    },ensure_ascii=False),flush=True)
    if not result.get("capture_execution_complete"):
        raise SystemExit("CAPTURE INCOMPLETE: inspect reported API/UI data and scan budgets")
