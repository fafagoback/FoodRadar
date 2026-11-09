"""Independent FoodRadar Method B: actual WebKit-rendered Uber Eats UI.

This ONLY reads visible item cards, their displayed text, tab clicks and
scrolls. Never reads React hydration state, fetch API responses, HF or DB.
Counts are SKU deduplicated, and completeness is NEVER inferred from a timer.
"""
from __future__ import annotations
import json
import os
import re
import time
from playwright.sync_api import sync_playwright

DOM_ITEMS=r"""() => {
 const nodes=[...document.querySelectorAll('[data-testid^="store-item-"]')];
 const results=[];
 for(const n of nodes) {
   if(!n.getClientRects().length)continue;
   const id=(n.getAttribute('data-testid')||'').slice('store-item-'.length);
   if(!/^[a-f0-9-]{36}$/i.test(id))continue;
   const lines=(n.innerText||'').split(/\n/).map(x=>x.trim()).filter(Boolean);
   const img=n.querySelector('img[alt]');
   const name=(img?.getAttribute('alt')||'').trim();
   if(!name || name.length<3)continue;
   const rx=/(?:NT\s*)?[$＄]\s*([\d,]+(?:\.\d{1,2})?)/g;
   const prices=[];
   for(const line of lines){
     if(/會員價|每公斤|\/kg|\/公斤/.test(line)) continue;
     for(const m of line.matchAll(rx)){
       const parsed=Number(m[1].replaceAll(',',''));
       if(parsed>0 && Number.isFinite(parsed)) prices.push(Math.round(parsed*100));
     }
     if(prices.length && lines.indexOf(line)<=3) break;
   }
   if(!prices.length)continue;
   const price=Math.min(...prices);
   results.push({sku:id,name,price_cents:price,price_candidates:prices,
                 display_lines:lines.slice(0,7)});
 }
 return results;
}"""

def scan_live_webpage(page, *, limit_categories=0, scroll_steps=16, stop_after_stale=3):
    """Explore actual visible category tabs, never API or embedded JSON."""
    seen={}
    category_stats=[]
    errors=[]
    def extract():
        current=page.evaluate(DOM_ITEMS)
        for item in current:
            sku=item["sku"]
            if sku not in seen:
                seen[sku]=item
        return len(current)
    extract()
    # Capture real UI category labels before they are virtualized.
    labels=page.locator('[role="tab"]').all_text_contents()
    labels=[x.strip() for x in labels if x.strip()]
    max_items=len(labels) if limit_categories<=0 else min(limit_categories,len(labels))
    for idx in range(max_items):
        label=labels[idx]
        try:
            tab=page.get_by_role("tab",name=label,exact=True).first
            tab.click(timeout=12000)
            page.wait_for_timeout(750)
            old=len(seen)
            extract()
            stale=0
            for _ in range(scroll_steps):
                page.mouse.wheel(0,1250)
                page.wait_for_timeout(280)
                before=len(seen)
                extract()
                stale=(stale+1) if len(seen)==before else 0
                if stale>=stop_after_stale:
                    break
            category_stats.append({"category":label,"new_skus":len(seen)-old,
                                   "cumulative_skus":len(seen)})
        except Exception as err:
            errors.append({"category":label,"error":type(err).__name__+":"+str(err)[:120]})
    return {
        "count":len(seen),
        "items":[{"sku":x["sku"],"name":x["name"],"price_cents":x["price_cents"],
                  "price_candidates":x["price_candidates"]} for x in seen.values()],
        "categories_listed":len(labels),"categories_visited":len(category_stats),
        "category_stats":category_stats,"errors":errors,
        "complete":False,
        "status":"REAL_RENDERED_UI_OBSERVED_NOT_PROVEN_COMPLETE",
        "method":"webkit_visible_ui_only",
    }

def capture_store(url, *, limit_categories=0, scroll_steps=16):
    with sync_playwright() as pw:
        browser=pw.webkit.launch(headless=True)
        try:
            page=browser.new_page(locale="zh-TW",viewport={"width":1440,"height":980})
            response=page.goto(url,wait_until="domcontentloaded",timeout=45000)
            page.wait_for_timeout(1600)
            status=response.status if response else None
            body=page.evaluate("()=>(document.body?.innerText||'').slice(0,400)")
            if status in (401,403,429) or '正在執行安全驗證' in body:
                return {"count":None,"items":[],"complete":False,
                        "status":"WEBPAGE_SECURITY_CHALLENGE","http_status":status}
            result=scan_live_webpage(page,limit_categories=limit_categories,scroll_steps=scroll_steps)
            result["http_status"]=status
            result["url"]=page.url
            return result
        finally:browser.close()

if __name__=="__main__":
    SITES=[
      ("costco","https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA"),
      ("pxmart","https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"),
      ("ubermart","https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"),
    ]
    for name,url in SITES:
        started=time.monotonic()
        try:
            result=capture_store(url,limit_categories=int(os.environ.get("DOM_CATEGORY_TEST_LIMIT","5")),
                                 scroll_steps=int(os.environ.get("DOM_SCROLL_TEST_LIMIT","10")))
            print("LIVE_UI_METHOD_B="+json.dumps({
                "store":name,"http":result.get("http_status"),
                "count":result.get("count"),"status":result["status"],
                "categories_listed":result.get("categories_listed"),
                "categories_visited":result.get("categories_visited"),
                "category_stats":result.get("category_stats"),
                "errors":result.get("errors"),
                "samples":result.get("items",[])[:5],
                "elapsed":round(time.monotonic()-started,2)
            },ensure_ascii=False),flush=True)
        except Exception as exc:
            print("LIVE_UI_METHOD_B_ERROR="+json.dumps({"store":name,"error":str(exc)[:350]},ensure_ascii=False),flush=True)
