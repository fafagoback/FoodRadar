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
 // DOM-only incremental observation: never re-read the same expensive
 // rendered innerText on every scroll. No API or app state is accessed.
 const observed=window.__foodradar_dom_observed ||= new Set();
 for(const n of nodes) {
   if(!n.getClientRects().length)continue;
   const id=(n.getAttribute('data-testid')||'').slice('store-item-'.length);
   if(!/^[a-f0-9-]{36}$/i.test(id))continue;
   if(observed.has(id))continue;
   const lines=(n.innerText||'').split(/\n/).map(x=>x.trim()).filter(Boolean);
   const img=n.querySelector('img[alt]');
   const name=(img?.getAttribute('alt')||'').trim();
   if(!name || name.length<3)continue;
   const rx=/(?:NT\s*)?[$＄]\s*([\d,]+(?:\.\d{1,2})?)/g;
   const prices=[];
   const titleLine=lines.findIndex(x=>x===name);
   // Store cards show reference/strikethrough and current prices BEFORE
   // the title. Scan the entire price block and choose the current lower
   // standard price; skip Costco's separate member-only prices.
   const priceLines=titleLine>=0 ? lines.slice(0,titleLine) : lines.slice(0,5);
   for(const line of priceLines){
     if(/會員價|每公斤|\/kg|\/公斤/.test(line)) continue;
     for(const m of line.matchAll(rx)){
       const parsed=Number(m[1].replaceAll(',',''));
       if(parsed>0 && Number.isFinite(parsed)) prices.push(Math.round(parsed*100));
     }
   }
   if(!prices.length)continue;
   const price=Math.min(...prices);
   results.push({sku:id,name,price_cents:price,price_candidates:prices,
                 display_lines:lines.slice(0,7)});
   observed.add(id);
 }
 return results;
}"""

def scan_live_webpage(page, *, limit_categories=0, scroll_steps=42, stop_after_stale=6):
    """Observe rendered product cards. A tab click alone is NOT inventory proof.

    Evidence-based fixes: prior 14-scroll / 4-stale heuristic stopped early;
    only mark the category exhausted upon verified scroll-end/stagnation, or
    explicitly flag the scan budget limit (instead of silently succeeding).
    Do not use any API, hydration data, screenshots, or shared API results.
    """
    seen, stats, errors = {}, [], []

    def extract():
        for item in page.evaluate(DOM_ITEMS):
            seen.setdefault(item["sku"], item)

    def position():
        return page.evaluate("""() => {
            const el=document.scrollingElement || document.documentElement;
            return {scroll_top:Math.round(el.scrollTop),
                    scroll_end:Math.round(Math.max(0,el.scrollHeight-el.clientHeight))};
        }""")

    def card_diagnostics():
        return page.evaluate("""() => {
            let rendered=0, missing_alt=0, missing_price=0;
            for(const node of document.querySelectorAll('[data-testid^="store-item-"]')){
                if(!node.getClientRects().length)continue;
                rendered++;
                if(!(node.querySelector('img[alt]')?.getAttribute('alt')||'').trim())
                    missing_alt++;
                if(!/(?:NT\\s*)?[$＄]\\s*[\\d,]+/.test(node.innerText||''))
                    missing_price++;
            }
            return {rendered_cards:rendered, missing_alt, missing_price};
        }""")

    extract()
    labels = list(dict.fromkeys(x.strip() for x in
              page.locator('[role="tab"]').all_text_contents() if x.strip()))
    total = len(labels)
    maximum = total if limit_categories <= 0 else min(limit_categories, total)
    for index, label in enumerate(labels[:maximum]):
        started = time.monotonic()
        try:
            clicked = page.evaluate("""(label) => {
                const tabs=[...document.querySelectorAll('[role="tab"]')];
                const t=tabs.find(x=>(x.textContent||'').trim()===label);
                if(!t)return false;
                t.click();
                return true;
            }""", label)
            if not clicked:
                page.evaluate("window.scrollTo(0,0)")
                page.get_by_role("tab",name=label,exact=True).first.click(timeout=4500)
            page.wait_for_timeout(400)
            old = len(seen)
            extract()
            last = position()
            stable_at_bottom = 0
            stagnant = 0
            end_seen = False
            scanned_steps = 0
            scan_exhausted = True
            for scanned_steps in range(1, scroll_steps + 1):
                page.mouse.wheel(0,1350)
                page.wait_for_timeout(210)
                before=len(seen)
                extract()
                pos=position()
                no_new=(len(seen)==before)
                same_scroll=(pos["scroll_top"]==last["scroll_top"])
                at_end=(pos["scroll_top"]>=pos["scroll_end"]-4)
                end_seen=end_seen or at_end
                stagnant=(stagnant+1) if same_scroll and no_new else 0
                stable_at_bottom=(stable_at_bottom+1) if at_end and no_new else 0
                last=pos
                # A stalled viewport at the end (not merely 4 empty UUID
                # reads) is independent evidence the browser cannot advance.
                if stable_at_bottom>=stop_after_stale or stagnant>=stop_after_stale:
                    scan_exhausted=False
                    break
            # Keep unexplained exhaustion explicit rather than claiming success.
            diagnostic=card_diagnostics()
            stat={"category":label,
                  "new_skus":len(seen)-old,
                  "cumulative_skus":len(seen),
                  "elapsed_seconds":round(time.monotonic()-started,2),
                  "scroll_steps":scanned_steps,
                  "scroll_end_seen":end_seen,
                  "stopped_by_stagnation":not scan_exhausted,
                  "budget_exhausted":scan_exhausted,
                  "final_scroll_top":last["scroll_top"],
                  "final_scroll_end":last["scroll_end"],
                  **diagnostic}
            stats.append(stat)
            print("RENDERED_UI_CATEGORY="+json.dumps({
                "index":index+1,"total":total, **stat
            },ensure_ascii=False),flush=True)
        except Exception as err:
            report={"category":label,"error":type(err).__name__+":"+str(err)[:180]}
            errors.append(report)
            print("RENDERED_UI_CATEGORY_ERROR="+json.dumps({
                "index":index+1,"total":total,**report
            },ensure_ascii=False),flush=True)
    all_visited=total>0 and len(stats)==total and not errors
    return {
        "count":len(seen),
        "items":[{"sku":i["sku"],"name":i["name"],
                  "price_cents":i["price_cents"],
                  "price_candidates":i["price_candidates"]}
                 for i in seen.values()],
        "categories_listed":total,
        "categories_visited":len(stats),
        "category_stats":stats,"errors":errors,
        "all_tabs_visited":all_visited,
        "scan_budgets_exhausted":sum(bool(s["budget_exhausted"]) for s in stats),
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
