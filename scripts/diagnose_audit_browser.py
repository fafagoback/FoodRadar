"""Diagnostic-only browser test for the three audit stores: no price writes."""
import json
from playwright.sync_api import sync_playwright
from daily_triple_audit import STORES, loc_cookie

with sync_playwright() as pw:
    browser=pw.chromium.launch(headless=True,args=["--no-sandbox"])
    for slug,label,url,lat,lon in STORES:
        ctx=browser.new_context(locale="zh-TW",viewport={"width":1440,"height":950})
        ctx.add_cookies([{"name":"uev2.loc","value":loc_cookie(lat,lon),
                         "domain":".ubereats.com","path":"/"}])
        page=ctx.new_page()
        seen=[]
        page.on("response",lambda r:seen.append({"status":r.status,"url":r.url[:220]})
                if "getStore" in r.url or "getCatalog" in r.url or "challenge" in r.url else None)
        try:
            response=page.goto(url,wait_until="domcontentloaded",timeout=65000)
            page.wait_for_timeout(9000)
            before=page.evaluate("""() => ({
                url:location.href, title:document.title,body:(document.body?.innerText || '').slice(0,3500),
                htmlSize:document.documentElement?.outerHTML?.length || 0,
                totalImages:document.images.length, images:[...document.images].slice(0,12).map(e=>({alt:e.alt,src:e.currentSrc?.slice(0,100)})),
                testids:[...document.querySelectorAll('[data-testid]')].slice(0,35).map(e=>e.getAttribute('data-testid')),
                scroll:document.documentElement.scrollHeight,clientHeight:innerHeight,
                scrollable:[...document.querySelectorAll('*')].filter(e=>e.scrollHeight>e.clientHeight+100 && ['scroll','auto'].includes(getComputedStyle(e).overflowY)).slice(0,12).map(e=>({tag:e.tagName,className:String(e.className).slice(0,100),height:e.scrollHeight}))
            })""")
            page.mouse.wheel(0,800)
            page.wait_for_timeout(800)
            after=page.evaluate("document.documentElement.scrollTop")
            print("BROWSER_DIAG="+json.dumps({"store":label,"http":response.status if response else None,
                "before":before,"page_scroll_after":after,"requests":seen[:18]},ensure_ascii=False),flush=True)
        except Exception as e:
            print("BROWSER_DIAG_ERROR="+json.dumps({"store":label,"error":str(e)[:1000]},ensure_ascii=False),flush=True)
        finally:ctx.close()
    browser.close()
