"""Read-only probe of native Uber Eats storefront HTML on different hosted CI OSes.

This intentionally never calls getStoreV1 or getCatalogPresentationV2:
the DOM is an independent storefront observation only.
"""
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

SITES=[
    ("Costco 內湖","https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA"),
    ("全聯北投明德","https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"),
    ("優市北投","https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"),
]
out=Path("audit-browser-probe")
out.mkdir(exist_ok=True)

def probe():
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        try:
            for i,(name,url) in enumerate(SITES):
                context=browser.new_context(locale="zh-TW", viewport={"width":1280,"height":900})
                page=context.new_page()
                try:
                    response=page.goto(url,wait_until="domcontentloaded",timeout=45000)
                    page.wait_for_timeout(3500)
                    status=response.status if response else None
                    body=page.evaluate("""() => (document.body?.innerText || '').slice(0,1200)""")
                    items=page.evaluate("""() => ({
                      testids: [...document.querySelectorAll('[data-testid]')].map(n=>n.dataset.testid).filter(Boolean).slice(0,50),
                      images: [...document.querySelectorAll('img[alt]')].slice(0,15).map(n=>n.alt),
                      menuCards: document.querySelectorAll('[data-testid*=item],[data-testid*=Item]').length
                    })""")
                    challenge=bool(status in (401,403,429) or "Cloudflare" in body or "安全驗證" in body)
                    print("CI_BROWSER_READ="+json.dumps({"runner":os.environ.get("RUNNER_OS"),"store":name,"http":status,
                        "url":page.url,"challenge":challenge,"title":page.title(),"body_excerpt":body[:350],
                        "testids":items["testids"],"image_alts":items["images"],"item_cards":items["menuCards"]
                    },ensure_ascii=False),flush=True)
                    page.screenshot(path=str(out/f"{os.environ.get('RUNNER_OS')}-{i}.png"),full_page=False)
                except Exception as e:
                    print("CI_BROWSER_ERROR="+json.dumps({"runner":os.environ.get("RUNNER_OS"),
                         "store":name,"error":str(e)[:350]},ensure_ascii=False),flush=True)
                finally:context.close()
        finally: browser.close()
if __name__=="__main__":probe()
