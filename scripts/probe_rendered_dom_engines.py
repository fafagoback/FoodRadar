"""Independent live-rendered Uber Eats webpage probe on GitHub Actions.

This does NOT read __REACT_QUERY_STATE__, call Uber API directly, or read
the first method's files. Only visible product card DOM elements qualify.
"""
import json
import os
import time
from playwright.sync_api import sync_playwright

STORES=[
 ("costco","https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA"),
 ("pxmart","https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"),
 ("ubermart","https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"),
]
JS=r"""() => {
  const all=[...document.querySelectorAll('[data-testid],li,article,[role="listitem"]')];
  const seen=new Set(),items=[];
  for(const n of all){
    if(!n.getClientRects().length)continue;
    const txt=(n.innerText||'').trim();
    if(!txt || txt.length>800 || !/(?:NT\$|\$|＄)\s*\d+/.test(txt))continue;
    const name=(n.querySelector('h2,h3,h4,[role="heading"]')?.innerText||n.querySelector('img[alt]')?.alt||txt.split('\n')[0]).trim();
    if(!name || name.length<3 || name.length>200)continue;
    const price=(txt.match(/(?:NT\$|\$|＄)\s*\d+(?:\.\d{1,2})?/)||[])[0];
    const key=name+'|'+price;
    if(!seen.has(key)){seen.add(key);items.push({name,price});}
  }
  return {items:items.slice(0,15),count:seen.size,visibleText:(document.body?.innerText||'').slice(0,240)};
}"""

with sync_playwright() as pw:
  engines=[("webkit",pw.webkit),("firefox",pw.firefox),("chromium",pw.chromium)]
  for engine_name,engine in engines:
    for store,url in STORES:
      try:
        browser=engine.launch(headless=True)
        ctx=browser.new_context(locale="zh-TW",viewport={"width":1440,"height":900})
        page=ctx.new_page()
        response=page.goto(url,wait_until="domcontentloaded",timeout=40000)
        page.wait_for_timeout(3000)
        result=page.evaluate(JS)
        challenge="正在執行安全驗證" in result["visibleText"] or "Cloudflare" in result["visibleText"]
        print("REAL_RENDERED_DOM="+json.dumps({"engine":engine_name,"store":store,"http":response.status if response else None,
          "final_url":page.url[:180],"title":page.title()[:120],"challenge":challenge,"data":result},ensure_ascii=False),flush=True)
        browser.close()
      except Exception as e:
        print("REAL_RENDERED_DOM_ERROR="+json.dumps({"engine":engine_name,"store":store,"error":str(e)[:300]},ensure_ascii=False),flush=True)
