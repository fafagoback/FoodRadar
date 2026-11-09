"""Inspect real WebKit rendered item card structure, without reading any API or SSR JSON."""
import json
from playwright.sync_api import sync_playwright
url="https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"
with sync_playwright() as pw:
  browser=pw.webkit.launch(headless=True)
  page=browser.new_page(locale="zh-TW",viewport={"width":1440,"height":1000})
  response=page.goto(url,wait_until="domcontentloaded",timeout=45000)
  page.wait_for_timeout(2200)
  info=page.evaluate(r"""() => {
    const needles=['舒潔棉柔厚韌','KIRIN 生茶525ml','Tempo極吸萬用'];
    const nodes=[...document.querySelectorAll('h1,h2,h3,h4,h5,[role="heading"],img[alt]')].filter(n=>
      needles.some(x=>(n.innerText||n.alt||'').includes(x)) && n.getClientRects().length>0).slice(0,9);
    const cards=nodes.map(n=>{const parents=[];let p=n;for(let i=0;i<6&&p;i++,p=p.parentElement){
      parents.push({tag:p.tagName,role:p.getAttribute('role'),testid:p.getAttribute('data-testid'),
       className:String(p.className).slice(0,110),text:(p.innerText||'').slice(0,220),
       productId:p.getAttribute('data-item-id')||p.getAttribute('data-uuid'),
       imgCount:p.querySelectorAll('img').length,
       headings:p.querySelectorAll('h1,h2,h3,h4,h5,[role=heading]').length,
       rect:(()=>{const r=p.getBoundingClientRect();return [Math.round(r.width),Math.round(r.height)]})()});
    }return {item:(n.innerText||n.alt||'').slice(0,90),parents}});
    const tabs=[...document.querySelectorAll('[role="tab"]')].filter(x=>x.getClientRects().length>0)
      .slice(0,8).map(x=>({label:x.innerText,id:x.id,selected:x.getAttribute('aria-selected')}));
    return {cards,tabs,title:document.title};
  }""")
  print("REAL_DOM_STRUCTURE="+json.dumps({"http":response.status,"data":info},ensure_ascii=False),flush=True)
  browser.close()
