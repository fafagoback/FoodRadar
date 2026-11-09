"""Real GitHub-hosted egress feasibility probe for independent public storefront HTML.

Read-only. No getStoreV1/getCatalogPresentationV2 calls and no copying FoodRadar Raw.
A source counts as working ONLY if it returns real product names WITH prices
for at least two stores, not just a successful HTTP status.
"""
from __future__ import annotations
import concurrent.futures, json, re, time
from urllib.parse import quote, urlparse
import requests
from curl_cffi import requests as curl_requests

STORES=[
("costco","https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA"),
("pxmart","https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"),
("ubermart","https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"),
]
DOLLAR=re.compile(r'(?:NT\\?\$|NTD|\$|＄|新臺幣|元)\s*\d{1,5}')
CHALLENGE=re.compile(r'cloudflare|正在執行安全驗證|verify you are human|just a moment|checking your browser',re.I)
PRODUCT=re.compile(r'哈根達斯|冰淇淋|牛奶|巧克力|咖啡|蔬菜|衛生紙|costco|全聯|優市|[零一二三四五六七八九十百]入',re.I)

def fetch_one(kind,store,url):
    started=time.monotonic()
    try:
        if kind=="jina_https":
            target="https://r.jina.ai/"+url
            resp=requests.get(target,headers={"Accept":"text/plain","User-Agent":"FoodRadar-Audit/1.0"},
                              timeout=42)
            text=resp.text
        elif kind=="jina_http":
            target="https://r.jina.ai/"+url.replace("https://","http://",1)
            resp=requests.get(target,headers={"Accept":"text/plain"},timeout=42)
            text=resp.text
        elif kind=="translate":
            parts=urlparse(url)
            target=f"https://www-ubereats-com.translate.goog{parts.path}?_x_tr_sl=zh-TW&_x_tr_tl=en&_x_tr_hl=en"
            resp=requests.get(target,headers={"Accept-Language":"en-US,en;q=0.9"},timeout=35)
            text=resp.text
        elif kind=="google_translate":
            target="https://translate.google.com/translate?sl=auto&tl=en&u="+quote(url,safe="")
            resp=requests.get(target,timeout=35)
            text=resp.text
        elif kind=="curl_chrome":
            resp=curl_requests.get(url,impersonate="chrome",timeout=35)
            text=resp.text
        elif kind=="curl_safari":
            resp=curl_requests.get(url,impersonate="safari",timeout=35)
            text=resp.text
        else:
            raise ValueError(kind)
        sample=text[:350]
        dollars=len(DOLLAR.findall(text))
        products=len(PRODUCT.findall(text))
        challenge=bool(CHALLENGE.search(text[:8000]))
        genuine = (resp.status_code==200 and not challenge and
                   dollars>=3 and products>=5 and len(text)>4000)
        return dict(method=kind,store=store,status=resp.status_code,chars=len(text),
                    prices=dollars,product_terms=products,challenge=challenge,
                    genuine_product_page=genuine,elapsed=round(time.monotonic()-started,2),
                    final_url=str(resp.url)[:150],sample=sample[:180])
    except Exception as e:
        return dict(method=kind,store=store,genuine_product_page=False,
                    error=str(e)[:300],elapsed=round(time.monotonic()-started,2))

def main():
    kinds=["jina_https","jina_http","translate","google_translate","curl_chrome","curl_safari"]
    jobs=[(k,name,url) for k in kinds for name,url in STORES]
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures={pool.submit(fetch_one,*job):job for job in jobs}
        results=[]
        for fut in concurrent.futures.as_completed(futures):
            result=fut.result()
            print("PUBLIC_HTML_PROBE="+json.dumps(result,ensure_ascii=False),flush=True)
            results.append(result)
    feasible=sorted({k for k in kinds if sum(x["genuine_product_page"]
                    for x in results if x["method"]==k)>=2})
    print("PUBLIC_HTML_FEASIBLE_METHODS="+json.dumps(feasible),flush=True)
    with open("public-html-probe.json","w",encoding="utf-8") as f:
        json.dump({"feasible":feasible,"results":results},f,ensure_ascii=False,indent=2)

if __name__=="__main__":
    main()
