"""Diagnose usable public storefront product HTML from curl_cffi Safari transport.

No Uber API requests. Displays only brief HTML selector/path samples.
"""
import json,re
from bs4 import BeautifulSoup
from curl_cffi import requests

STORES=[
("costco","https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA"),
("pxmart","https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"),
("ubermart","https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg"),
]

for name,url in STORES:
    try:
        resp=requests.get(url,impersonate="safari",timeout=35)
        soup=BeautifulSoup(resp.text,"html.parser")
        scripts=[{"id":s.get("id"),"type":s.get("type"),"length":len(s.string or s.text),
                   "start":str(s.string or s.text)[:80]} for s in soup.find_all("script")]
        images=[{"alt":x.get("alt"),"attrs":{k:str(v)[:60] for k,v in x.attrs.items() if k.startswith("data-")}} for x in soup.find_all("img",alt=True) if x.get("alt")][:8]
        targets=[{"tag":n.name,"id":n.get("data-testid"),"text":n.get_text(" ",strip=True)[:220],
                  "classes":n.get("class",[])[:5]} for n in soup.find_all(attrs={"data-testid":True})
                 if any(x in str(n.get("data-testid","")).lower() for x in ("item","product","catalog","store"))][:18]
        json_scripts=[s for s in scripts if s["id"] or s["type"] in ("application/json","application/ld+json")]
        h=resp.text
        idx=h.find("哈根達斯")
        context=h[max(0,idx-400):idx+500] if idx>=0 else ""
        print("SAFARI_HTML_SHAPE="+json.dumps({"store":name,"status":resp.status_code,
              "page_len":len(h),"text_len":len(soup.get_text(" ",strip=True)),
              "script_count":len(scripts),"json_scripts":json_scripts[:15],
              "large_scripts":sorted(scripts,key=lambda x:x["length"],reverse=True)[:7],
              "testids":targets,"images":images,"haagen_context":context[:900],
              "visible_text_sample":soup.get_text(" ",strip=True)[:350]},
              ensure_ascii=False),flush=True)
    except Exception as e:
        print("SAFARI_SHAPE_ERROR="+json.dumps({"store":name,"error":str(e)[:500]},ensure_ascii=False),flush=True)
