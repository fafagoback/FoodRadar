"""Inspect React Query SSR page bootstrap from HTML, NOT Uber API.

Read only; report paths and sample product fields, never whole raw data.
"""
import json
import re
from bs4 import BeautifulSoup
from curl_cffi import requests
from probe_safari_html_shape import STORES

def walk(obj,path="",depth=0,maxdepth=18):
    if depth>maxdepth:return
    if isinstance(obj,dict):
        lower={str(k).lower() for k in obj}
        if {"price","title"}.issubset(lower) or {"price","uuid"}.issubset(lower):
            yield (path,list(obj.keys())[:28],{k:str(obj.get(k))[:140] for k in
                  ("uuid","title","price","isAvailable","isSoldOut","imageUrl") if k in obj})
        if any("catalog" in str(k).lower() for k in obj):
            yield (path+"#catalog_keys",[k for k in obj if "catalog" in str(k).lower()],{})
        for k,v in obj.items():
            if isinstance(v,(list,dict)):yield from walk(v,path+"/"+str(k),depth+1,maxdepth)
    elif isinstance(obj,list):
        for i,v in enumerate(obj):
            if i>12:break
            if isinstance(v,(list,dict)):yield from walk(v,path+f"[{i}]",depth+1,maxdepth)

for name,url in STORES:
    try:
        r=requests.get(url,impersonate="safari",timeout=40)
        s=BeautifulSoup(r.text,"html.parser")
        node=s.find("script",id="__REACT_QUERY_STATE__")
        raw=(node.string if node else None) or (node.get_text() if node else "")
        # Uber escapes quote delimiters as literal \\u0022 in its script
        # (not valid standalone JSON until this HTML-script escaping is decoded).
        # In Uber's script, structural quotes are unicode-escaped; quote
        # characters inside percent-encoded string content can also occur.
        # Keep the latter escaped, rather than prematurely ending a JSON string.
        decoded=re.sub(r"(?<!%5C)\\u0022", '"', raw, flags=re.I)
        try:
            data=json.loads(decoded)
        except json.JSONDecodeError as err:
            print("SAFARI_BOOTSTRAP_PARSE_CONTEXT="+json.dumps({
                "store":name,"position":err.pos,"context":decoded[max(0,err.pos-170):err.pos+180],
                "raw_context":raw[max(0,err.pos-170):err.pos+180]
            },ensure_ascii=False),flush=True)
            raise
        queries=data.get("queries") or []
        summarized=[{"key":q.get("queryKey"),"state_data_type":type((q.get("state") or {}).get("data")).__name__,
                     "keys":list((q.get("state") or {}).get("data") or {})[:12]
                     if isinstance((q.get("state") or {}).get("data"),dict) else []} for q in queries[:22]]
        found=list(walk(data))
        matches=[r for r in found if not r[0].endswith("#catalog_keys")]
        catalog=[r for r in found if r[0].endswith("#catalog_keys")]
        print("SAFARI_BOOTSTRAP="+json.dumps({"store":name,"http":r.status_code,"query_count":len(queries),
              "queries":summarized[:18],"product_nodes":len(matches),
              "sample_products":matches[:8],
              "catalog_paths":catalog[:15]},ensure_ascii=False),flush=True)
    except Exception as e:
        print("SAFARI_BOOTSTRAP_ERROR="+json.dumps({"store":name,"error":str(e)[:420]},ensure_ascii=False),flush=True)
