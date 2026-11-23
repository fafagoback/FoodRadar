"""Method B: obtain catalog preview from Uber Eats' public *HTML* page.

Uses curl_cffi Safari TLS/HTTP request to request /tw/store/... as a webpage.
Never calls /api/getStoreV1, /api/getCatalogPresentationV2, HF or Turso.

The webpage's SSR __REACT_QUERY_STATE__ contains some product records, but
these are INITIAL page products ONLY. Therefore it MUST NOT claim a complete
inventory nor masquerade as independently sourced catalog data: the state
was server-hydrated using getStoreV1 internally.
"""
from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as safari_requests


def decode_embedded_json(raw):
    """Undo Uber HTML script delimiter escaping without ruining JSON strings."""
    # Structural double quotes are encoded as \u0022; embedded quote
    # characters that are preceded by percent-encoded \\ (%5C) must remain
    # escaped to avoid ending their JSON string prematurely.
    expanded = re.sub(r"(?<!%5C)\\u0022", '"', raw, flags=re.I)
    return json.loads(expanded)


def extract_preview_html(html, *, http_status=200, final_url=""):
    """Return only observed preview SKUs/price and explicit partial status."""
    if http_status != 200 or "def.uber.com" in urlparse(final_url).netloc:
        return {
            "count":None, "items":[], "complete":False,
            "status":"WEB_HTML_BLOCKED","http_status":http_status,
            "error":"Store HTML did not load product data"
        }
    soup=BeautifulSoup(html,"html.parser")
    script=soup.find("script",id="__REACT_QUERY_STATE__")
    if not script:
        return {"count":None,"items":[],"complete":False,
                "status":"HTML_BOOTSTRAP_MISSING","http_status":http_status}
    payload=decode_embedded_json(script.string or script.get_text() or "")
    queries=payload.get("queries") or []
    stores=[q.get("state",{}).get("data") for q in queries
            if isinstance(q,dict) and isinstance(q.get("queryKey"),list)
            and q["queryKey"] and q["queryKey"][0]=="getStoreV1"
            and isinstance(q.get("state",{}).get("data"),dict)]
    if not stores:
        return {"count":None,"items":[],"complete":False,
                "status":"HTML_NO_STORE_PREVIEW","http_status":http_status}
    data=stores[0]
    rows={}
    for sections in (data.get("catalogSectionsMap") or {}).values():
        if not isinstance(sections,list):continue
        for section in sections:
            if not isinstance(section,dict):continue
            body=((section.get("payload") or {}).get("standardItemsPayload") or {})
            for item in body.get("catalogItems") or []:
                if not isinstance(item,dict):continue
                sku=item.get("uuid") or item.get("itemUuid")
                title=item.get("title")
                cents=item.get("price")
                if not isinstance(sku,str) or not isinstance(title,str) or not isinstance(cents,(int,float)):
                    continue
                if cents <= 0:continue
                rows.setdefault(sku, {"sku":sku,"name":title,"price_cents":cents,
                                      "is_available":item.get("isAvailable"),
                                      "is_sold_out":item.get("isSoldOut")})
    # Presence of a store preview does not prove all categories/page offsets.
    if not rows:
        return {"count":None,"items":[],"complete":False,
                "status":"HTML_NO_PRICED_ITEMS","http_status":http_status}
    return {
        "count":len(rows),"items":list(rows.values()),"complete":False,
        "status":"HTML_PREVIEW_PARTIAL","http_status":http_status,
        "method":"public_storefront_safari_html",
        "limitation":"Initial HTML embeds server-cached getStoreV1 preview only; "
                     "not a separate complete catalog source",
        "preview_categories":sum(len(x) for x in (data.get("catalogSectionsMap") or {}).values() if isinstance(x,list)),
        "store_uuid":data.get("uuid"),
    }


def collect_public_html(url):
    """Direct store PAGE request (not API) executed by GitHub Actions."""
    parsed=urlparse(url)
    if parsed.scheme!="https" or parsed.netloc not in ("www.ubereats.com","ubereats.com") or not parsed.path.startswith("/tw/store/"):
        raise ValueError("Only https://www.ubereats.com/tw/store/ pages supported")
    response=safari_requests.get(url,impersonate="safari",timeout=40)
    result=extract_preview_html(response.text,http_status=response.status_code,final_url=str(response.url))
    result["source_url"]=url
    return result
