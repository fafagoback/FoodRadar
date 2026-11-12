"""Inspect how Uber Eats' REAL browser loads a PX Mart category beyond offset 1020.

Targeted read-only WebKit navigation & network audit. No crawling outside ONE
store; no HF, Turso, or third-party proxy. Detect whether the UI itself ever
requests deep pages or instead navigates per subsection.
"""
from __future__ import annotations
import json
import sys
import time
import urllib.parse
from collections import Counter
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from menu_core import attach_delivery_location
from catalog_complete import _section_ids
import requests

STORE="db0e33e1-2eb6-5ac6-954d-fdedda8fe7b4"
TARGET="71498aa4-69d8-58ec-87cf-8d9ffd75d6e0"
URL="https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"
LAT,LON=25.115,121.520
OUT=Path("audit-results/pxmart-browser-pagination-1020.json")
OUT.parent.mkdir(exist_ok=True)

s=requests.Session()
assert attach_delivery_location(s,{"discovered_points":[{
    "lat":LAT, "lon":LON, "county":"台北市"}]})
cookie=s.cookies.get("uev2.loc")
assert cookie
api_header={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
            "Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
            "Content-Type":"application/json","x-csrf-token":"x"}
base=s.post("https://www.ubereats.com/api/getStoreV1",
            json={"storeUuid":STORE,"sfNuggetCount":24},headers=api_header,timeout=30)
base.raise_for_status()
data=base.json().get("data") or {}
parent=[x for xs in (data.get("aisles") or {}).values() if isinstance(xs,list)
        for x in xs if isinstance(x,dict) and x.get("catalogSectionUUID")==TARGET]
desc=[x for x in data.get("sections") or [] if isinstance(x,dict) and x.get("uuid")==TARGET]
print("PX_WEB_TARGET_METADATA="+json.dumps({
    "target_parent_uuid":TARGET,"aisle_matches":parent[:2],
    "section_descriptors":desc[:1],"store_sections":len(_section_ids(data))
},ensure_ascii=False,default=str)[:3000],flush=True)

network=[]
dom_ids=set()
tabs=[]
errors=[]
with sync_playwright() as pw:
    browser=pw.webkit.launch(headless=True)
    try:
        context=browser.new_context(locale="zh-TW",
                                    viewport={"width":1366,"height":920})
        context.add_cookies([{"name":"uev2.loc","value":cookie,
                             "domain":".ubereats.com","path":"/"}])
        page=context.new_page()

        def on_response(response):
            if "getCatalogPresentationV2" not in response.url:
                return
            try:
                payload=response.request.post_data_json or {}
                paging=payload.get("pagingInfo") or {}
                filters=payload.get("storeFilters") or {}
                body=response.json()
                data=body.get("data") or {}
                groups=data.get("catalog") or []
                ids=[]
                if isinstance(groups,list):
                    for group in groups:
                        for item in (((group.get("payload") or {})
                                      .get("standardItemsPayload") or {})
                                      .get("catalogItems") or []):
                            if isinstance(item,dict) and item.get("uuid"):
                                ids.append(item["uuid"])
                rec={
                    "http":response.status,"url_path":response.url.split("?")[0][-90:],
                    "app_status":body.get("status"),
                    "app_code":data.get("code"),"app_message":str(data.get("message") or "")[:120],
                    "requested_offset":paging.get("offset"),
                    "next_offset":(data.get("pagingInfo") or {}).get("offset"),
                    "hasMore":(data.get("pagingInfo") or {}).get("hasMore"),
                    "sections":filters.get("sectionUuids"),
                    "subsections":filters.get("subsectionUuids"),
                    "sectionTypes":filters.get("sectionTypes"),
                    "items":len(ids),"unique_ids":len(set(ids)),
                    "sample_skus":ids[:3]}
                network.append(rec)
                if (rec["requested_offset"] or 0)>=840 or TARGET in (rec["sections"] or []):
                    print("PX_WEB_REQUEST="+json.dumps(rec,ensure_ascii=False),flush=True)
            except Exception as exc:
                errors.append("network:"+type(exc).__name__+":"+str(exc)[:150])

        page.on("response",on_response)
        response=page.goto(URL,wait_until="domcontentloaded",timeout=60000)
        page.wait_for_timeout(3000)
        initial=page.evaluate("()=>(document.body?.innerText||'').slice(0,1000)")
        tabs=[str(x).strip() for x in page.locator('[role="tab"]').all_text_contents()]
        print("PX_WEB_OPEN="+json.dumps({
            "http":response.status if response else None,
            "url":page.url,"title":page.title(),"tabs":tabs,
            "body_intro":initial[:500],
            "browser_catalog_requests":len(network)
        },ensure_ascii=False),flush=True)
        if (response and response.status in [401,403,429]) or "正在執行安全驗證" in initial:
            raise RuntimeError("Browser security challenge; no valid UI network evidence")

        # Try clicking each visible category tab until the TARGET parent starts
        # loading, or their full list is exhausted.
        matching_names=[]
        for item in parent+desc:
            for key in ["name","title","label","sectionName","displayName"]:
                v=item.get(key)
                if isinstance(v,str) and v:matching_names.append(v)
                if isinstance(v,dict):
                    for key2 in ["text","title","name"]:
                        if isinstance(v.get(key2),str):matching_names.append(v[key2])
        print("PX_WEB_TARGET_TITLES="+json.dumps(matching_names,ensure_ascii=False),flush=True)

        ordered=[]
        for title in matching_names:
            for t in tabs:
                if t and t not in ordered and (title in t or t in title):
                    ordered.append(t)
        # Other tabs only if target not discoverable by title.
        ordered.extend([t for t in tabs if t and t not in ordered])
        selected=None
        for t in ordered[:35]:
            try:
                before=len(network)
                clicked=page.evaluate("""(name)=>{
                    const t=[...document.querySelectorAll('[role="tab"]')]
                      .find(x=>(x.textContent||'').trim()===name);
                    if(!t)return false;
                    t.click();return true;
                }""",t)
                if not clicked:continue
                page.wait_for_timeout(700)
                if any(TARGET in (req.get("sections") or [])
                       for req in network[before:]):
                    selected=t
                    break
                if t in ordered[:len(matching_names)]:
                    selected=t
                    break
            except Exception as exc:
                errors.append("tab:"+t+":"+str(exc)[:100])
        print("PX_WEB_SELECTED_TAB="+json.dumps({
            "tab":selected,"all_tabs":len(tabs),
            "network_count":len(network)
        },ensure_ascii=False),flush=True)

        # Observe real DOM cards as user scrolls. Includes virtualized item
        # cards; accumulate UUIDs, do NOT assume DOM count is full inventory.
        for step in range(1,141):
            visible=page.evaluate("""()=>
                [...document.querySelectorAll('[data-testid^="store-item-"]')]
                .filter(x=>x.getClientRects().length)
                .map(x=>(x.getAttribute('data-testid')||'').slice(11))
                .filter(x=>/^[a-f0-9-]{36}$/i.test(x))
            """)
            dom_ids.update(visible)
            page.mouse.wheel(0,1450)
            page.wait_for_timeout(250)
            if step in (1,10,20,40,60,80,100,120,140):
                pos=page.evaluate("""()=>{
                    const el=document.scrollingElement||document.documentElement;
                    return {top:Math.round(el.scrollTop),
                            bottom:Math.round(el.scrollHeight-el.clientHeight)}
                }""")
                latest=network[-1] if network else None
                print("PX_WEB_SCROLL="+json.dumps({
                    "step":step,"observed_ui_uuid":len(dom_ids),
                    "network_requests":len(network),
                    "latest":latest,
                    "scroll":pos
                },ensure_ascii=False),flush=True)

        summary={
            "store_uuid":STORE,"target_section_uuid":TARGET,
            "browser_http":response.status if response else None,
            "browser_tabs":tabs,"selected_tab":selected,
            "browser_catalog_requests":len(network),
            "browser_offsets":sorted(set(x.get("requested_offset") for x in network
                                          if isinstance(x.get("requested_offset"),int))),
            "browser_direct_1020_requested":any(
                x.get("requested_offset")==1020 for x in network),
            "browser_target_section_request_count":sum(
                TARGET in (r.get("sections") or []) for r in network),
            "browser_dom_card_uuids_seen":len(dom_ids),
            "browser_1020_or_later": [
                x for x in network if (x.get("requested_offset") or 0)>=1020],
            "browser_network_requests":network,
            "errors":errors,
            "no_hf_turso_write":True,
        }
        OUT.write_text(json.dumps(summary,ensure_ascii=False,indent=2),
                       encoding="utf-8")
        print("PX_WEB_BROWSER_FINAL="+json.dumps({
            key:value for key,value in summary.items()
            if key not in ("browser_network_requests","browser_tabs")
        },ensure_ascii=False),flush=True)
        if not network:
            raise RuntimeError("Actual WebKit page did not issue any catalog API request")
    finally:
        browser.close()
