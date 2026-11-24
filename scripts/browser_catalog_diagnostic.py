"""Read-only browser UI test. No direct RPC calls, login, orders, or bypass."""
import json, os, re, sys, urllib.parse
from pathlib import Path
from playwright.sync_api import sync_playwright, Error as PWError

ROOT=Path(__file__).resolve().parents[1]
CASES=[
 {"name":"屈臣氏天北門市","url":"https://www.ubereats.com/tw/store/watsons屈臣氏-天北門市-s0454/D9QOYkpSSaWGIAcGkGiW4Q",
  "parents":["e2b979b9-d1e4-512d-974b-52fee6d93604","1afa9c0f-13f3-524e-ae44-70e2fbefcd3e"],
  "child":"637d42bd-facb-589f-8aa7-068af8552717","labels":["美體美髮","日用品","保健食品"]},
 {"name":"萬家福天母店","url":"https://www.ubereats.com/tw/store/萬家福-天母店/dzzaHEkXQWqFJ5GgeOHG7w",
  "parents":["746d8e24-651c-5dcb-9c38-b681fe8fc96d","63bae3c0-fd6c-58d1-beb4-162504f683be"],
  "child":"5a1e3e94-c9ad-5869-9edc-20d0e9c77c91","labels":["休閒零嘴","日用百貨","個人清潔護理"]}]
case=CASES[int(sys.argv[1])]
out=ROOT/"browser-evidence"/str(sys.argv[1])
out.mkdir(parents=True,exist_ok=True)
report={"store":case["name"],"visits":[],"network":[],"blocked":False}
cfg=json.loads((ROOT/"config.json").read_text())
loc={"address":{"address1":"台北市座標點#foodradar","address2":"","aptOrSuite":"","city":"台北市","country":"TW","postalCode":"100","region":""},"latitude":cfg["latitude"],"longitude":cfg["longitude"],"reference":"台北市座標點#foodradar","referenceType":"google_places","type":"google_places"}
def log(tag,data):
 print(tag+"="+json.dumps(data,ensure_ascii=False,default=str),flush=True)
def rows(groups):
 return [i for g in groups or [] if isinstance(g,dict) for i in ((g.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or [] if isinstance(i,dict)]
with sync_playwright() as pw:
 browser=pw.chromium.launch(channel="chrome",headless=True)
 context=browser.new_context(locale="zh-TW",timezone_id="Asia/Taipei",viewport={"width":1280,"height":900})
 # Same delivery location as the preceding read-only comparison; no login.
 context.add_cookies([{"name":"uev2.loc","value":urllib.parse.quote(json.dumps(loc,ensure_ascii=False)),"domain":".ubereats.com","path":"/"}])
 page=context.new_page()
 metadata={}
 def received(response):
  if not re.search(r"/api/(getStoreV1|getCatalogPresentationV2|getInStoreSearchV1)",response.url): return
  req=response.request
  try: payload=req.post_data_json or {}
  except Exception: payload={}
  try: body=response.json()
  except Exception: body={}
  data=body.get("data") if isinstance(body,dict) else {}
  data=data if isinstance(data,dict) else {}
  filters=payload.get("storeFilters") or {}
  groups=data.get("catalog") or []
  if not isinstance(groups,list): groups=[]
  items=rows(groups)
  paging=data.get("pagingInfo") or {}
  r={"api":response.url.split("/api/")[-1].split("?")[0],"http":response.status,"code":data.get("code"),
     "offset":(payload.get("pagingInfo") or {}).get("offset"),"next_offset":paging.get("offset"),
     "has_more":paging.get("hasMore"),"sections":filters.get("sectionUuids"),
     "subsections":filters.get("subsectionUuids"),"segmented":filters.get("shouldReturnSegmentedControlData"),
     "unique_skus":len({i.get("uuid") for i in items if i.get("uuid")}),
     "keys":list(data)}
  report["network"].append(r)
  log("BROWSER_RPC",r)
  if response.status in (403,429) or str(data.get("code")) in ("403","429"):
   report["blocked"]=True
  if r["api"]=="getStoreV1":
   metadata.update(data)
  (out/f"rpc-{len(report['network']):04d}.json").write_text(json.dumps({"request":payload,"response":body},ensure_ascii=False),encoding="utf8")
 page.on("response",received)
 def snapshot(label):
  body=page.locator("body").inner_text(timeout=6000)
  links=page.locator("a[href]").evaluate_all("""els => els.map(e=>({text:(e.innerText||e.getAttribute('aria-label')||'').trim().slice(0,100),href:e.getAttribute('href')})).filter(x=>x.href&&(/\\/store\\/|\\/category\\/|\\/aisle\\//.test(x.href)))""")
  buttons=page.locator("button,[role=button],[role=tab]").evaluate_all("""els => els.filter(e=>e.getBoundingClientRect().width&&e.getBoundingClientRect().height).map(e=>(e.innerText||e.getAttribute('aria-label')||'').trim().slice(0,90)).filter(Boolean).slice(0,120)""")
  result={"label":label,"url":page.url,"title":page.title(),"body_excerpt":body[:1200],"links":links[:160],"buttons":buttons}
  (out/(label+".txt")).write_text(body,encoding="utf8")
  page.screenshot(path=str(out/(label+".png")),timeout=12000)
  report["visits"].append(result)
  log("UI_STATE",{"label":label,"url":page.url,"title":result["title"],"excerpt":body[:450],"category_links":len(links),"buttons":buttons[:50]})
  return result
 def click_target(uid, labels):
  if report["blocked"]:return None
  # Use actual visible DOM link, never synthesize a category URL.
  for selector in (f'a[href*="{uid}"]',f'[data-testid*="{uid}"]'):
   if not uid:continue
   matches=page.locator(selector)
   for i in range(min(matches.count(),12)):
    el=matches.nth(i)
    if el.is_visible():
     text=(el.inner_text(timeout=3000) or el.get_attribute("aria-label") or "").strip()
     el.click(timeout=7000)
     return {"uuid":uid,"clicked":text,"selector":selector}
  for label in labels:
   matches=page.get_by_text(label,exact=True)
   for i in range(min(matches.count(),12)):
    el=matches.nth(i)
    if el.is_visible():
     el.click(timeout=7000)
     return {"uuid":uid,"clicked":label,"selector":"visible exact text"}
  return None
 def scroll_test(label,steps=36):
  begin=len(report["network"])
  before=page.evaluate("document.body.scrollHeight")
  stagnant=0
  last=None
  for n in range(steps):
   if report["blocked"]:break
   page.mouse.move(1030,760)
   page.mouse.wheel(0,1900)
   page.wait_for_timeout(750)
   state=page.evaluate("({y:window.scrollY,h:document.body.scrollHeight,t:document.body.innerText.length})")
   signature=(state["y"],state["h"],state["t"],len(report["network"]))
   stagnant=stagnant+1 if signature==last else 0
   last=signature
   if stagnant>=4:break
  after=page.evaluate("document.body.scrollHeight")
  info={"label":label,"scrolls":n+1,"initial_height":before,"final_height":after,"new_rpc":len(report["network"])-begin}
  log("SCROLL_RESULT",info)
  report.setdefault("scrolls",[]).append(info)
 try:
  resp=page.goto(case["url"],wait_until="domcontentloaded",timeout=45000)
  report["initial_http"]=resp.status if resp else None
  page.wait_for_timeout(3500)
  if resp and resp.status in (403,429):report["blocked"]=True
  state=snapshot("homepage")
  if re.search(r"access denied|verify you are human|captcha",state["body_excerpt"],re.I):report["blocked"]=True
  if not report["blocked"]:
   # Find the native parent label if the home page's own store response supplied it.
   for idx,uid in enumerate(case["parents"]):
    if report["blocked"]:break
    if idx:
     page.goto(case["url"],wait_until="domcontentloaded",timeout=45000)
     page.wait_for_timeout(1800)
    labels=[]
    for entries in (metadata.get("aisles") or {}).values():
     if not isinstance(entries,list):continue
     for entry in entries:
      if isinstance(entry,dict) and entry.get("catalogSectionUUID")==uid:
       labels.extend(str(entry.get(k)) for k in ("title","name","label") if isinstance(entry.get(k),str))
    labels.extend([case["labels"][idx]])
    chosen=click_target(uid,labels)
    log("CATEGORY_CLICK",{"target":uid,"result":chosen})
    if not chosen:continue
    page.wait_for_timeout(1800)
    snapshot(f"category-{idx}-before")
    scroll_test(f"parent-{idx}")
    snapshot(f"category-{idx}-after")
    if report["blocked"]:break
    # Return to top to see actual category navigation controls.
    page.mouse.wheel(0,-100000)
    page.wait_for_timeout(800)
    chosen_child=click_target(case["child"],[])
    log("CHILD_CLICK",{"target":case["child"],"result":chosen_child})
    if chosen_child:
     page.wait_for_timeout(1500)
     snapshot(f"child-{idx}-before")
     scroll_test(f"child-{idx}",50)
     snapshot(f"child-{idx}-after")
 except (PWError,ValueError,KeyError) as exc:
  report["error"]=str(exc)
 finally:
  report["rpc_count"]=len(report["network"])
  report["deep_requests"]=[x for x in report["network"] if isinstance(x["offset"],int) and x["offset"]>=1000]
  report["full_store_proven"]=False
  (out/"report.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf8")
  log("BROWSER_FINAL",{"store":report["store"],"initial_http":report.get("initial_http"),"blocked":report["blocked"],"error":report.get("error"),"rpc_count":report["rpc_count"],"deep_requests":report["deep_requests"],"scrolls":report.get("scrolls",[]),"visits":[{"label":v["label"],"url":v["url"],"title":v["title"]} for v in report["visits"]],"full_store_proven":False})
  browser.close()
