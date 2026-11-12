"""Direct live request at offset 1020 for the *currently* qualifying PX Mart.

No speculation based on offset=960, no database/HF writes.
Capture observed app failure or actual item/paging data from Uber's response.
Only this known big-store section is queried (0, 960, 1020, optionally 1080).
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import requests

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from menu_core import attach_delivery_location, global_limiter
from catalog_complete import _catalog_entries, _item_count, _catalog_skus

STORE="db0e33e1-2eb6-5ac6-954d-fdedda8fe7b4"
SECTION="71498aa4-69d8-58ec-87cf-8d9ffd75d6e0"
NAME="全聯福利中心 北投明德店"
HEAD={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
    "Accept":"*/*", "Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
    "Content-Type":"application/json","x-csrf-token":"x",
}
s=requests.Session()
assert attach_delivery_location(s,{"discovered_points":[{
    "lat":25.115, "lon":121.520, "county":"台北市"}]})
results=[]
def probe(offset):
    payload={
        "sortAndFilters":None,
        "storeFilters":{"storeUuid":STORE,"sectionUuids":[SECTION],
                        "subsectionUuids":None},
        "pagingInfo":{"enabled":True,"offset":offset},
        "source":"NV_L2_CATALOG",
    }
    global_limiter.wait()
    response=s.post("https://www.ubereats.com/api/getCatalogPresentationV2",
                    json=payload,headers=HEAD,timeout=30)
    result={"store":NAME,"store_uuid":STORE,"section_uuid":SECTION,
            "requested_offset":offset,"requested_at_utc":datetime.now(timezone.utc).isoformat(),
            "http_status":response.status_code,"response_bytes":len(response.content)}
    try:
        body=response.json()
        data=body.get("data") or {} if isinstance(body,dict) else {}
        result.update(application_status=body.get("status") if isinstance(body,dict) else None,
                      application_code=data.get("code") if isinstance(data,dict) else None,
                      application_message=str(data.get("message") or "")[:300] if isinstance(data,dict) else None,
                      data_keys=list(data)[:25] if isinstance(data,dict) else [])
        try:
            parsed, entries=_catalog_entries(body)
            result.update(valid_catalog=True,items_in_payload=_item_count(entries),
                          unique_skus_in_payload=len(_catalog_skus([body])),
                          hasMore=parsed["pagingInfo"]["hasMore"],
                          next_offset=parsed["pagingInfo"].get("offset"))
        except (ValueError,TypeError) as exc:
            result.update(valid_catalog=False,parse_error=f"{type(exc).__name__}: {exc}")
    except (ValueError,TypeError) as exc:
        result.update(valid_json=False,json_error=f"{type(exc).__name__}: {exc}")
    print("FOODRADAR_DIRECT_OFFSET_1020=" +
          json.dumps(result,ensure_ascii=False),flush=True)
    results.append(result)
    return result

try:
    zero=probe(0)
    before=probe(960)
    if not (before.get("valid_catalog") and before.get("hasMore") is True
            and before.get("next_offset")==1020
            and before.get("items_in_payload",0)>0):
        raise SystemExit("CURRENT API does not show a valid next page at 1020; test not qualified")
    target=probe(1020)
    if target.get("valid_catalog") and target.get("hasMore") is True and target.get("next_offset")==1080:
        probe(1080)
    report={
        "store":NAME, "currently_confirmed_has_1020_page":True,
        "actually_requested_offset_1020":True,
        "direct_1020_valid_catalog":bool(target.get("valid_catalog")),
        "direct_1020_skus":target.get("unique_skus_in_payload"),
        "direct_1020_http":target.get("http_status"),
        "direct_1020_app_status":target.get("application_status"),
        "direct_1020_app_code":target.get("application_code"),
        "offsets_requested":[r["requested_offset"] for r in results],
        "probe_results":results,
        "HF_write":False,"Turso_write":False,
    }
    print("FOODRADAR_DIRECT_OFFSET_1020_SUMMARY=" +
          json.dumps(report,ensure_ascii=False),flush=True)
    import os
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"],"a",encoding="utf-8") as f:
            f.write("### 全聯北投明德店：真正請求 offset=1020\n\n")
            f.write(f"- **當前 offset=960 下一頁確為 1020：是**\n")
            f.write(f"- **直接發出 1020 請求：是**\n")
            f.write(f"- **1020 是否取得合法商品資料：{target.get('valid_catalog')}**\n")
            f.write(f"- **1020 回傳商品 UUID：{target.get('unique_skus_in_payload')}**\n")
            f.write(f"- **HTTP / 應用狀態：{target.get('http_status')} / {target.get('application_status')} / {target.get('application_code')}**\n")
            f.write(f"- **商品錯誤：{target.get('parse_error',target.get('application_message'))}**\n")
except Exception as exc:
    print("FOODRADAR_DIRECT_OFFSET_1020_EXCEPTION="+json.dumps({
        "error":f"{type(exc).__name__}: {exc}","results":results},ensure_ascii=False),flush=True)
    raise
finally:
    s.close()
