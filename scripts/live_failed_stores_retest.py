"""Read-only live regression against real native API failures from 11/11.

No HF/Turso mutation. A recovered store must reach every category terminal
pagination page, and retain all observed parent UUIDs by real subsection data.
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from raw_capture import fetch_store_raw

CASES=[
    ("0fd40e62-4a52-49a5-8620-0706906896e1",
     "https://www.ubereats.com/tw/store/watsons%E5%B1%88%E8%87%A3%E6%B0%8F-%E5%A4%A9%E5%8C%97%E9%96%80%E5%B8%82-s0454/D9QOYkpSSaWGIAcGkGiW4Q"),
    ("2a2613cb-f67c-5774-935d-bb47511095d0",
     "https://www.ubereats.com/tw/store/%E5%A4%A7%E6%A8%B9%E8%97%A5%E5%B1%80-%E5%A3%AB%E6%9E%97%E6%96%87%E6%9E%97%E5%BA%97/KiYTy_Z8V3STXbtHURCV0A"),
    ("c98b81a0-e46e-4f08-be3d-9e84815e0b9c",
     "https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E8%A1%8C%E7%BE%A9/yYuBoORuTwi-PZ6EgV4LnA"),
    ("0941d15a-f620-5970-b04a-b9edf626cc7f",
     "https://www.ubereats.com/tw/store/%E9%87%91%E5%90%89%E5%88%A9%E5%AF%B5%E7%89%A9%E7%B2%BE%E5%93%81%E5%A4%A9%E6%AF%8D%E5%BA%97/CUHRWvYgWXCwSrnt9ibMfw"),
]

failed=0
with tempfile.TemporaryDirectory(prefix="foodradar-native-subsection-retest-") as tmp:
    root=Path(tmp)
    for store_uuid,store_url in CASES:
        store={"store_uuid":store_uuid,"store_url":store_url,
               "discovered_points":[{"lat":25.123246847935267,
                                     "lon":121.52951826298867,"county":"台北市"}]}
        result=fetch_store_raw(store,root,max_retries=1)
        record={"store_uuid":store_uuid,"success":bool(result.get("success")),
                "catalog_complete":result.get("catalog_complete"),
                "sections":result.get("catalog_section_count"),
                "pages":len(result.get("catalog_responses") or []),
                "occurrences":result.get("catalog_item_occurrences"),
                "error":result.get("error")}
        print("LIVE_CATALOG_RECOVERY_RETEST="+json.dumps(record,ensure_ascii=False),flush=True)
        if not result.get("success") or result.get("catalog_complete") is not True:
            failed+=1
    print("LIVE_CATALOG_RECOVERY_RETEST_SUMMARY="+json.dumps({
        "tested":len(CASES),"failed":failed,
        "raw_response_count":len(list(root.rglob("*.body")))}),flush=True)
if failed:
    raise SystemExit("At least one real previously failing store remains incomplete")
