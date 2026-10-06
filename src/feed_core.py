# -*- coding: utf-8 -*-
"""
Uber Eats 全台店家工作節點爬蟲 (Stage 2: Parallel Matrix Worker)
【核心功能】：
1. 讀取分配給本機的分片檔案 (tasks/chunk_X.json)。
2. 逐一針對各掃描點發送 Uber Eats 原生 Feed API (getFeedV1) 探索周邊店家。
3. 採用動態翻頁 (offset, hasMore) 徹底見底採集，並抽取店家名稱、UUID、經緯度座標、評分與標籤 (不爬菜單)。
4. 節點內自動去重，並產出 stores_chunk_X.json 與 stores_chunk_X.csv。
5. 即時回報進度與寫入 GitHub Actions $GITHUB_STEP_SUMMARY。
"""

import os
import sys
import re
import csv
import json
import time
import random
import urllib.parse
import argparse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

TW_TZ = timezone(timedelta(hours=8))

# 確保標準輸出支援 UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass


def append_github_step_summary(markdown_text: str):
    """將 Markdown 內容寫入 GitHub Actions $GITHUB_STEP_SUMMARY"""
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        try:
            with open(summary_file, "a", encoding="utf-8") as f:
                f.write(markdown_text + "\n")
        except Exception as e:
            print(f"⚠️ 寫入 GITHUB_STEP_SUMMARY 失敗: {e}")
    else:
        print(f"\n[Worker Step Summary]\n{markdown_text}\n")


def extract_stores_from_feed(obj, current_store_uuid=None):
    """從 Feed API 回傳之樹狀結構中遞迴抽取所有店家元件"""
    res = []
    if isinstance(obj, dict):
        s_uuid = obj.get('storeUuid') or current_store_uuid
        raw_u = obj.get('uuid')
        if raw_u and len(str(raw_u)) == 36 and not s_uuid:
            s_uuid = str(raw_u)

        action = obj.get('actionUrl', '')
        if action and isinstance(action, str) and '/store/' in action:
            title = ""
            if isinstance(obj.get('title'), dict):
                title = obj.get('title', {}).get('text', '')
            elif isinstance(obj.get('title'), str):
                title = obj.get('title')
            elif 'name' in obj and isinstance(obj.get('name'), str):
                title = obj.get('name')

            # 座標與標記 (mapMarker)
            marker = obj.get('mapMarker', {})
            store_lat = marker.get('latitude')
            store_lon = marker.get('longitude')

            # 評分與評論數
            rating_obj = obj.get('rating', {})
            rating_text = rating_obj.get('text') if isinstance(rating_obj, dict) else ''

            store_payload = obj.get('tracking', {}).get('storePayload', {})
            rating_info = store_payload.get('ratingInfo', {})
            rating_score = rating_info.get('storeRatingScore')
            rating_count = rating_info.get('ratingCount')

            # 標籤與中繼資訊
            meta_list = obj.get('meta', []) or []
            meta_texts = [m.get('text') for m in meta_list if isinstance(m, dict) and m.get('text')]

            # 封面大圖
            img_url = ''
            if obj.get('image', {}).get('items'):
                img_url = obj['image']['items'][0].get('url', '')

            # 萃取 slug 與乾淨 URL
            m = re.match(r'^(?:/tw)?/store/([^/?#]+)/([a-zA-Z0-9_-]+)', action)
            slug = m.group(1) if m else ''
            u_slug = m.group(2) if m else ''
            clean_url = f"https://www.ubereats.com/tw/store/{slug}/{u_slug}" if slug else f"https://www.ubereats.com{action}"

            res.append({
                "store_uuid": s_uuid or u_slug,
                "name": title.strip() if title else "",
                "slug": slug,
                "url_uuid": u_slug,
                "store_url": clean_url,
                "store_lat": store_lat,
                "store_lon": store_lon,
                "rating_text": rating_text,
                "rating_score": round(float(rating_score), 2) if rating_score is not None else None,
                "rating_count": rating_count,
                "meta_tags": meta_texts,
                "image_url": img_url,
                "is_orderable": store_payload.get('isOrderable', True),
                "availability_state": store_payload.get('storeAvailablityState', '')
            })

        for k, v in obj.items():
            res.extend(extract_stores_from_feed(v, s_uuid))
    elif isinstance(obj, list):
        for v in obj:
            res.extend(extract_stores_from_feed(v, current_store_uuid))
    return res


def scan_single_point(point: dict, max_pages: int = 10) -> tuple:
    """針對單一經緯度點位進行動態翻頁掃描"""
    p_id = point["id"]
    lat = point["latitude"]
    lon = point["longitude"]
    county = point.get("county", "台灣")
    addr_ref = f"{county}座標點#{p_id}"

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-csrf-token": "x",
        "Content-Type": "application/json"
    }

    loc_cookie = {
        "address": {
            "address1": addr_ref,
            "address2": "",
            "aptOrSuite": "",
            "city": county,
            "country": "TW",
            "postalCode": "100",
            "region": ""
        },
        "latitude": lat,
        "longitude": lon,
        "reference": addr_ref,
        "referenceType": "google_places",
        "type": "google_places"
    }
    session.cookies.set("uev2.loc", urllib.parse.quote(json.dumps(loc_cookie)), domain=".ubereats.com")

    stores_found = {}
    offset = 0
    page = 1
    has_more = True

    while has_more and (max_pages == 0 or page <= max_pages):
        payload = {
            "userQuery": "",
            "feedProviderType": "LOCATION",
            "feedVersion": 2,
            "targetLocation": {
                "latitude": lat,
                "longitude": lon,
                "reference": addr_ref,
                "referenceType": "google_places",
                "address": {
                    "address1": addr_ref,
                    "city": county,
                    "country": "TW",
                    "postalCode": "100"
                }
            },
            "pageInfo": {"offset": offset}
        }

        page_success = False
        for attempt in range(1, 4):
            try:
                time.sleep(random.uniform(0.1, 0.25))
                resp = session.post("https://www.ubereats.com/api/getFeedV1", json=payload, headers=headers, timeout=12)
                
                if resp.status_code == 429:
                    sleep_s = 3.0 * attempt + random.uniform(1.0, 2.0)
                    time.sleep(sleep_s)
                    continue
                elif resp.status_code != 200:
                    time.sleep(1.0 * attempt)
                    continue

                envelope = resp.json()
                data = envelope.get("data")
                if not isinstance(data, dict) or "feedItems" not in data or not isinstance(data.get("meta"), dict):
                    raise ValueError("invalid feed response")
                feed_items = data.get("feedItems", [])
                meta = data.get("meta", {})
                if not isinstance(meta.get("hasMore"), bool):
                    raise ValueError("missing or invalid feed pagination metadata")
                has_more = meta.get("hasMore", False)
                next_offset = meta.get("offset", offset)

                extracted = extract_stores_from_feed(feed_items)
                for s in extracted:
                    key = s["store_uuid"] or s["store_url"]
                    if not key:
                        continue
                    if key not in stores_found:
                        s_copy = dict(s)
                        s_copy["discovered_points"] = [{
                            "point_id": p_id,
                            "lat": lat,
                            "lon": lon,
                            "county": county
                        }]
                        stores_found[key] = s_copy
                    else:
                        # 補充可能缺失的欄位
                        for f in ["name", "store_lat", "store_lon", "rating_score", "rating_count", "image_url"]:
                            if s.get(f) and not stores_found[key].get(f):
                                stores_found[key][f] = s[f]

                if has_more and (next_offset == offset or len(feed_items) == 0):
                    raise ValueError("feed pagination stalled while hasMore=true")
                if not has_more:
                    has_more = False

                offset = next_offset
                page += 1
                page_success = True
                break
            except Exception as e:
                print(f'Feed page {page}, attempt {attempt}: {type(e).__name__}: {e}', flush=True)
                time.sleep(1.0 * attempt)

        if not page_success:
            raise RuntimeError(f"point {p_id}: page {page} failed after 3 attempts")

    if has_more:
        raise RuntimeError(f"point {p_id}: truncated at max_pages={max_pages}; use 0 for complete discovery")

    return p_id, list(stores_found.values()), page - 1



