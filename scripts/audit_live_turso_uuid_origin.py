"""Read-only live Turso audit of every published store identity and location."""
import json
import math
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from menu_core import canonical_store_uuid
from publish_turso_delta import Remote

LAT, LON = 25.123246847935267, 121.52951826298867


def norm(x):
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(x or "")).casefold())


def distance(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
        a, b, c, d = map(math.radians, [LAT, LON, lat, lon])
        h = math.sin((c-a)/2)**2 + math.cos(a)*math.cos(c)*math.sin((d-b)/2)**2
        return round(6371.0088 * 2 * math.asin(min(1, math.sqrt(h))), 2)
    except (ValueError, TypeError, OverflowError):
        return None


def main():
    remote = Remote(os.environ["TURSO_DATABASE_URL"], os.environ["TURSO_READONLY_TOKEN"])
    try:
        meta = {r["key"]: json.loads(r["value"]) for r in remote.query(
            "SELECT key,value FROM metadata WHERE key IN "
            "('application','latest_batch','source_counts','latest_processed_at')")}
        if meta.get("application") != "FoodRadar":
            raise ValueError("Unexpected database")
        rows = []
        while True:
            batch = remote.query("""SELECT store_id,store_uuid,name,address,city,locality,
                latitude,longitude,order_url,first_seen,last_seen,status,is_open
                FROM store_directory ORDER BY store_id LIMIT 300 OFFSET ?""", (len(rows),))
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < 300:
                break
        groups = defaultdict(list)
        name_geo = defaultdict(list)
        urls = defaultdict(list)
        versions = Counter()
        first_dates = Counter()
        cities = Counter()
        distance_ranges = Counter()
        seen_recent = Counter()
        mismatches = []
        for r in rows:
            uid = str(r["store_uuid"]).lower()
            date = str(r.get("first_seen") or "")[:10]
            first_dates[date] += 1
            seen_recent[str(r.get("last_seen") or "")[:10]] += 1
            cities[str(r.get("city") or "UNKNOWN")] += 1
            try:
                versions[str(__import__("uuid").UUID(uid).version)] += 1
            except ValueError:
                versions["invalid"] += 1
            key = (norm(r["name"]), norm(r["address"]))
            if all(key):
                groups[key].append(uid)
            if key[0] and r["latitude"] is not None and r["longitude"] is not None:
                name_geo[(key[0],round(float(r["latitude"]),5),round(float(r["longitude"]),5))].append(uid)
            url = str(r.get("order_url") or "")
            canonical = canonical_store_uuid("",url)
            if canonical and canonical != uid:
                mismatches.append({"uuid":uid,"canonical_from_url":canonical,
                                   "name":r["name"],"address":r["address"]})
            if canonical:
                urls[canonical].append(uid)
            r["distance_from_scan_km"] = distance(r["latitude"],r["longitude"])
            d = r["distance_from_scan_km"]
            if d is None:
                distance_ranges["missing_coordinates"] += 1
            elif d > 20:
                distance_ranges["over_20km"] += 1
            elif d > 10:
                distance_ranges["10_to_20km"] += 1
            elif d > 5:
                distance_ranges["5_to_10km"] += 1
            elif d > 3:
                distance_ranges["3_to_5km"] += 1
            else:
                distance_ranges["within_3km"] += 1
        def duplicates(source):
            return [sorted(set(ids)) for ids in source.values() if len(set(ids)) > 1]
        same_url = duplicates(urls)
        same_name_addr = duplicates(groups)
        same_name_geo = duplicates(name_geo)
        farthest = sorted(
            (r for r in rows if r["distance_from_scan_km"] is not None),
            key=lambda r: r["distance_from_scan_km"], reverse=True
        )[:15]
        summary = {
            "dataset":"FoodRadar live Turso, read-only",
            "latest_batch":meta.get("latest_batch"),
            "source_counts":meta.get("source_counts"),
            "total_store_rows":len(rows),
            "unique_uuids":len({r["store_uuid"].lower() for r in rows}),
            "duplicate_uuid_rows":len(rows)-len({r["store_uuid"].lower() for r in rows}),
            "status_counts":dict(Counter(str(r["status"]) for r in rows)),
            "store_uuid_version_counts":dict(versions),
            "first_seen_date_counts":dict(sorted(first_dates.items())),
            "last_seen_date_counts":dict(sorted(seen_recent.items())),
            "city_counts":dict(cities.most_common()),
            "distance_from_tianmu":dict(distance_ranges),
            "uuid_mismatch_with_store_url_count":len(mismatches),
            "same_url_canonical_uuid_group_count":len(same_url),
            "same_name_and_address_different_uuid_group_count":len(same_name_addr),
            "same_name_and_coordinates_different_uuid_group_count":len(same_name_geo),
            "uuid_mismatch_samples":mismatches[:25],
            "same_url_group_samples":same_url[:25],
            "name_address_group_samples":same_name_addr[:25],
            "name_geo_group_samples":same_name_geo[:25],
            "farthest_stores":farthest,
            "warning":"Same name/address may indicate an alias or distinct legitimate storefronts; do not delete automatically."
        }
        output = Path("audit-results/foodradar-live-uuid-origin-20261111.json")
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps({"summary":summary,"stores":rows},ensure_ascii=False,indent=2),encoding="utf-8")
        print("FOODRADAR_FULL_UUID_AUDIT="+json.dumps(summary,ensure_ascii=False),flush=True)
        print("FOODRADAR_AUDIT_REPORT_PATH="+str(output),flush=True)
    finally:
        remote.close()


if __name__ == "__main__":
    main()
