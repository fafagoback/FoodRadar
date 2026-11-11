"""Evidence-only audit of historical storefront UUIDs; never merges/deletes data."""
import json
import re
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

from menu_core import canonical_store_uuid


def _normalized(value):
    return re.sub(r"\\s+", "", unicodedata.normalize("NFKC", str(value or "")).casefold())


def _url_identity(url):
    path = urlparse(str(url or "")).path.rstrip("/")
    match = re.search(r"/store/[^/]+/([^/]+)$", path)
    return match.group(1) if match else ""


def audit_stores(db):
    """Count unique UUIDs, recent identities, and high-confidence possible duplicates.

    Physical duplicates are only *flagged*: two brands can share an address,
    and multiple Uber IDs may legitimately represent distinct virtual shops.
    """
    db.row_factory = sqlite3.Row
    records = [dict(r) for r in db.execute(
        "SELECT store_uuid,name,address,latitude,longitude,order_url,"
        "first_seen,last_seen,status FROM stores"
    )]
    metadata = dict(db.execute("SELECT key,value FROM metadata"))
    latest = str(metadata.get("latest_batch") or "").strip('"')
    try:
        latest_date = __import__("datetime").datetime.strptime(latest, "%Y%m%d%H%M%S").date().isoformat()
    except ValueError:
        latest_date = ""
    by_uuid = defaultdict(list)
    by_url = defaultdict(list)
    by_name_address = defaultdict(list)
    by_name_geo = defaultdict(list)
    first_seen_dates = Counter()
    for item in records:
        uuid = str(item["store_uuid"]).lower()
        by_uuid[uuid].append(uuid)
        url = _url_identity(item["order_url"])
        if url:
            by_url[url].append(uuid)
        key = (_normalized(item["name"]), _normalized(item["address"]))
        if key[0] and key[1]:
            by_name_address[key].append(uuid)
        if key[0] and item["latitude"] is not None and item["longitude"] is not None:
            coord_key = (key[0], round(float(item["latitude"]), 5), round(float(item["longitude"]), 5))
            by_name_geo[coord_key].append(uuid)
        first_seen_dates[str(item["first_seen"] or "")[:10]] += 1
    def collision_groups(groups):
        return [
            sorted(set(ids)) for ids in groups.values()
            if len(set(ids)) > 1
        ]
    url_duplicates = collision_groups(by_url)
    name_address_duplicates = collision_groups(by_name_address)
    geo_duplicates = collision_groups(by_name_geo)
    url_uuid_conflicts = [
        {"uuid": r["store_uuid"], "url_uuid": canonical_store_uuid("", r["order_url"])}
        for r in records if canonical_store_uuid("", r["order_url"])
        and canonical_store_uuid("", r["order_url"]) != r["store_uuid"]
    ]
    return {
        "total_store_rows": len(records),
        "unique_store_uuids": len(by_uuid),
        "active_store_rows": sum(r["status"] == "active" for r in records),
        "stores_seen_on_latest_date": sum(str(r["last_seen"] or "")[:10] == latest_date for r in records),
        "latest_batch": latest,
        "new_uuids_by_first_seen_date": dict(sorted(first_seen_dates.items())),
        "same_url_different_uuid_groups": len(url_duplicates),
        "same_name_address_different_uuid_groups": len(name_address_duplicates),
        "same_name_coordinates_different_uuid_groups": len(geo_duplicates),
        "url_uuid_conflict_count": len(url_uuid_conflicts),
        "same_url_samples": url_duplicates[:15],
        "same_name_address_samples": name_address_duplicates[:15],
        "same_name_coordinates_samples": geo_duplicates[:15],
        "url_uuid_conflict_samples": url_uuid_conflicts[:15],
        "note": "Possible same-shop groups are audit candidates only, not confirmed duplicates.",
    }


if __name__ == "__main__":
    import sys
    with sqlite3.connect(sys.argv[1]) as db:
        report = audit_stores(db)
    print("FOODRADAR_STORE_HISTORY_AUDIT=" + json.dumps(report, ensure_ascii=False), flush=True)
    if len(sys.argv) > 2:
        Path(sys.argv[2]).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
