"""Run one REAL Uber Eats Beitou crawl through HF-compatible Raw, SQLite and packed web DB.

This acceptance never writes to HF or production Turso; it verifies that the
exact same capture/ETL/pack code supports the published web fields end-to-end.
Only CI runs on explicit test(live) commits / workflow_dispatch.
"""
import hashlib
import json
import sqlite3
import sys
import tarfile
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from raw_capture import fetch_store_raw, documents_from_hf_raw
from serving_state import apply_snapshot
from build_web_db import build_web_db

SID = "d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42"


def main():
    store = {
        "store_uuid": SID,
        "store_url": ("https://www.ubereats.com/tw/store/"
                      "uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/"
                      "0uUO4WnaXv-V8OsNuuueQg"),
        "discovered_points": [
            {"lat": 25.123246847935267, "lon": 121.52951826298867, "county": "台北市"}
        ]
    }
    batch = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        raw_dir = root / "raw"
        raw_dir.mkdir()
        capture = fetch_store_raw(store, raw_dir)
        if not capture.get("success") or not capture.get("catalog_complete"):
            raise AssertionError(f"Full real catalog failed: {capture.get('error')}")
        responses = {}
        tar_path = root / "archive.tar.gz"
        with tarfile.open(tar_path, "w:gz") as archive:
            for path in sorted(raw_dir.rglob("*.body")):
                name = path.relative_to(raw_dir).as_posix()
                responses[name] = hashlib.sha256(path.read_bytes()).hexdigest()
                archive.add(path, arcname=name)
        manifest = {"batch_id": batch, "complete": True,
                    "stores": [capture], "responses": responses}
        with tarfile.open(tar_path, "r:gz") as archive:
            doc = documents_from_hf_raw(archive, manifest)[0]

        products = [(i["identifier"], i["name"], i.get("catalog_web") or {})
                    for s in doc.get("hasMenu", {}).get("hasMenuSection", [])
                    for i in s.get("hasMenuItem", [])]
        brand = [(pid, name, meta) for pid, name, meta in products
                 if "哈根達斯" in name or "Häagen" in name]
        if len(set(pid for pid, _, _ in brand)) < 10:
            raise AssertionError("Full category API did not return Haagen SKUs")

        state = root / "serving.db"
        conn = sqlite3.connect(state)
        conn.row_factory = sqlite3.Row
        apply_snapshot(conn, [doc], batch)
        joined = conn.execute("SELECT product_uuid,product_name,catalog_web,price_evidence_status "
                              "FROM products WHERE store_uuid=?", (SID,)).fetchall()
        state_brand = [row for row in joined if "哈根達斯" in row["product_name"]
                       or "Häagen" in row["product_name"]]
        if len(state_brand) < 10 or len(state_brand) != len(set(pid for pid, _, _ in brand)):
            raise AssertionError("Native brands lost between HTTP raw and serving DB")
        conn.close()

        packed = root / "packed.db"
        build_web_db(state, packed)
        with sqlite3.connect(packed) as db:
            cards = [json.loads(card) for (card,) in db.execute(
                "SELECT card FROM web_products WHERE store_uuid=?", (SID,))]
            selected = [c for c in cards if "哈根達斯" in c["product_name"]
                        or "Häagen" in c["product_name"]]
        # Brand must still be queryable in the public index when shop is open.
        if doc.get("isOpen") is True and not selected:
            raise AssertionError("Brand in normalized DB but absent from web_products")
        if selected and not any(c.get("display_original_price") for c in selected):
            raise AssertionError("Published web cards lack API's native struck-through price")
        if selected and not any(c.get("image_url") for c in selected):
            raise AssertionError("Published web cards lack native product images")

        result = {
            "batch": batch, "native_responses": len(responses),
            "catalog_pages": len(capture["catalog_responses"]),
            "catalog_item_occurrences": capture["catalog_item_occurrences"],
            "native_brand_unique_skus": len(set(pid for pid, _, _ in brand)),
            "normalized_brand_products": len(state_brand),
            "web_brand_products": len(selected),
            "web_brand_with_platform_list_price": sum(bool(c.get("display_original_price")) for c in selected),
            "web_brand_with_image": sum(bool(c.get("image_url")) for c in selected),
            "web_brand_promos": sum(bool(c.get("verified_discount_pct")) for c in selected),
            "store_open": doc.get("isOpen"),
            "sample": [{k: row.get(k) for k in
                        ("product_name", "price", "display_original_price", "is_available",
                         "is_sold_out", "cart_promo_label")} for row in selected[:5]]
        }
        print("LIVE_CATALOG_E2E=" + json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
