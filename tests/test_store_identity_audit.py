import base64
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from audit_store_identity import audit_stores
from menu_core import canonical_store_uuid


class StoreIdentityAuditTests(unittest.TestCase):
    def test_uber_url_uuid_overrides_inherited_parent_feed_id(self):
        real = str(uuid.UUID("cbd0f35e-a7cf-515a-b8f6-0f838d084819"))
        slug = base64.urlsafe_b64encode(uuid.UUID(real).bytes).decode().rstrip("=")
        url = "https://www.ubereats.com/tw/store/test/" + slug
        self.assertEqual(canonical_store_uuid(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", url), real)

    def test_history_audit_flags_physical_duplicates_without_mutation(self):
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.executescript("""
            CREATE TABLE stores(
                store_uuid TEXT PRIMARY KEY, name TEXT, address TEXT,
                latitude REAL, longitude REAL, order_url TEXT,
                first_seen TEXT, last_seen TEXT, status TEXT);
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
        """)
        db.execute("INSERT INTO metadata VALUES('latest_batch','20261111101700')")
        rows = [
            ("a", "同一商家", "台北市天母路1號", 25.12345, 121.52951,
             "https://www.ubereats.com/tw/store/first/first-id",
             "2026-11-01T10:00:00+08:00", "2026-11-11T10:17:00+08:00", "active"),
            ("b", "同一商家", "台北市天母路1號", 25.12345, 121.52951,
             "https://www.ubereats.com/tw/store/second/second-id",
             "2026-11-02T10:00:00+08:00", "2026-11-11T10:17:00+08:00", "active"),
            ("c", "其他商家", "台北市天母路2號", 25.12346, 121.52952,
             "https://www.ubereats.com/tw/store/third/third-id",
             "2026-11-03T10:00:00+08:00", "2026-11-10T10:17:00+08:00", "inactive"),
        ]
        db.executemany("INSERT INTO stores VALUES (?,?,?,?,?,?,?,?,?)", rows)
        result = audit_stores(db)
        self.assertEqual(result["unique_store_uuids"], 3)
        self.assertEqual(result["stores_seen_on_latest_date"], 2)
        self.assertEqual(result["active_store_rows"], 2)
        self.assertEqual(result["same_name_address_different_uuid_groups"], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM stores").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
