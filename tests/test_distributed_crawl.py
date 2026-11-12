"""15-runner partition and runner-16 reconciliation integrity contracts (offline)."""
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from distributed_crawl import shard_for_uuid, merge_shards


class DistributionTests(unittest.TestCase):
    def test_stable_and_exactly_one_shard_for_every_store(self):
        stores = [f"{n:08x}-0000-4000-8000-000000000000" for n in range(509)]
        assignments = [
            [sid for sid in stores if shard_for_uuid(sid, 15) == idx]
            for idx in range(15)]
        self.assertEqual(set().union(*(set(a) for a in assignments)), set(stores))
        self.assertEqual(sum(map(len, assignments)), len(stores))
        self.assertEqual(shard_for_uuid(stores[0], 15),
                         shard_for_uuid(stores[0], 15))

    def make_parts(self, root, stores):
        parts = root / "parts"
        for index in range(15):
            part = parts / f"foodradar-raw-shard-{index}"
            raw = part / "raw"
            raw.mkdir(parents=True)
            assigned = sorted(sid for sid in stores if shard_for_uuid(sid, 15) == index)
            # Feed page is different for each runner; do not overwrite it.
            name = f"feed/shard-{index:02d}/page-0001-attempt-01-status-200.body"
            body = b'{"data":{"feedItems":[],"meta":{"hasMore":false}}}'
            (raw / "feed" / f"shard-{index:02d}").mkdir(parents=True)
            (raw / name).write_bytes(body)
            meta = {
                "format": "foodradar-shard-v1", "run_id": "123", "source_sha": "abc",
                "shard_index": index, "shard_count": 15,
                "public_ip": None, "feed_pages": 1,
                "discovered_store_uuids": sorted(stores),
                "assigned_store_uuids": assigned,
                "stores": [{"store_uuid": sid, "success": False,
                            "error": "Catalog native code 500 at offset 1020"}
                           for sid in assigned],
                "responses": {name: hashlib.sha256(body).hexdigest()},
                "api": {"requests": 0, "rate_limit_events": 0},
            }
            (part / "shard.json").write_text(json.dumps(meta), encoding="utf-8")
        return parts

    def test_missing_shard_fails_before_hf(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = self.make_parts(root, ["11111111-1111-4111-8111-111111111111"])
            (parts / "foodradar-raw-shard-7" / "shard.json").unlink()
            with self.assertRaisesRegex(ValueError, "Missing shard 7"):
                merge_shards({}, parts, root=root, upload_raw=False)

    def test_feed_disagreement_detects_store_omission(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            a, b = "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
            parts = self.make_parts(root, [a, b])
            owner = shard_for_uuid(b, 15)
            path = parts / f"foodradar-raw-shard-{owner}" / "shard.json"
            meta = json.loads(path.read_text())
            meta["discovered_store_uuids"].remove(b)
            meta["assigned_store_uuids"].remove(b)
            meta["stores"] = [s for s in meta["stores"] if s["store_uuid"] != b]
            path.write_text(json.dumps(meta))
            with self.assertRaisesRegex(RuntimeError, "coverage mismatch"):
                merge_shards({}, parts, root=root, upload_raw=False)

    def test_tampered_http_body_fails_before_hf(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = self.make_parts(root, ["11111111-1111-4111-8111-111111111111"])
            p = parts / "foodradar-raw-shard-4" / "raw/feed/shard-04/page-0001-attempt-01-status-200.body"
            p.write_bytes(b'corrupted')
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                merge_shards({}, parts, root=root, upload_raw=False)


if __name__ == "__main__":
    unittest.main()
