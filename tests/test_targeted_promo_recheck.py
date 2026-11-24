import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
from promo_target_recheck import targeted_recheck, plan_target_pages
from test_promo_api_recheck import SID, make_item


def groups(items):
    return [{"payload": {"standardItemsPayload": {"catalogItems": items}}}]


class TargetedRecheckTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.retry = self.root / "promo-recheck" / SID
        self.base = {"data": {"title": "Fixture", "catalogSectionsMap": {}}}
        self.original = {"store_uuid": SID, "response_path": "base.body", "catalog_responses": []}
        (self.root / "base.body").write_text(json.dumps(self.base))
        self.bodies = {}
        for i in range(8):
            path = f"page-{i}.body"
            section = "section-" + str(i)
            payload = {"sortAndFilters": None, "storeFilters": {"storeUuid": SID,
                        "sectionUuids": [section], "subsectionUuids": None},
                       "pagingInfo": {"enabled": True, "offset": 0}, "source": "NV_L2_CATALOG"}
            body = {"data": {"catalog": groups([make_item(i)]), "pagingInfo": {"hasMore": False}}}
            self.bodies[section] = body
            (self.root/path).write_text(json.dumps(body))
            self.original["catalog_responses"].append({"path": path, "section_uuid": section,
                "subsection_uuid": None, "offset": 0, "request_payload": payload})
        self.calls = []

    def fetch(self, session, url, payload, root, tag):
        self.calls.append((url, payload))
        if "getStoreV1" in url:
            body = self.base
        else:
            body = self.bodies[payload["storeFilters"]["sectionUuids"][0]]
        path = tag + "-status-200.body"
        f = root/path
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(body))
        return path, body

    def check(self):
        with patch("promo_target_recheck.attach_delivery_location", return_value=True):
            return targeted_recheck({"store_uuid": SID}, self.root, self.original,
                                    ["item-0002"], self.retry, fetch=self.fetch)

    def test_one_sku_rechecks_two_requests_not_nine(self):
        got = self.check()
        self.assertEqual(got["status"], "MATCHED")
        self.assertEqual(got["mode"], "targeted_sku")
        self.assertEqual(got["second_catalog_pages"], 1)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[1][1]["storeFilters"]["sectionUuids"], ["section-2"])

    def test_different_price_is_conflict(self):
        self.bodies["section-2"]["data"]["catalog"] = groups([make_item(2, price=999)])
        self.assertEqual(self.check()["status"], "CONFLICT")

    def test_moved_sku_requires_full_check(self):
        self.bodies["section-2"]["data"]["catalog"] = groups([make_item(9)])
        self.assertIsNone(self.check())

    def test_unknown_request_provenance_requires_full_check(self):
        del self.original["catalog_responses"][2]["request_payload"]
        self.assertIsNone(plan_target_pages(self.root, self.original, ["item-0002"]))
