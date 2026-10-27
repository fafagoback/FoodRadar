"""getStoreV1 must see the same delivery coordinate as the store feed."""
import json
import sys
import unittest
import urllib.parse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from menu_core import attach_delivery_location


class DeliveryLocationTests(unittest.TestCase):
    def test_location_cookie_uses_real_discovery_point(self):
        session = requests.Session()
        found = {"discovered_points": [{"lat": 25.123246847935267,
                                         "lon": 121.52951826298867,
                                         "county": "台北市"}]}
        self.assertTrue(attach_delivery_location(session, found))
        encoded = session.cookies.get("uev2.loc", domain=".ubereats.com")
        self.assertTrue(encoded)
        location = json.loads(urllib.parse.unquote(encoded))
        self.assertEqual(location["latitude"], found["discovered_points"][0]["lat"])
        self.assertEqual(location["longitude"], found["discovered_points"][0]["lon"])
        self.assertEqual(location["referenceType"], "google_places")

    def test_missing_delivery_location_does_not_fabricate_open_status(self):
        session = requests.Session()
        self.assertFalse(attach_delivery_location(session, {}))
        self.assertIsNone(session.cookies.get("uev2.loc"))


if __name__ == "__main__":
    unittest.main()
