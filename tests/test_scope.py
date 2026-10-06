import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from crawl import distance_km, nearby


class ScopeTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(latitude=25.123246847935267, longitude=121.52951826298867, radius_km=3)

    def test_distance_and_radius(self):
        self.assertEqual(distance_km(25, 121, 25, 121), 0)
        self.assertAlmostEqual(distance_km(0, 0, 0, 1), 111.195, places=2)
        self.assertIsNotNone(nearby(dict(store_lat=25.123, store_lon=121.529), self.config))
        self.assertIsNone(nearby(dict(store_lat=22.6, store_lon=120.3), self.config))

    def test_unknown_and_invalid_excluded(self):
        for lat in (None, 'invalid', float('nan'), 100):
            self.assertIsNone(nearby(dict(store_lat=lat, store_lon=121), self.config))

    def test_menu_coordinates_authoritative(self):
        store = dict(store_lat=25.123, store_lon=121.529)
        self.assertIsNone(nearby(store, self.config, dict(geo=dict(latitude=22.6, longitude=120.3))))


if __name__ == '__main__':
    unittest.main()
