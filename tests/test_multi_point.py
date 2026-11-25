import json,sys,tempfile,unittest,uuid
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"src"))
from discovery_coverage import township_points,merge_store_points
from distributed_crawl import discover
class MultiPointTests(unittest.TestCase):
    def test_polygon_grid_and_island(self):
        doc={"features":[{"properties":{"COUNTYNAME":"澎湖縣","TOWNNAME":"馬公市","TOWNCODE":"10016010"},
            "geometry":{"type":"MultiPolygon","coordinates":[
                [[[119.5,23.5],[119.6,23.5],[119.6,23.6],[119.5,23.6],[119.5,23.5]]],
                [[[119.41,23.41],[119.412,23.41],[119.412,23.412],[119.41,23.412],[119.41,23.41]]]]}}]}
        points=township_points(doc,.04,require_national=False)
        self.assertGreater(len(points),1)
        self.assertTrue(any(p["longitude"]<119.42 for p in points))
        with self.assertRaises(ValueError):township_points(doc,.04)
    def test_discovery_deduplicates_across_points_and_keeps_both(self):
        sid=str(uuid.UUID(int=1))
        points=[{"id":"a","county":"臺北市","latitude":25.1,"longitude":121.5},
                {"id":"b","county":"新北市","latitude":25.0,"longitude":121.5}]
        cfg={"discovery_scope":"custom","discovery_points":points,"max_pages":0}
        def scan(point,max_pages,raw_dir):
            return point["id"],[{"store_uuid":sid,"store_url":"https://example.invalid/store/"+sid,
                "store_lat":25.0,"store_lon":121.5,"discovered_points":[{"point_id":point["id"],
                "lat":point["latitude"],"lon":point["longitude"],"county":point["county"]}]}],1
        with tempfile.TemporaryDirectory() as t,patch.dict("os.environ",{"FR_DISCOVERY_SCOPE":"custom"}),patch("distributed_crawl.scan_single_point",side_effect=scan):
            coverage={};stores,pages=discover(cfg,Path(t),0,coverage)
            self.assertEqual(len(stores),1);self.assertEqual(pages,2)
            self.assertEqual([p["point_id"] for p in stores[sid]["discovered_points"]],["b","a"])
            self.assertEqual(len(coverage["reports"]),2)
