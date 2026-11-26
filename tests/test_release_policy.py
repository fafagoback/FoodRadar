import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from release_policy import failure_budget_allows, validate_partial_manifest


class StoreFailurePolicy(unittest.TestCase):
    def test_exact_boundary_no_minimum_or_rounding(self):
        for failed,total,allowed in [(292,50644,True),(5,100,True),(6,100,False),
              (1,20,True),(1,19,False),(0,1,True),(1,1,False),(51,1000,False)]:
            self.assertEqual(failure_budget_allows(failed,total),allowed)

    def test_any_individual_failure_reason(self):
        for error in ["HTTP 403","HTTP 429","Timeout","Refreshed native category roster changed", "unknown error"]:
            stores=[{"store_uuid":str(i),"success":i!=0,"error":error} for i in range(20)]
            meta={"stores":stores,"feed_pages":1,"successful_store_count":19,
                  "failed_store_count":1,"failed_store_uuids":["0"]}
            self.assertEqual(len(validate_partial_manifest(meta)[1]),1)
