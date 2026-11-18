import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from repair_hf_batch import paths,matching_anomaly
class RepairTest(unittest.TestCase):
    def test_paths(self):
        self.assertEqual(paths("20261113110657"),["Raw/20261113110657/manifest.json","Raw/20261113110657/capture.tar.gz"])
        with self.assertRaises(ValueError): paths("../oops")
    def test_evidence(self):
        def doc(price=229,store="優市 北投店"):
            return {"name":store,"hasMenu":{"hasMenuSection":[{"hasMenuItem":[{"name":"ARIEL抗菌洗衣精800g瓶裝-高效除蟎","offers":{"price":price}}]}]}}
        self.assertEqual(len(matching_anomaly([doc()])),1)
        self.assertFalse(matching_anomaly([doc(price=89)]))
        self.assertFalse(matching_anomaly([doc(store="中山店")]))
