import ast
import json
import sys
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from menu_core import convert_api_data_to_schema
class ScopeTests(unittest.TestCase):
    def test_fixed_coordinate_unlimited_feed(self):
        config=json.loads((ROOT/'config.json').read_text(encoding='utf-8'))
        self.assertEqual(config['latitude'],25.123246847935267)
        self.assertEqual(config['longitude'],121.52951826298867)
        self.assertEqual(config['max_pages'],0)
        self.assertNotIn('radius_km',config)
        tree=ast.parse((ROOT/'src/crawl.py').read_text(encoding='utf-8'))
        self.assertFalse(any(isinstance(n,ast.Name) and n.id=='nearby' for n in ast.walk(tree)))
    def test_original_menu_contract(self):
        doc=convert_api_data_to_schema({'title':'Test','catalogSectionsMap':{},'isOpen':False},'https://www.ubereats.com/tw/store/test/aaaaaaaa')
        self.assertEqual(doc['@type'],'Restaurant')
        self.assertIsInstance(doc['hasMenu']['hasMenuSection'],list)
        self.assertNotIn('distance_km',doc)
        self.assertNotIn('batch_id',doc)
if __name__=='__main__':
    unittest.main()
