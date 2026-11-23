import unittest
import requests
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from menu_core import attach_delivery_location, fetch_single_store
from pipeline import assert_useful_price_batch

class PriceReleaseGuards(unittest.TestCase):
    def test_store_call_must_have_feed_delivery_coordinate(self):
        session=requests.Session()
        self.assertFalse(attach_delivery_location(session,{'discovered_points': []}))
        good={'discovered_points':[{'lat':25.123246847935267,'lon':121.52951826298867,'county':'台北市'}]}
        self.assertTrue(attach_delivery_location(session,good))
        self.assertIsNotNone(session.cookies.get('uev2.loc'))

    def test_unlocated_store_never_fetches_untrusted_menu(self):
        bad={'store_url':'https://www.ubereats.com/tw/store/test/ffffffffffffffffffffff',
             'store_uuid':'ffffffff-ffff-4fff-8fff-ffffffffffff','name':'No location'}
        result=fetch_single_store(bad,'/tmp','20261027190000_')
        self.assertEqual(result['status'],'FAILED')
        self.assertIn('delivery coordinate',result['error'])

    def test_complete_closed_batch_is_not_publishable(self):
        docs=[{'isOpen':True,'closedMessage':'Delivery unavailable'} for _ in range(100)]
        with self.assertRaisesRegex(RuntimeError,'0/100 open-hours'):
            assert_useful_price_batch(docs,'20261027183000')

    def test_daytime_mass_delivery_failure_not_publishable(self):
        # Multiple positive observations exist, but 98% responses are
        # geolocation-closed: this is not a trustworthy price release.
        docs=[{'isOpen':True,'closedMessage':'Delivery unavailable'} for _ in range(98)]
        docs += [{'isOpen':True,'workingHoursTagline':'Open until 10 PM',
                  'openingHoursSpecification':[
                    {'dayOfWeek':['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'],
                     'opens':'00:00','closes':'00:00'}],
                  'hasMenu':{'hasMenuSection':[{'hasMenuItem':[{'name':'飯','offers':{'price':100}}]}]}} for _ in range(2)]
        with self.assertRaisesRegex(RuntimeError,'delivery-location'):
            assert_useful_price_batch(docs,'20261027183000')

    def test_normal_live_crawl_passes(self):
        docs=[{'isOpen':True,'workingHoursTagline':'Open until 10 PM',
               'openingHoursSpecification':[
                 {'dayOfWeek':['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'],
                  'opens':'00:00','closes':'00:00'}],
               'hasMenu':{'hasMenuSection':[{'hasMenuItem':[{'name':'飯','offers':{'price':100}}]}]}} for _ in range(504)]
        docs += [{'isOpen':True,'closedMessage':'Delivery unavailable'} for _ in range(78)]
        self.assertEqual(assert_useful_price_batch(docs,'20261027193552'),504)

if __name__=='__main__':
    unittest.main()
