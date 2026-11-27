"""Offline regression tests for the signed second-stage work-pool client."""
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from db_task_pool import DBTaskPool, AUDIENCE


class Reply:
    def __init__(self, data, status=200):
        self.data = data
        self.status_code = status
        self.headers = {}
    def json(self):
        return self.data


class FakeSession:
    def __init__(self, replies):
        self.replies=list(replies)
        self.calls=[]
    def post(self, endpoint, **kwargs):
        self.calls.append((endpoint,kwargs))
        return self.replies.pop(0)


class PoolTests(unittest.TestCase):
    def test_seed_uses_verified_crawl_source(self):
        session=FakeSession([Reply({"source_stores":50741,"inserted":50741})])
        pool=DBTaskPool(session)
        pool._token_value="signed"
        pool._token_valid_until=float("inf")
        result=pool.seed_from_crawl("45886126553")
        self.assertEqual(result["inserted"],50741)
        req=session.calls[0][1]
        self.assertEqual(req["json"]["op"],"seed_from_crawl")
        self.assertEqual(req["json"]["args"]["source_attempt"],1)
        self.assertEqual(req["headers"]["X-FR-Workflow-Token"],"signed")

    def test_claim_is_one_store_only(self):
        session=FakeSession([Reply({"claimed":{"store_uuid":"a","source":{"artifact":"one"}},"drained":False})])
        pool=DBTaskPool(session)
        pool._token_value="signed"
        pool._token_valid_until=float("inf")
        result=pool.claim(4,"one")
        self.assertEqual(result["claimed"]["store_uuid"],"a")
        self.assertEqual(session.calls[0][1]["json"]["args"],
                         {"worker":4,"preferred_artifact":"one"})

    def test_finalize_requires_real_artifact_reference(self):
        pool=DBTaskPool(FakeSession([]))
        with self.assertRaises(ValueError):
            pool.finish(1,"store","token","")

    def test_results_are_paged_without_skipping_or_duplicating(self):
        first=[{"id":i,"store_uuid":str(i)} for i in range(1,1001)]
        second=[{"id":1001,"store_uuid":"1001"}]
        session=FakeSession([Reply(first),Reply(second)])
        pool=DBTaskPool(session)
        pool._token_value="signed"
        pool._token_valid_until=float("inf")
        rows=list(pool.results())
        self.assertEqual(len(rows),1001)
        self.assertEqual(session.calls[1][1]["json"]["args"]["after_id"],1000)

    def test_declared_oidc_audience_is_distinct_from_crawler(self):
        self.assertEqual(AUDIENCE,"foodradar:db-pool")


if __name__ == "__main__":
    unittest.main()
