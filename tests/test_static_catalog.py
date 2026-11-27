"""Offline migration regression: real SQLite rows -> deterministic static blobs -> Pages."""
import hashlib
import json
import shutil
import os
import subprocess
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import zstandard as zstd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"scripts"))
from build_static_catalog import build, bucket, verify
from prepare_pages import prepare, _paged_manifest, referenced_blobs

BATCH = "20261123063000"
LAST = "2026-11-23T06:30:00+08:00"


def fixture(packed_path, source_path):
    with sqlite3.connect(packed_path) as db:
        db.executescript("""
          CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
          CREATE TABLE store_directory(
            store_id INTEGER PRIMARY KEY, store_uuid TEXT, name TEXT, city TEXT,
            locality TEXT, address TEXT, latitude REAL, longitude REAL, rating REAL,
            review_count INTEGER, order_url TEXT, first_seen TEXT, last_seen TEXT,
            status TEXT, is_open INTEGER);
          CREATE TABLE web_products(
            store_uuid TEXT, product_uuid TEXT, card TEXT, search_text TEXT,
            city TEXT, category TEXT, discount_pct REAL, savings REAL,
            unit_price REAL, rating REAL, new_item INTEGER, promo INTEGER,
            price_drop INTEGER, platform_promo INTEGER, latitude REAL, longitude REAL);
        """)
        meta = {
            "application":"FoodRadar", "schema_version":2, "latest_batch":BATCH,
            "latest_processed_at":LAST, "baseline_processed_at":"2026-11-10T00:00:00+08:00",
            "data_revision":"a"*64, "source_counts":{"stores":2,"products":2}}
        db.executemany("INSERT INTO metadata VALUES(?,?)",
                       [(key,json.dumps(value)) for key,value in meta.items()])
        for idx,sid in enumerate(["s1","s2"]):
            db.execute("INSERT INTO store_directory VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                idx,sid,"雞排店"+str(idx),"台北市","中山區","台北市中山區",25.05,121.52,
                4.5,100,"https://www.ubereats.com/",LAST,LAST,"active",1))
            card = {
                "store_uuid":sid,"product_uuid":"p"+str(idx),"product_name":"雞排",
                "store_name":"雞排店"+str(idx),"category_name":"炸物","price":100,
                "effective_price":50,"eff_price":50,"current_price":50,
                "price_evidence_status":"VERIFIED_MENU","promo_type":"買1送1",
                "quantity":2,"verified_discount_pct":50,"crawled_time":"__CURRENT_BATCH__",
                "latitude":25.05,"longitude":121.52,"is_open":1}
            db.execute("INSERT INTO web_products VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                sid,"p"+str(idx),json.dumps(card,ensure_ascii=False),
                "雞排店"+str(idx)+" 雞排 炸物 台北市", "台北市","炸物",50,50,50,4.5,1,1,1,1,25.05,121.52))
    with sqlite3.connect(source_path) as db:
        db.executescript("""
          CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
          CREATE TABLE events(id INTEGER PRIMARY KEY,store_uuid TEXT,product_uuid TEXT,
            event_time TEXT,event_type TEXT,old_state TEXT,new_state TEXT);
          CREATE TABLE products(store_uuid TEXT,product_uuid TEXT,menu_quote_history TEXT);
        """)
        db.execute("INSERT INTO metadata VALUES('latest_batch',?)",(BATCH,))
        db.execute("INSERT INTO products VALUES(?,?,?)",("s1","p0",json.dumps([
            [LAST,50,100,"買1送1",2]])))
        db.execute("INSERT INTO products VALUES(?,?,?)",("s2","p1","[]"))
        db.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?)",(
            1,"s1","p0",LAST,"PRICE_CHANGED",None,json.dumps({
                "price":100,"effective_price":50,"price_valid":True,"is_open":1,
                "price_evidence_status":"VERIFIED_MENU"})))


class StaticMigrationTests(unittest.TestCase):
    def test_verified_cards_are_deduplicated_and_history_separated(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            packed,source=root/"packed.db",root/"serving.db"
            fixture(packed,source)
            data=root/"data"
            manifest=build(packed,source,data)
            self.assertEqual(manifest["counts"],{"stores":2,"products":2})
            self.assertEqual(manifest["summary"]["deals"],2)
            self.assertEqual(manifest["summary"]["promotions"],2)
            self.assertEqual(verify(data)["batch"],BATCH)
            def decode(h):
                return json.loads(zstd.ZstdDecompressor().decompress(
                    (data/"blobs"/(h+".zst")).read_bytes()))
            cards=decode(manifest["files"]["products"][0])
            self.assertEqual(len(cards),2)
            self.assertEqual(cards[0]["effective_price"],50)
            self.assertEqual(cards[0]["promo_type"],"買1送1")
            self.assertEqual(len(decode(manifest["files"]["catalog"])),2)
            postings=decode(manifest["files"]["search"][str(bucket("b:雞排"))])
            self.assertEqual(postings["b:雞排"],[0,1])
            history=decode(manifest["files"]["history"][str(bucket("s1",128))])
            self.assertEqual(len(history["s1\x1fp0"]),2)
            current_bytes=(data/"current.json").read_bytes()
            other=root/"other"
            self.assertEqual(build(packed,source,other),manifest)
            self.assertEqual((other/"current.json").read_bytes(),current_bytes)
            web=root/"web"
            web.mkdir()
            (web/"index.html").write_text("__FOODRADAR_RELEASE__",encoding="utf-8")
            release=prepare(web, data, commit="cafebabe1234")
            self.assertEqual(release["batch"],BATCH)
            browser_manifest = json.loads((web/"data"/"manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(browser_manifest["schema"],"foodradar-static-v2")
            self.assertEqual(browser_manifest["counts"],{"stores":2,"products":2})
            self.assertEqual(browser_manifest["files"]["browse"]["discounts30:discount_desc"]["total"],2)
            self.assertEqual(browser_manifest["files"]["browse"]["global:rating_desc"]["total"],2)
            self.assertIn("global:rating_desc@台北市",browser_manifest["files"]["browse"])
            self.assertIsInstance(browser_manifest["files"]["catalog"],list,
                                  "Client catalog must be chunked, not a whole-file hash")
            self.assertTrue(all(p.stat().st_size<=2*1024*1024
                                for p in (web/"data"/"blobs").glob("*.zst")))
            self.assertEqual(len(browser_manifest["files"]["catalog"]),1)
            self.assertEqual(len(browser_manifest["files"]["store_catalog"]),1)
            self.assertEqual(browser_manifest["search_buckets"],4096)
            self.assertEqual(browser_manifest["history_buckets"],4096)
            self.assertEqual(browser_manifest["files"]["browse"]["promosPlatform:price_asc"]["total"],2)
            chunks=browser_manifest["files"]["browse"]["global:rating_desc"]["chunks"]
            ids=json.loads(zstd.ZstdDecompressor().decompress(
                (web/"data"/"blobs"/(chunks[0]+".zst")).read_bytes()))
            self.assertEqual(sorted(ids),[0,1])
            self.assertLess((web/"data"/"manifest.json").stat().st_size,5*1024*1024)
            # Crawl now publishes the paged V2 pointer; Pages must accept it directly.
            prepared=root/"v2_hf"
            shutil.copytree(data,prepared)
            converted=_paged_manifest(json.loads((prepared/"current.json").read_text()),prepared)
            (prepared/"current.json").write_text(json.dumps(converted,ensure_ascii=False),
                                                  encoding="utf-8")
            web2=root/"web2"
            web2.mkdir()
            (web2/"index.html").write_text("__FOODRADAR_RELEASE__",encoding="utf-8")
            deploy2=prepare(web2,prepared,commit="01234567abcdef")
            self.assertEqual(deploy2["batch"],BATCH)
            served=json.loads((web2/"data"/"manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(served["schema"],"foodradar-static-v2")
            # A V2-only HF release gains geo shards during Pages preparation.
            # Existing V2 assets must be preserved byte-for-byte and the extra
            # spatial files must be independently addressable.
            self.assertTrue(referenced_blobs(converted).issubset(referenced_blobs(served)))
            self.assertIn("geo", served)
            self.assertEqual(served["geo"]["cell_degrees"], 0.025)
            self.assertEqual(len(served["geo"]["products"]), 1)
            self.assertEqual(len(served["geo"]["stores"]), 1)
            for h in referenced_blobs(served):
                self.assertTrue((web2/"data"/"blobs"/(h+".zst")).exists())
            self.assertEqual(served["lookup_buckets"],4096)
            self.assertEqual(served["history_buckets"],4096)

    def test_native_observations_replace_duplicate_events_and_retain_older_history(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            packed,source=root/"packed.db",root/"serving.db"
            fixture(packed,source)
            with sqlite3.connect(source) as db:
                db.execute("""CREATE TABLE product_price_observations(
                    store_uuid TEXT,product_uuid TEXT,observed_at TEXT,batch_id TEXT,
                    menu_price_cents INTEGER,promotion_type TEXT,buy_quantity INTEGER,
                    free_quantity INTEGER,received_quantity INTEGER,payable_total_cents INTEGER)""")
                db.executemany("INSERT INTO product_price_observations VALUES(?,?,?,?,?,?,?,?,?,?)",[
                    ("s1","p0","2026-11-10T06:30:00+08:00","20261110063000",10000,"NONE",1,0,1,10000),
                    ("s1","p0",LAST,BATCH,10000,"SAME_SKU_BXGY",1,1,2,10000)])
            result=build(packed,source,root/"output")
            h=result["files"]["history"][str(bucket("s1",128))]
            entries=json.loads(zstd.ZstdDecompressor().decompress(
                (root/"output"/"blobs"/(h+".zst")).read_bytes()))["s1\x1fp0"]
            self.assertEqual(len(entries),2)
            self.assertTrue(all(e["event_type"]=="MENU_QUOTE" for e in entries))
            values=[json.loads(e["new_state"])["effective_price"] for e in entries]
            self.assertEqual(values,[100.0,50.0])

    def test_geo_manifest_reproducible_across_python_hash_seeds(self):
        """Identical input must yield identical content-hash references, regardless of hash randomization."""
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            packed, source=root/"packed.db", root/"serving.db"
            fixture(packed, source)
            data=root/"static"
            build(packed, source, data)
            results=[]
            for seed in ("1", "777"):
                web=root/("web-"+seed)
                web.mkdir()
                (web/"index.html").write_text("__FOODRADAR_RELEASE__", encoding="utf-8")
                env=dict(os.environ, PYTHONHASHSEED=seed, GITHUB_SHA="abc123def456")
                subprocess.run([
                    sys.executable, str(ROOT/"scripts"/"prepare_pages.py"),
                    "--staging",str(data),"--web-dir",str(web)
                ], env=env, check=True, capture_output=True, text=True)
                results.append((web/"data"/"manifest.json").read_bytes())
            self.assertEqual(results[0], results[1],
                "Identical FoodRadar V2 search and spatial shards must be byte-for-byte reproducible")

    def test_adaptive_hash_indexes_preserve_all_keys(self):
        from collections import defaultdict
        from unittest.mock import patch
        from prepare_pages import adaptive_index, hash_bucket
        first = "key-0"
        second = next("key-"+str(i) for i in range(1,200000)
                      if hash_bucket("key-"+str(i),4096)==hash_bucket(first,4096)
                      and hash_bucket("key-"+str(i),8192)!=hash_bucket(first,8192))
        groups=defaultdict(dict)
        values={first:"a"*60, second:"b"*60}
        groups[hash_bucket(first,4096)].update(values)
        written=[]
        def writer(value):
            written.append(dict(value))
            return str(len(written)-1)
        with patch("prepare_pages.MAX_DECOMPRESSED_BYTES",100):
            count, index=adaptive_index(groups,writer)
        self.assertEqual(count,8192)
        for key,value in values.items():
            self.assertEqual(written[int(index[str(hash_bucket(key,count))])][key],value)

    def test_spooled_products_keep_chunk_and_search_order(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            packed,source=root/"packed.db",root/"serving.db"
            fixture(packed,source)
            with patch("build_static_catalog.CHUNK_ROWS", 1):
                manifest=build(packed,source,root/"data")
            self.assertEqual(manifest["counts"]["products"],2)
            self.assertEqual(len(manifest["files"]["products"]),2)
            def decode(digest):
                return json.loads(zstd.ZstdDecompressor().decompress(
                    (root/"data"/"blobs"/(digest+".zst")).read_bytes()))
            self.assertEqual([decode(h)[0]["product_uuid"] for h in manifest["files"]["products"]],["p0","p1"])
            self.assertEqual(decode(manifest["files"]["search"][str(bucket("b:雞排"))])["b:雞排"],[0,1])
            self.assertEqual(len(decode(manifest["files"]["catalog"])),2)
            self.assertEqual(list((root/"data").glob("fr-static-*")),[])

    def test_corrupt_blob_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            packed,source=root/"packed.db",root/"serving.db"
            fixture(packed,source)
            data=root/"static"
            manifest=build(packed,source,data)
            blob=data/"blobs"/(manifest["files"]["catalog"]+".zst")
            blob.write_bytes(blob.read_bytes()+b"corrupt")
            with self.assertRaises(ValueError):
                verify(data)


if __name__ == "__main__":
    unittest.main()
