"""Lossless physical checkpoint optimization; unchanged production reader/schema.

Only the compact SQLite file is copied. No relational working DB is created.
Every original MessagePack byte is verified, and an actual random-access
benchmark gates adoption. Rejected candidates are deleted.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import random
import sqlite3
import statistics
import struct
import tempfile
import time
from pathlib import Path

import msgpack
import zstandard as zstd
from compact_templates import Templates, KEY, choose_pool
from compact_payload import PayloadCodec, KEY as PAYLOAD_KEY, MAGIC

TABLES = (("dictionary", "key"), ("shop_state", "store"), ("price_history", "store"))
SCHEMA = """CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;"""


def ro(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)


def digest(path):
    """Exact key + decompressed MessagePack bytes, not lossy field equality."""
    h = hashlib.sha256()
    d = zstd.ZstdDecompressor()
    with ro(path) as db:
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("Compact database integrity failure")
        tables = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != {t for t, _ in TABLES}:
            raise ValueError("Unexpected compact database schema")
        dictionary={k:msgpack.unpackb(d.decompress(v),raw=False,strict_map_key=False)
                    for k,v in db.execute("SELECT key,payload FROM dictionary")}
        templates=Templates(dictionary)
        payload_codec=PayloadCodec(dictionary.get(PAYLOAD_KEY))
        for table, key in TABLES:
            h.update(table.encode("utf8") + b"\0")
            for identity, blob in db.execute(f"SELECT {key},payload FROM {table} ORDER BY {key}"):
                if table=="dictionary" and identity in (KEY,PAYLOAD_KEY):continue
                token = msgpack.packb(identity, use_bin_type=True)
                value = d.decompress(blob) if table=="dictionary" else payload_codec.raw(blob)
                if table=="shop_state" and templates.pool:
                    header,rows=msgpack.unpackb(value,raw=False,strict_map_key=False)
                    value=msgpack.packb([header,templates.rows(rows)],use_bin_type=True)
                h.update(struct.pack(">QQ", len(token), len(value)))
                h.update(token)
                h.update(value)
    return h.hexdigest()


def build(source,target,page_size,levels=(12,16),*,share=False,min_shops=2,train=0):
    d=zstd.ZstdDecompressor()
    codecs=[zstd.ZstdCompressor(level=n,write_content_size=True) for n in levels]
    with ro(source) as old, sqlite3.connect(target) as new:
        dictionary={k:msgpack.unpackb(d.decompress(v),raw=False,strict_map_key=False)
                    for k,v in old.execute("SELECT key,payload FROM dictionary")}
        prior=Templates(dictionary)
        old_codec=PayloadCodec(dictionary.get(PAYLOAD_KEY))
        info={}
        if share:
            pool,info=choose_pool(prior,(old_codec.unpack(v)[1] for v, in
                    old.execute("SELECT payload FROM shop_state")),min_shops=min_shops)
            dictionary[KEY]=pool
        new_templates=Templates(dictionary)
        if train:
            # Deterministic, bounded sample of compact payloads, not full DB expansion.
            sample=[]
            for table in ("shop_state","price_history"):
                total=old.execute("SELECT COUNT(*) FROM "+table).fetchone()[0]
                step=max(1,total//1200)
                for i,(blob,) in enumerate(old.execute("SELECT payload FROM "+table+" ORDER BY store")):
                    if i%step or len(sample)>=2400:continue
                    raw=old_codec.raw(blob)
                    if table=="shop_state" and share:
                        header,rows=msgpack.unpackb(raw,raw=False,strict_map_key=False)
                        raw=msgpack.packb([header,new_templates.encode_rows(prior.rows(rows))],use_bin_type=True)
                    if len(raw)>32:sample.append(raw[:65536])
            if len(sample)<8 or sum(map(len,sample))<train*8:
                return {"training_skipped":True}
            trained=zstd.train_dictionary(train,sample).as_bytes()
            dictionary[PAYLOAD_KEY]=trained
            info["trained_dictionary_bytes"]=len(trained)
        new_codec=PayloadCodec(dictionary.get(PAYLOAD_KEY))
        trained_codecs=[zstd.ZstdCompressor(level=n,dict_data=new_codec.trained,write_content_size=True)
                        for n in levels] if new_codec.trained else []
        new.execute("PRAGMA page_size="+str(page_size))
        new.execute("PRAGMA journal_mode=OFF")
        new.execute("PRAGMA synchronous=OFF")
        new.executescript(SCHEMA)
        for table,key in TABLES:
            def rows():
                for identity,blob in old.execute(f"SELECT {key},payload FROM {table} ORDER BY {key}"):
                    if table=="dictionary" and identity in (KEY,PAYLOAD_KEY):continue
                    raw=d.decompress(blob) if table=="dictionary" else old_codec.raw(blob)
                    changed=False
                    if share and table=="shop_state":
                        header,items=msgpack.unpackb(raw,raw=False,strict_map_key=False)
                        raw=msgpack.packb([header,new_templates.encode_rows(prior.rows(items))],use_bin_type=True)
                        changed=True
                    # Do not copy a dictionary-dependent frame when the dictionary changed.
                    best=min((c.compress(raw) for c in codecs),key=len)
                    if not changed and (not blob.startswith(MAGIC) or old_codec.dictionary==new_codec.dictionary):
                        best=min(best,blob,key=len)
                    if table!="dictionary" and trained_codecs:
                        best=min([best]+[MAGIC+c.compress(raw) for c in trained_codecs],key=len)
                    yield identity,best
            new.executemany(f"INSERT INTO {table} VALUES(?,?)",rows())
        for k in (KEY,PAYLOAD_KEY):
            if dictionary.get(k):
                raw=msgpack.packb(dictionary[k],use_bin_type=True)
                new.execute("INSERT INTO dictionary VALUES(?,?)",(k,min((c.compress(raw) for c in codecs),key=len)))
        new.commit()
        return info


def benchmark(path, repeats=3):
    """Exercise the same physical read/decode operations as CompactStore.

    Measure dictionary initialization, state-only reads (store header/cards),
    price-history reads and the combined per-shop read, in randomized order.
    Full scans and p95 per-shop reads must not become slower.
    """
    d = zstd.ZstdDecompressor()
    def decode(blob):
        return msgpack.unpackb(d.decompress(blob), raw=False, strict_map_key=False)
    with ro(path) as db:
        ids = [x[0] for x in db.execute("SELECT store FROM shop_state ORDER BY store")]
    if not ids:
        return None
    random.Random(416).shuffle(ids)
    opens, scans, state_p95, history_p95, pair_p95 = [], [], [], [], []
    def p95(values):
        return sorted(values)[max(0, math.ceil(len(values) * .95) - 1)]
    for _ in range(repeats):
        t = time.perf_counter()
        with ro(path) as db:
            dictionary = {k: decode(v) for k, v in db.execute("SELECT key,payload FROM dictionary")}
            if not dictionary:
                raise ValueError("Missing compact dictionary")
            templates=Templates(dictionary)
            payload_codec=PayloadCodec(dictionary.get(PAYLOAD_KEY))
            opens.append(time.perf_counter() - t)
            a, b, both = [], [], []
            t = time.perf_counter()
            for sid in ids:
                q = time.perf_counter()
                state = db.execute("SELECT payload FROM shop_state WHERE store=?", (sid,)).fetchone()
                header,rows=payload_codec.unpack(state[0])
                templates.rows(rows)
                middle = time.perf_counter()
                history = db.execute("SELECT payload FROM price_history WHERE store=?", (sid,)).fetchone()
                payload_codec.unpack(history[0])
                end = time.perf_counter()
                a.append(middle - q)
                b.append(end - middle)
                both.append(end - q)
            scans.append(time.perf_counter() - t)
            state_p95.append(p95(a))
            history_p95.append(p95(b))
            pair_p95.append(p95(both))
    return {"open_ms": statistics.median(opens) * 1000,
            "scan_ms": statistics.median(scans) * 1000,
            "state_p95_us": statistics.median(state_p95) * 1e6,
            "history_p95_us": statistics.median(history_p95) * 1e6,
            "pair_p95_us": statistics.median(pair_p95) * 1e6}


def select(path, directory, *, measure=benchmark, candidates=(512, 1024), include_templates=True):
    """Return the best passing file or None. Source remains untouched."""
    path, directory = Path(path), Path(directory)
    expected = digest(path)
    base_size = path.stat().st_size
    baseline = measure(path)
    report = {"before_bytes": base_size, "after_bytes": base_size,
              "baseline": baseline, "exact_sha256": expected, "candidates": []}
    winner = None
    if baseline is None:
        return winner, report
    modes=[(n,False,2,0) for n in candidates]
    if include_templates:
        modes += [(n,True,2,0) for n in candidates]+[(512,True,8,0),(1024,True,8,0)]
    modes += [(512,False,2,65536),(1024,False,2,65536),(512,False,2,131072),(1024,True,8,65536)]
    for size,share,min_shops,train in modes:
        target = directory / f"candidate-{size}-{int(share)}-{min_shops}-{train}.db"
        started = time.perf_counter()
        try:
            info=build(path,target,size,share=share,min_shops=min_shops,train=train)
            if info.get('training_skipped') or (share and not info.get('shared_groups')):
                target.unlink(missing_ok=True);continue
            if digest(target) != expected:
                raise ValueError("Repacked checkpoint changed exact logical bytes")
            n = target.stat().st_size
            metrics = measure(target)
            # No arbitrary tolerance: smaller files are only eligible if
            # every measured read metric is at least as good as baseline.
            passes = bool(metrics and all(metrics[k] <= v for k, v in baseline.items()))
            row = {"page_size": size, "shared_templates":share,"min_shops":min_shops,"train_bytes":train, **info, "bytes": n, "exact": True,
                   "build_seconds": round(time.perf_counter() - started, 3),
                   "metrics": metrics, "read_gate_pass": passes}
            report["candidates"].append(row)
            if passes and n < report["after_bytes"]:
                if winner is not None:
                    winner.unlink()
                winner = target
                report["after_bytes"] = n
                report["selected_page_size"] = size
                report["selected_shared_templates"]=share
                report["selected_train_bytes"]=train
            else:
                target.unlink()
        except Exception:
            target.unlink(missing_ok=True)
            if winner is not None:
                winner.unlink(missing_ok=True)
            raise
    return winner, report


def optimize_part(part):
    """Optimize a flushed live CompactStore and reopen its connection safely."""
    path = part.path
    part.conn.commit()
    # Repack once for an existing physical layout, and again only if the file
    # subsequently grows by >=10% (or its stored marker is absent).
    marker = path.with_suffix(path.suffix + ".physical.json")
    if marker.exists():
        prior = json.loads(marker.read_text())
        if prior.get("version") == 3 and path.stat().st_size <= prior["bytes"] * 1.10:
            return {"skipped": "within_last_verified_size", "bytes": path.stat().st_size}
    with tempfile.TemporaryDirectory(prefix="fr-compact-", dir=path.parent) as folder:
        winner, report = select(path, folder)
        if winner is not None:
            part.conn.close()
            try:
                os.replace(winner, path)
            finally:
                part.conn = sqlite3.connect(path)
                part.conn.execute("PRAGMA journal_mode=MEMORY")
                part.conn.execute("PRAGMA temp_store=MEMORY")
                part.d={k:msgpack.unpackb(zstd.ZstdDecompressor().decompress(v),raw=False,strict_map_key=False)
                        for k,v in part.conn.execute("SELECT key,payload FROM dictionary")}
                part.templates=Templates(part.d)
                part.payload_codec=PayloadCodec(part.d.get(PAYLOAD_KEY))
                part.tokens.clear()
        marker.write_text(json.dumps({"version": 3, "bytes": path.stat().st_size}))
        print("FOODRADAR_PHYSICAL_COMPACTION=" + json.dumps(report), flush=True)
        return report
