"""Benchmark a compact state representation, no production DB/cache mutations.

All retained fields round-trip; immutable historical source hashes and redundant
event JSON stay in HF Raw. This is a candidate format, not a release.
"""
import hashlib
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
import uuid
import msgpack
import zstandard as zstd

source = Path(sys.argv[1]).resolve()
meta = json.loads((source.parent/"manifest.json").read_text())
with source.open("rb") as f:
    assert hashlib.file_digest(f,"sha256").hexdigest()==meta["sqlite_sha256"]
db=sqlite3.connect(source.as_uri()+"?mode=ro",uri=True)
db.row_factory=sqlite3.Row
db.execute("PRAGMA query_only=ON")
def scalar(sql):
    return db.execute(sql).fetchone()[0]
def pk(s):
    try:
        return uuid.UUID(s).bytes if str(uuid.UUID(s))==s else s
    except (ValueError,AttributeError,TypeError):
        return s
def unpk(v):
    return str(uuid.UUID(bytes=v)) if isinstance(v,bytes) else v
compressor=zstd.ZstdCompressor(level=10,write_content_size=True)
decompressor=zstd.ZstdDecompressor()
def packed(v):
    raw=msgpack.packb(v,use_bin_type=True)
    compressed=compressor.compress(raw)
    assert decompressor.decompress(compressed)==raw
    return compressed
def unpack(v):
    return msgpack.unpackb(decompressor.decompress(v),raw=False,strict_map_key=False)
report={"target_bytes":20_000_000,"original_bytes":source.stat().st_size,
        "batch":meta["batch_id"],"production_ready":False,"quote_keys":"store-local product ordinal"}
start=time.perf_counter()
out=Path("compact-candidate.db")
dst=sqlite3.connect(out)
dst.executescript("""PRAGMA page_size=1024;
PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;""")
# Global string dictionaries reduce duplication of menus, URLs, names and
# categories across different branches of a chain. UUIDs use binary form.
dicts=defaultdict(dict)
def token(kind,val):
    if val is None:
        return None
    if not isinstance(val,str):
        return val
    d=dicts[kind]
    if val not in d:
        d[val]=len(d)
    return d[val]
def catalog(raw):
    value=json.loads(raw or "{}")
    if not isinstance(value,dict):
        raise ValueError("unexpected catalog data")
    encoded=[]
    for key,val in sorted(value.items()):
        kid=token("catalog_key",key)
        # Every value, including lists/bools, round-trips from JSON.
        encoded.append([kid,token("catalog:"+key,json.dumps(val,ensure_ascii=False,separators=(",",":")))])
    return encoded
def encode_field(col,v):
    if col.endswith("_uuid"):
        return pk(v)
    if col=="catalog_web":
        return catalog(v)
    return token(col,v)
def decode_field(col,v,original):
    if col.endswith("_uuid"):
        return unpk(v)
    if col=="catalog_web":
        return {reverse["catalog_key"][k]:json.loads(reverse["catalog:"+reverse["catalog_key"][k]][i]) for k,i in v}
    return reverse[col][v] if isinstance(original,str) else v
store_cols=[r[1] for r in db.execute("PRAGMA table_info(stores)") if r[1] not in ("store_uuid","state_hash")]
# Keep every other field, including legacy numerical projections, in the test.
# Rebuild state_hash from canonical fields, menu_quote_history from canonical
# quotes, and source_hash by immutable HF Raw lookup instead of duplicating them.
excluded=("store_uuid","state_hash","menu_quote_history","price_evidence_source_hash")
product_cols=[r[1] for r in db.execute("PRAGMA table_info(products)") if r[1] not in excluded]
report["omitted_product_columns"]=list(excluded)
report["omitted_event_rows"]=scalar("SELECT count(*) FROM events")
# Preserve exact observation timestamp, batch ID, and all monetary/promo data.
batch_keys=list(db.execute("SELECT DISTINCT batch_id,observed_at FROM product_price_observations ORDER BY observed_at,batch_id"))
batch_keys=[tuple(r) for r in batch_keys]
batch_index={b:i for i,b in enumerate(batch_keys)}
mask_width=(len(batch_keys)+7)//8
kinds={"NONE":0,"SAME_SKU_BXGY":1,"DIRECT_DISCOUNT":2}
def qvalue(r):
    return (r["menu_price_cents"],kinds[r["promotion_type"]],
        r["buy_quantity"],r["free_quantity"],r["received_quantity"],
        r["original_total_cents"],r["payable_total_cents"])
def quote_payload(rows, product_index, include_hash=False):
    groups=defaultdict(dict)
    for r in rows:
        key=r["product_uuid"]
        vals=qvalue(r)
        if include_hash:
            vals=vals+(bytes.fromhex(r["source_hash"]),)
        pos=batch_index[(r["batch_id"],r["observed_at"])]
        masks=groups[key]
        masks[vals]=masks.get(vals,0)|(1<<pos)
    return [[product_index[pid],[[list(v),mask.to_bytes(mask_width,"little")] for v,mask in vals.items()]]
        for pid,vals in sorted(groups.items())]
def verify_quotes(payload,rows,product_ids):
    reconstructed={}
    for pid,values in payload:
        for value,mask in values:
            bits=int.from_bytes(mask,"little")
            while bits:
                least=bits&-bits
                pos=least.bit_length()-1
                key=(product_ids[pid],pos)
                assert key not in reconstructed
                reconstructed[key]=tuple(value)
                bits^=least
    expected={(r["product_uuid"],batch_index[(r["batch_id"],r["observed_at"])]):qvalue(r) for r in rows}
    assert reconstructed==expected
counts={"stores":0,"products":0,"quotes":0}
sizes={"shop_payload":0,"price_payload":0,"price_payload_with_all_hashes":0}
max_shop_bytes=0
max_price_bytes=0
sids=[r[0] for r in db.execute("SELECT store_uuid FROM stores ORDER BY store_uuid")]
for sid in sids:
    store=db.execute("SELECT * FROM stores WHERE store_uuid=?",(sid,)).fetchone()
    products=list(db.execute("SELECT * FROM products WHERE store_uuid=? ORDER BY product_uuid",(sid,)))
    quotes=list(db.execute("SELECT * FROM product_price_observations WHERE store_uuid=? ORDER BY product_uuid,observed_at,batch_id",(sid,)))
    state=[[encode_field(c,store[c]) for c in store_cols],
           [[encode_field(c,p[c]) for c in product_cols] for p in products]]
    sh=packed(state)
    product_ids=[p["product_uuid"] for p in products]
    product_index={pid:i for i,pid in enumerate(product_ids)}
    price=quote_payload(quotes,product_index)
    ph=packed(price)
    verify_quotes(unpack(ph),quotes,product_ids)
    # Benchmark the additional cost of retaining each SHA locally rather than
    # locating it in authoritative Raw through (store, product, batch).
    with_hash=packed(quote_payload(quotes,product_index,True))
    dst.execute("INSERT INTO shop_state VALUES(?,?)",(pk(sid),sh))
    dst.execute("INSERT INTO price_history VALUES(?,?)",(pk(sid),ph))
    counts["stores"]+=1
    counts["products"]+=len(products)
    counts["quotes"]+=len(quotes)
    sizes["shop_payload"]+=len(sh)
    sizes["price_payload"]+=len(ph)
    sizes["price_payload_with_all_hashes"]+=len(with_hash)
    max_shop_bytes=max(max_shop_bytes,len(sh))
    max_price_bytes=max(max_price_bytes,len(ph))
# No observation can silently disappear (including orphan quote records).
assert counts["products"]==scalar("SELECT count(*) FROM products")
assert counts["quotes"]==scalar("SELECT count(*) FROM product_price_observations")
reverse={k:list(d.keys()) for k,d in dicts.items()}
for k,values in reverse.items():
    dst.execute("INSERT INTO dictionary VALUES(?,?)",(k,packed(values)))
for k,values in {
    "__store_columns":store_cols,"__product_columns":product_cols,
    "__batch_keys":batch_keys,
    "__crawl_batches":[list(r) for r in db.execute("SELECT * FROM crawl_batches")],
    "__metadata":[list(r) for r in db.execute("SELECT * FROM metadata")]
}.items():
    dst.execute("INSERT INTO dictionary VALUES(?,?)",(k,packed(values)))
dst.commit()
assert dst.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
# Read every shop and compare every retained field to the old database.
t=time.perf_counter()
for sid in sids:
    sh=unpack(dst.execute("SELECT payload FROM shop_state WHERE store=?",(pk(sid),)).fetchone()[0])
    store=db.execute("SELECT * FROM stores WHERE store_uuid=?",(sid,)).fetchone()
    products=list(db.execute("SELECT * FROM products WHERE store_uuid=? ORDER BY product_uuid",(sid,)))
    assert len(sh[1])==len(products)
    for values,original,cols in [(sh[0],store,store_cols)]+[(v,p,product_cols) for v,p in zip(sh[1],products)]:
        for col,code in zip(cols,values):
            original_value=original[col]
            if col=="catalog_web":
                original_value=json.loads(original_value or "{}")
            assert decode_field(col,code,original[col])==original_value,(col,sid)
report["verified_retained_fields_seconds"]=round(time.perf_counter()-t,3)
report["counts"]=counts
report["payload_bytes"]=sizes
report["max_shop_payload_bytes"]=max_shop_bytes
report["max_price_payload_bytes"]=max_price_bytes
report["dictionary_entries"]={k:len(v) for k,v in reverse.items()}
report["dictionary_payload_bytes"]=dst.execute("SELECT sum(length(payload)) FROM dictionary").fetchone()[0]
report["objects"]=dict(dst.execute("SELECT name,sum(pgsize) FROM dbstat GROUP BY name ORDER BY 2 DESC"))
report["candidate_sqlite_bytes"]=out.stat().st_size
report["within_20mb"]=out.stat().st_size<=report["target_bytes"]
report["historical_hashes_not_in_candidate"]=True
report["excluded_events_not_in_candidate"]=True
report["total_seconds"]=round(time.perf_counter()-start,3)
# Benchmark all store payload reads; product lookup can use the store primary
# key plus its sorted UUID list. This is not a web latency benchmark.
t=time.perf_counter()
sample=sids[::max(1,len(sids)//500)][:500]
for sid in sample:
    for table in ("shop_state","price_history"):
        unpack(dst.execute(f"SELECT payload FROM {table} WHERE store=?",(pk(sid),)).fetchone()[0])
report["store_lookup_count"]=len(sample)
report["store_lookup_seconds"]=round(time.perf_counter()-t,6)
text=json.dumps(report,ensure_ascii=False,indent=2)
Path("db-budget-report.json").write_text(text,encoding="utf-8")
print("DB_COMPACT_REPORT="+json.dumps(report,ensure_ascii=False),flush=True)
if os.environ.get("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"],"a",encoding="utf-8") as f:
        f.write("## 精簡格式實測（尚非正式上線）\n\n")
        f.write("完整保留現有商品欄位與全部數值價格觀測；原始歷史證據與冗餘異動 JSON 不放在此候選 DB。\n\n")
        f.write("候選 SQLite：**%.3f MB**\n\n"%(out.stat().st_size/1e6))
        f.write("\x60\x60\x60json\n"+text+"\n\x60\x60\x60\n")
