"""List the live FoodRadar Turso schema using read-only credentials."""
import os, json, urllib.request, base64
url=os.environ["TURSO_DATABASE_URL"].replace("libsql://","https://").rstrip("/")
token=os.environ["TURSO_READONLY_TOKEN"]
def query(stmt):
    req=urllib.request.Request(url+"/v2/pipeline",
        json.dumps({"requests":[{"type":"execute","stmt":{"sql":stmt}},{"type":"close"}]}).encode(),
        {"Authorization":"Bearer "+token,"Content-Type":"application/json"})
    with urllib.request.urlopen(req,timeout=90) as fh: obj=json.load(fh)
    r=obj["results"][0]
    if r["type"]=="error": raise RuntimeError(r["error"].get("message","SQL error"))
    data=r["response"]["result"]
    names=[v["name"] for v in data["cols"]]
    return [dict(zip(names,[v.get("value") if v["type"]!="blob" else "[BLOB]" for v in row])) for row in data["rows"]]
tables=query("SELECT name, type, sql FROM sqlite_master WHERE type IN ('table','view') ORDER BY name")
print("LIVE_OBJECTS "+json.dumps(tables,ensure_ascii=False),flush=True)
for t in tables:
    name=t["name"]
    if not name.replace("_","").isalnum(): continue
    cols=query('PRAGMA table_info("'+name+'")')
    print("LIVE_COLUMNS "+json.dumps({"table":name,"columns":cols},ensure_ascii=False),flush=True)
    if name not in ("sqlite_sequence",):
        count=query('SELECT COUNT(*) AS n FROM "'+name+'"')
        print("LIVE_COUNT "+json.dumps({"table":name,"count":count[0]["n"]},ensure_ascii=False),flush=True)
if any(t["name"]=="metadata" for t in tables):
    print("METADATA_KEYS "+json.dumps([x["key"] for x in query('SELECT key FROM metadata ORDER BY key')],ensure_ascii=False),flush=True)

import zstandard, msgpack
def binary_query(stmt):
    request=urllib.request.Request(url+"/v2/pipeline",
        json.dumps({"requests":[{"type":"execute","stmt":{"sql":stmt}},{"type":"close"}]}).encode(),
        {"Authorization":"Bearer "+token,"Content-Type":"application/json"})
    with urllib.request.urlopen(request,timeout=90) as fh: obj=json.load(fh)
    res=obj["results"][0]
    if res["type"]!="ok":raise RuntimeError(str(res["error"]))
    rows=res["response"]["result"]["rows"]
    return [base64.urlsafe_b64decode(row[0]["base64"]+"="*(-len(row[0]["base64"])%4)) for row in rows]
blobs=binary_query("SELECT payload FROM store_bundles WHERE store_id=652 AND chunk_no=0 LIMIT 1")
for b in blobs:
    unpacked=msgpack.unpackb(zstandard.ZstdDecompressor().decompress(b),raw=False)
    print("BUNDLE_KEYS "+json.dumps(sorted(unpacked.keys()),ensure_ascii=False),flush=True)
    for k in ("store_columns","product_columns","event_columns"):
        print("BUNDLE_COLUMNS "+json.dumps({"name":k,"columns":unpacked.get(k)},ensure_ascii=False),flush=True)
for b in binary_query("SELECT payload FROM auxiliary_bundles LIMIT 1"):
    aux=msgpack.unpackb(zstandard.ZstdDecompressor().decompress(b),raw=False)
    print("AUXILIARY_KEYS "+json.dumps(sorted(aux.keys()) if isinstance(aux,dict) else type(aux).__name__,ensure_ascii=False),flush=True)
    if isinstance(aux,dict):
        for k,v in aux.items():
            if isinstance(v,dict):
                print("AUXILIARY_SUBKEYS "+json.dumps({"key":k,"fields":sorted(v.keys())[:100]},ensure_ascii=False),flush=True)
