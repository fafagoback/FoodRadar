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
