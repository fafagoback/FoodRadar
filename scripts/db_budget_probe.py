"""Read-only measurement of the real checkpoint, not a production migration."""
import hashlib
import json
import os
import sqlite3
import subprocess
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

source = Path(sys.argv[1]).resolve()
report = {"target_bytes": 20_000_000, "source_bytes": source.stat().st_size}
db = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
db.execute("PRAGMA query_only=ON")
def scalar(sql, params=()):
    return db.execute(sql, params).fetchone()[0]
def footprint(conn):
    return dict(conn.execute("SELECT name,sum(pgsize) FROM dbstat GROUP BY name ORDER BY sum(pgsize) DESC"))
def emit():
    text = json.dumps(report, ensure_ascii=False, indent=2)
    Path("db-budget-report.json").write_text(text, encoding="utf-8")
    print("DB_BUDGET_REPORT=" + json.dumps(report, ensure_ascii=False), flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("## 真實 SQLite 容量（非正式改版）\n\n")
            f.write("|物件|容量（MB）|\n|---|---:|\n")
            for name, size in report.get("objects", {}).items():
                f.write(f"|{name}|{size/1_000_000:.3f}|\n")
            f.write("\n```json\n" + text + "\n```\n")
meta = json.loads((source.parent/"manifest.json").read_text())
with source.open("rb") as f:
    assert hashlib.file_digest(f, "sha256").hexdigest() == meta["sqlite_sha256"], "source digest mismatch"
report["batch"] = meta["batch_id"]
report["objects"] = footprint(db)
report["page_size"] = scalar("PRAGMA page_size")
report["free_pages"] = scalar("PRAGMA freelist_count")
report["free_bytes"] = report["page_size"] * report["free_pages"]
report["rows"] = {t: scalar(f"SELECT count(*) FROM {t}") for t in
    ("stores","products","events","product_price_observations","crawl_batches")}
report["event_types"] = [
    {"type": kind, "rows": count, "json_bytes": size}
    for kind,count,size in db.execute("""SELECT event_type,count(*),
    sum(coalesce(length(cast(old_state AS blob)),0)+coalesce(length(cast(new_state AS blob)),0))
    FROM events GROUP BY event_type ORDER BY 3 DESC""")]
cols = [r[1] for r in db.execute("PRAGMA table_info(products)")]
report["product_text_bytes"] = dict(zip(
    cols, db.execute("SELECT "+",".join('sum(coalesce(length(cast("'+c+'" AS blob)),0))' for c in cols)+" FROM products").fetchone()))
latest = scalar("SELECT max(observed_at) FROM product_price_observations")
cutoff = (datetime.fromisoformat(latest)-timedelta(days=21)).isoformat()
report["recent_21d_rows"] = scalar("SELECT count(*) FROM product_price_observations WHERE observed_at>=?", (cutoff,))
emit()
# Lossless identity + quote benchmark. Not a full product/catalog DB:
# do not mistake the price-only benchmark for a 20 MB production result.
def binary(v):
    try:
        return bytes.fromhex(v.replace("-",""))
    except (ValueError,AttributeError):
        return str(v).encode()
def create_candidate(path, recent_only):
    dst = sqlite3.connect(path)
    dst.executescript("""PRAGMA journal_mode=OFF;
      PRAGMA synchronous=OFF;
      CREATE TABLE sku(id INTEGER PRIMARY KEY, store_key BLOB NOT NULL, product_key BLOB NOT NULL);
      CREATE TABLE batch(id INTEGER PRIMARY KEY, batch_key TEXT NOT NULL, observed_at TEXT NOT NULL);
      CREATE TABLE quotes(sku INTEGER NOT NULL,batch INTEGER NOT NULL,menu INTEGER NOT NULL,
        kind INTEGER NOT NULL,buy INTEGER NOT NULL,free INTEGER NOT NULL,received INTEGER NOT NULL,
        original INTEGER NOT NULL,payable INTEGER NOT NULL,source BLOB NOT NULL,
        PRIMARY KEY(sku,batch)) WITHOUT ROWID;
      """)
    ids = {}
    batches = {}
    started = time.perf_counter()
    last_seen = {}
    kinds = {"NONE":0,"SAME_SKU_BXGY":1,"DIRECT_DISCOUNT":2}
    query = """SELECT store_uuid,product_uuid,batch_id,observed_at,menu_price_cents,promotion_type,
      buy_quantity,free_quantity,received_quantity,original_total_cents,payable_total_cents,source_hash
      FROM product_price_observations"""
    params = ()
    if recent_only:
        query += " WHERE observed_at>=?"
        params = (cutoff,)
    for sid,pid,bid,at,menu,kind,buy,free,received,original,payable,sha in db.execute(query,params):
        key = (sid,pid)
        if key not in ids:
            ids[key] = len(ids)+1
            dst.execute("INSERT INTO sku VALUES(?,?,?)",(ids[key],binary(sid),binary(pid)))
        bkey = (bid,at)
        if bkey not in batches:
            batches[bkey] = len(batches)+1
            dst.execute("INSERT INTO batch VALUES(?,?,?)",(batches[bkey],bid,at))
        dst.execute("INSERT INTO quotes VALUES(?,?,?,?,?,?,?,?,?,?)",
          (ids[key],batches[bkey],menu,kinds[kind],buy,free,received,original,payable,binary(sha)))
    dst.execute("CREATE UNIQUE INDEX sku_identity ON sku(store_key,product_key)")
    dst.commit()
    expected=report["recent_21d_rows"] if recent_only else report["rows"]["product_price_observations"]
    actual=dst.execute("SELECT count(*) FROM quotes").fetchone()[0]
    assert actual==expected,(actual,expected)
    assert dst.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
    result={"quotes":actual,"skus":len(ids),"batches":len(batches),
            "bytes":path.stat().st_size,"objects":footprint(dst),"build_seconds":round(time.perf_counter()-started,3)}
    # Timings are local indexed price lookups, not web query timings.
    sample=list(ids.values())[::max(1,len(ids)//1000)][:1000]
    start=time.perf_counter()
    for sku in sample:
        list(dst.execute("SELECT * FROM quotes WHERE sku=? ORDER BY batch",(sku,)))
    result["indexed_lookup_count"]=len(sample)
    result["indexed_lookup_seconds"]=round(time.perf_counter()-start,6)
    dst.close()
    if shutil.which("zstd") and subprocess.run(["zstd","-q","-3","-f",str(path),"-o",str(path)+".zst"]).returncode==0:
        result["compressed_bytes"]=Path(str(path)+".zst").stat().st_size
    return result
report["price_only_21d_candidate"]=create_candidate(Path("recent-price.db"),True)
emit()
report["price_only_all_history_candidate"]=create_candidate(Path("all-price.db"),False)
emit()
