"""Round 8: exact shared catalog blocks with per-shop residuals and lazy block reads."""
import hashlib,json,random,sqlite3,sys,tempfile,time,statistics,resource
from pathlib import Path
import zstandard as z
from benchmark_normalized_storage import wire,unwire,load,ro,emit,p95
from benchmark_storage_round7 import dictionary,read_dict,measure as baseline
from datetime import datetime
deadline=datetime.fromisoformat("2026-11-25T08:00:00+08:00").timestamp()
def pack(v):return z.ZstdCompressor(level=19).compress(wire(v))
def unpack(v):return unwire(z.ZstdDecompressor().decompress(v))
def run(src,tmp):
 d,raw=load(src);ids=list(raw);random.Random(416).shuffle(ids)
 emit("FR8_BASE",dict(bytes=src.stat().st_size,shops=len(ids),products=sum(len(s[1]) for s,h in raw.values()),**baseline(src,ids,True)))
 pc=d["__product_columns"]
 for fields in [("product_name","description","category"),("product_name","description","category","price","quantity","promo_type"),("product_name","description")]:
  for chunk in (8,32,128):
   if time.time()>=deadline:return
   p=tmp/"candidate.db";start=time.perf_counter()
   try:
    cols=[pc.index(k) for k in fields if k in pc];rest=[i for i in range(len(pc)) if i not in cols]
    # Exact content-addressed blocks; sort projected rows to align shared catalogs, retain original positions.
    with sqlite3.connect(p) as db:
     db.executescript("PRAGMA page_size=512; CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB) WITHOUT ROWID; CREATE TABLE block(id INTEGER PRIMARY KEY,payload BLOB); CREATE TABLE shop(store BLOB PRIMARY KEY,payload BLOB) WITHOUT ROWID;")
     db.executemany("INSERT INTO dictionary VALUES(?,?)",dictionary(d,"pooled").items())
     index={}
     def encode(s,h):
      rows=s[1];order=sorted(range(len(rows)),key=lambda i:wire([rows[i][j] for j in cols]))
      refs=[]
      for k in range(0,len(order),chunk):
       ix=order[k:k+chunk];v=[[rows[i][j] for j in cols] for i in ix];key=wire(v)
       if key not in index:
        index[key]=len(index)+1;db.execute("INSERT INTO block VALUES(?,?)",(index[key],pack(v)))
       refs.append(index[key])
      return pack([s[0],order,refs,[[row[j] for j in rest] for row in rows],h])
     for sid,(s,h) in raw.items():db.execute("INSERT INTO shop VALUES(?,?)",(sid,encode(s,h)))
     def read(conn,sid):
      head,order,refs,res,h=unpack(conn.execute("SELECT payload FROM shop WHERE store=?",(sid,)).fetchone()[0])
      rows=[[None]*len(pc) for _ in order]
      for row,v in zip(rows,res):
       for j,x in zip(rest,v):row[j]=x
      pos=0
      for ref in refs:
       for v in unpack(conn.execute("SELECT payload FROM block WHERE id=?",(ref,)).fetchone()[0]):
        for j,x in zip(cols,v):rows[order[pos]][j]=x
        pos+=1
      assert pos==len(order)
      return [head,rows],h
     build=time.perf_counter()-start
     assert wire(read_dict(db))==wire(d)
     for sid in ids:assert wire(read(db,sid))==wire(raw[sid])
     db.commit()
     size=p.stat().st_size
     sizes={t:db.execute(f"SELECT sum(length(payload)) FROM {t}").fetchone()[0] for t in ("dictionary","block","shop")}
     opens=[];scans=[];qs=[]
     for _ in range(3):
      t=time.perf_counter()
      with ro(p) as conn:
       dd=read_dict(conn);opens.append(time.perf_counter()-t);t=time.perf_counter()
       for sid in ids:
        q=time.perf_counter();read(conn,sid);qs.append(time.perf_counter()-q)
       scans.append(time.perf_counter()-t)
     sid=next(k for k in ids if raw[k][0][1]);s,h=unwire(wire(raw[sid]));i=pc.index("price");old=s[1][0][i];s[1][0][i]=old+1 if type(old) in (int,float) else 1
     t=time.perf_counter();db.execute("UPDATE shop SET payload=? WHERE store=?",(encode(s,h),sid));db.commit();update=time.perf_counter()-t
     for k in ids:assert wire(read(db,k))==wire((s,h) if k==sid else raw[k])
     emit("FR8_RESULT",dict(fields=fields,chunk=chunk,bytes=size,exact=True,incremental_isolation=True,blocks=len(index),breakdown=sizes,build_s=build,update_ms=update*1000,open_ms=statistics.median(opens)*1000,scan_ms=statistics.median(scans)*1000,shop_p95_us=p95(qs)*1e6,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
   except Exception as e:emit("FR8_FAILURE",dict(fields=fields,chunk=chunk,error=repr(e)))
   finally:p.unlink(missing_ok=True)
def main():
 root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
 for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
  p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"];emit("FR8_HASH",part["sha256"])
  with tempfile.TemporaryDirectory() as tmp:run(p,Path(tmp))
if __name__=="__main__":main()
