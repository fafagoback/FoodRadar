"""Round 9: shared trained codec dictionary, local column encoding, fast metadata.
The dictionary is included in total DB bytes; no external serving dependency.
"""
import hashlib,json,random,sqlite3,sys,tempfile,time,statistics,resource
from pathlib import Path
from datetime import datetime
import zstandard as z
from benchmark_normalized_storage import wire,unwire,load,ro,emit,p95
from benchmark_storage_round6 import to_columns,from_columns
from benchmark_storage_round7 import measure as baseline
DEADLINE=datetime.fromisoformat("2026-11-25T08:00:00+08:00").timestamp()
def run(src,tmp):
 d,raw=load(src);ids=list(raw);random.Random(416).shuffle(ids)
 emit("FR9_BASE",dict(bytes=src.stat().st_size,shops=len(ids),products=sum(len(s[1]) for s,h in raw.values()),**baseline(src,ids,True)))
 with ro(src) as db:metadata=list(db.execute("SELECT key,payload FROM dictionary"))
 for mode in ("row","column","adaptive"):
  def representations(s,h):
   a=wire([0,s,h])
   if mode=="row":return [a]
   b=wire([1,[s[0],to_columns(s[1])],h])
   return [b] if mode=="column" else [a,b]
  samples=[v for sid in ids[:1200] for v in representations(*raw[sid])]
  for capacity in (32768,131072,524288):
   if time.time()>=DEADLINE:return
   p=tmp/"candidate.db"
   try:
    t=time.perf_counter();trained=z.train_dictionary(capacity,samples);dictionary=trained.as_bytes()
    c=z.ZstdCompressor(level=12,dict_data=trained);plain=z.ZstdCompressor(level=12)
    def encode(s,h):
     return min([bytes([flag])+codec.compress(v) for v in representations(s,h) for flag,codec in ((0,plain),(1,c))],key=len)
    with sqlite3.connect(p) as db:
     db.executescript("PRAGMA page_size=512; CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE codec(id INTEGER PRIMARY KEY,payload BLOB NOT NULL); CREATE TABLE shop(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;")
     db.executemany("INSERT INTO dictionary VALUES(?,?)",metadata)
     db.execute("INSERT INTO codec VALUES(1,?)",(dictionary,))
     db.executemany("INSERT INTO shop VALUES(?,?)",((sid,encode(*raw[sid])) for sid in ids))
    build=time.perf_counter()-t
    def reader(db):
     plain=z.ZstdDecompressor();dd={k:unwire(plain.decompress(v)) for k,v in db.execute("SELECT key,payload FROM dictionary ORDER BY key")}
     codec=z.ZstdDecompressor(dict_data=z.ZstdCompressionDict(db.execute("SELECT payload FROM codec").fetchone()[0]))
     def read(sid):
      v=db.execute("SELECT payload FROM shop WHERE store=?",(sid,)).fetchone()[0]
      flag,s,h=unwire((codec if v[0] else plain).decompress(v[1:]))
      return ([s[0],from_columns(s[1])] if flag else s),h
     return dd,read
    with ro(p) as db:
     dd,read=reader(db);assert wire(dd)==wire(d)
     assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
     assert db.execute("SELECT count(*) FROM shop").fetchone()[0]==len(ids)
     for sid in ids:assert wire(read(sid))==wire(raw[sid]),sid
     breakdown={k:db.execute(f"SELECT sum(length(payload)) FROM {k}").fetchone()[0] for k in ("dictionary","codec","shop")}
    size=p.stat().st_size;opens=[];scans=[];qs=[]
    for _ in range(3):
     t=time.perf_counter();db=ro(p);dd,read=reader(db);opens.append(time.perf_counter()-t);t=time.perf_counter()
     for sid in ids:
      q=time.perf_counter();read(sid);qs.append(time.perf_counter()-q)
     scans.append(time.perf_counter()-t);db.close()
    sid=next(k for k in ids if raw[k][0][1]);s,h=unwire(wire(raw[sid]));i=d["__product_columns"].index("price");old=s[1][0][i];s[1][0][i]=old+1 if type(old) in (int,float) else 1
    t=time.perf_counter()
    with sqlite3.connect(p) as db:db.execute("UPDATE shop SET payload=? WHERE store=?",(encode(s,h),sid))
    update=time.perf_counter()-t
    with ro(p) as db:
     dd,read=reader(db)
     for k in ids:assert wire(read(k))==wire((s,h) if k==sid else raw[k]),k
    emit("FR9_RESULT",dict(mode=mode,capacity=capacity,bytes=size,exact=True,incremental_isolation=True,breakdown=breakdown,build_s=build,update_ms=update*1000,open_ms=statistics.median(opens)*1000,scan_ms=statistics.median(scans)*1000,shop_p95_us=p95(qs)*1e6,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
   except Exception as e:emit("FR9_FAILURE",dict(mode=mode,capacity=capacity,error=repr(e)))
   finally:p.unlink(missing_ok=True)
def main():
 root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
 for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
  p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]=="90af51959743bd61ecb897fae86be11b003e66c3b24d0f66bcb6a120570f7662";emit("FR9_HASH",part["sha256"])
  with tempfile.TemporaryDirectory() as tmp:run(p,Path(tmp))
if __name__=="__main__":main()
