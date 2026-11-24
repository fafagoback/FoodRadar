"""Round 7: search exact cross-field text pooling and independently compressed shop records.
Pinned real checkpoint; no crawl, publication, or original cache mutation.
"""
import hashlib,itertools,json,os,random,resource,sqlite3,statistics,sys,tempfile,time
from pathlib import Path
import zstandard as z
from benchmark_normalized_storage import wire,unwire,load,ro,emit,p95
from benchmark_storage_round6 import encode_blob,decode_blob,to_columns,from_columns
DEADLINE=1795564800 # overwritten below from explicit UTC timestamp
from datetime import datetime
DEADLINE=datetime.fromisoformat("2026-11-25T08:00:00+08:00").timestamp()

def dictionary(d,style):
    fields=[k for k in ("product_name","description","category") if k in d]
    if style=="separate":return {k:encode_blob(v,True,True) for k,v in d.items()}
    pool=[];index={};refs={}
    for k in fields:
        refs[k]=[]
        for v in d[k]:
            key=wire(v)
            if key not in index:index[key]=len(pool);pool.append(v)
            refs[k].append(index[key])
    return {**{k:encode_blob(v,True,True) for k,v in d.items() if k not in fields},"__r7_text_pool":encode_blob([list(d),pool,refs],True,False)}
def read_dict(db):
    d={k:decode_blob(v) for k,v in db.execute("SELECT key,payload FROM dictionary ORDER BY key")}
    if "__r7_text_pool" in d:
        order,pool,refs=d.pop("__r7_text_pool")
        d.update({k:[pool[i] for i in v] for k,v in refs.items()})
        d={k:d[k] for k in order}
    return d

def blob(s,h,mode,level):
    c=z.ZstdCompressor(level=level)
    row=c.compress(wire([0,s,h]))
    if mode=="row":return row
    col=c.compress(wire([1,[s[0],to_columns(s[1])],h]))
    if mode=="column":return col
    return min(row,col,key=len)
def decode(b):
    flag,s,h=unwire(z.ZstdDecompressor().decompress(b))
    return ([s[0],from_columns(s[1])] if flag else s),h

def measure(p,ids,base=False):
    opens=[];scans=[];qs=[]
    for _ in range(3):
        start=time.perf_counter();db=ro(p)
        if base:
            d={k:unwire(z.ZstdDecompressor().decompress(v)) for k,v in db.execute("SELECT key,payload FROM dictionary")}
            def read(sid):
                s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
                return unwire(z.ZstdDecompressor().decompress(s)),unwire(z.ZstdDecompressor().decompress(h))
        else:
            d=read_dict(db)
            def read(sid):return decode(db.execute("SELECT payload FROM shop WHERE store=?",(sid,)).fetchone()[0])
        opens.append(time.perf_counter()-start);start=time.perf_counter()
        for sid in ids:
            t=time.perf_counter();read(sid);qs.append(time.perf_counter()-t)
        scans.append(time.perf_counter()-start);db.close()
    return dict(open_ms=round(statistics.median(opens)*1000,3),scan_ms=round(statistics.median(scans)*1000,3),shop_p95_us=round(p95(qs)*1e6,3),peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

def trial(p,d,raw,ids,style,mode,page,level,encoded):
    t=time.perf_counter()
    with sqlite3.connect(p) as db:
        db.executescript(f"PRAGMA page_size={page}; CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE shop(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;")
        db.executemany("INSERT INTO dictionary VALUES(?,?)",encoded.items())
        db.executemany("INSERT INTO shop VALUES(?,?)",((sid,blob(s,h,mode,level)) for sid,(s,h) in raw.items()))
    build=time.perf_counter()-t
    with ro(p) as db:
        assert wire(read_dict(db))==wire(d)
        assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
        assert db.execute("SELECT count(*) FROM shop").fetchone()[0]==len(ids)
        for sid in ids:assert wire(decode(db.execute("SELECT payload FROM shop WHERE store=?",(sid,)).fetchone()[0]))==wire(raw[sid])
        sizes=dict(db.execute("SELECT key,length(payload) FROM dictionary"))
        payload=db.execute("SELECT sum(length(payload)) FROM shop").fetchone()[0]
    size=p.stat().st_size;metrics=measure(p,ids)
    sid=next(k for k in ids if raw[k][0][1]);s,h=unwire(wire(raw[sid]));i=d["__product_columns"].index("price")
    old=s[1][0][i];s[1][0][i]=old+1 if type(old) in (int,float) else 1
    t=time.perf_counter()
    with sqlite3.connect(p) as db:db.execute("UPDATE shop SET payload=? WHERE store=?",(blob(s,h,mode,level),sid))
    update=(time.perf_counter()-t)*1000
    with ro(p) as db:
        for k in ids:assert wire(decode(db.execute("SELECT payload FROM shop WHERE store=?",(k,)).fetchone()[0]))==wire((s,h) if k==sid else raw[k])
    return dict(bytes=size,exact=True,incremental_isolation=True,build_s=round(build,3),update_ms=round(update,3),dictionary_bytes=sizes,shop_bytes=payload,**metrics)

def main():
    root=Path(sys.argv[1]);state=Path(sys.argv[2]);m=json.loads((root/"manifest.json").read_text())
    parts=m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]
    fingerprint=[p["sha256"] for p in parts]
    saved=json.loads(state.read_text()) if state.exists() else {"fingerprint":fingerprint,"results":[]}
    assert saved["fingerprint"]==fingerprint,"Comparison data changed"
    done={tuple(x["params"]) for x in saved["results"]}
    candidates=list(itertools.product(("pooled","separate"),("adaptive","column","row"),(512,1024,2048,4096),(3,8,12,16,19)))
    # Prioritize the best observed structural family, then explore remaining combinations.
    def rank(c):
        comparable=[x for x in saved["results"] if x.get("exact") and x["params"][:2]==list(c[:2])]
        return (min((x["bytes"] for x in comparable),default=10**15),abs(c[2]-512),abs(c[3]-12))
    started=time.monotonic()
    with tempfile.TemporaryDirectory() as tmp:
        loaded=[]
        for part in parts:
            p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]
            d,raw=load(p);ids=list(raw);random.Random(416).shuffle(ids)
            loaded.append((d,raw,ids))
        dictionaries={}
        while time.time()<DEADLINE and time.monotonic()-started<28*60:
            remaining=[c for c in candidates if c not in done]
            if not remaining:break
            c=min(remaining,key=rank);done.add(c);style,mode,page,level=c
            result={"params":list(c),"parts":[]}
            try:
                for i,(d,raw,ids) in enumerate(loaded):
                    key=(i,style)
                    if key not in dictionaries:dictionaries[key]=dictionary(d,style)
                    p=Path(tmp)/"candidate.db"
                    try:result["parts"].append(trial(p,d,raw,ids,*c,dictionaries[key]))
                    finally:p.unlink(missing_ok=True)
                result.update(bytes=sum(p["bytes"] for p in result["parts"]),exact=True)
            except Exception as e:result["error"]=repr(e)
            saved["results"].append(result);state.write_text(json.dumps(saved,ensure_ascii=False,indent=2))
            emit("FR7_RESULT",result)
    print("FR7_REMAINING="+str(len(candidates)-len(done)),flush=True)
if __name__=="__main__":main()
