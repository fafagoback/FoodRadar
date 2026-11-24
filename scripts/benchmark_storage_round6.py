"""Round 6: independent shop access with transactional SQLite, no global product pool.
Only reads a hash-verified existing checkpoint. No network, crawl, release, or cache writes.
All dictionary and encoded shop/history data round-trip exactly. Experimental only.
"""
import hashlib, json, lzma, math, random, sqlite3, statistics, sys, tempfile, time
from pathlib import Path
import msgpack, zstandard as z
from benchmark_normalized_storage import wire, unwire, load, ro, emit, p95
from benchmark_storage_search import front, unfront

def encode_blob(value, mixed=False, frontcode=False):
    candidates=[(0,wire(value))]
    if frontcode and isinstance(value,list):
        f=front(value)
        if f is not None:candidates.append((1,wire(f)))
    out=[]
    for flag,data in candidates:
        out.append(bytes([flag])+z.ZstdCompressor(level=12).compress(data))
        if mixed:out.append(bytes([flag+2])+lzma.compress(data,preset=6))
    return min(out,key=len)

def decode_blob(blob):
    flag=blob[0]
    v=unwire(lzma.decompress(blob[1:]) if flag&2 else z.ZstdDecompressor().decompress(blob[1:]))
    return unfront(v) if flag&1 else v

def to_columns(rows):
    if not rows:return [0,[]]
    n=len(rows);cols=[]
    for col in zip(*rows):
        values=list(col)
        # All comparisons use encoded bytes, preserving types and nulls.
        counts={};pool=[];refs=[]
        for v in values:
            k=wire(v)
            if k not in counts:counts[k]=len(pool);pool.append(v)
            refs.append(counts[k])
        modes=[[0,values],[1,pool[0]]] if len(pool)==1 else [[0,values],[2,[pool,refs]]]
        # Sparse representation of a dominant value; positions retain exact order.
        hist={}
        for i in refs:hist[i]=hist.get(i,0)+1
        default=max(hist,key=hist.get)
        modes.append([3,[pool[default],[[i,pool[j]] for i,j in enumerate(refs) if j!=default]]])
        cols.append(min(modes,key=lambda x:len(wire(x))))
    return [n,cols]

def from_columns(value):
    n,cols=value
    if not n:return []
    out=[]
    for mode,v in cols:
        if mode==0:out.append(v)
        elif mode==1:out.append([v]*n)
        elif mode==2:out.append([v[0][i] for i in v[1]])
        elif mode==3:
            col=[v[0]]*n
            for i,x in v[1]:col[i]=x
            out.append(col)
        else:raise ValueError(mode)
    return [list(r) for r in zip(*out)]

def shop_blob(state,history,mode):
    row=encode_blob([0,state,history])
    if mode=="row":return row
    head,rows=state
    col=encode_blob([1,[head,to_columns(rows)],history])
    return col if mode=="column" else min((row,col),key=len)

def read_shop(blob):
    flag,state,h=decode_blob(blob)
    if flag:state=[state[0],from_columns(state[1])]
    return state,h

def connect(p,original=False):
    db=ro(p)
    if original:
        d={k:unwire(z.ZstdDecompressor().decompress(v)) for k,v in db.execute("SELECT key,payload FROM dictionary ORDER BY key")}
        def read(sid):
            s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
            return unwire(z.ZstdDecompressor().decompress(s)),unwire(z.ZstdDecompressor().decompress(h))
    else:
        d={k:decode_blob(v) for k,v in db.execute("SELECT key,payload FROM dictionary ORDER BY key")}
        def read(sid):return read_shop(db.execute("SELECT payload FROM shop WHERE store=?",(sid,)).fetchone()[0])
    return db,d,read

def measure(p,ids,original=False):
    opens=[];scans=[];queries=[]
    for _ in range(3):
        t=time.perf_counter();db,d,read=connect(p,original);opens.append(time.perf_counter()-t)
        t=time.perf_counter()
        for sid in ids:
            q=time.perf_counter();read(sid);queries.append(time.perf_counter()-q)
        scans.append(time.perf_counter()-t);db.close()
    return {"open_ms":round(statistics.median(opens)*1000,3),"scan_ms":round(statistics.median(scans)*1000,3),"shop_p95_us":round(p95(queries)*1e6,3)}

def benchmark(src,tmp):
    d,raw=load(src);ids=list(raw);random.Random(416).shuffle(ids)
    emit("FR6_BASE",{"bytes":src.stat().st_size,"shops":len(ids),"products":sum(len(s[1]) for s,h in raw.values()),**measure(src,ids,True)})
    # Distinct structural candidates. Later candidates combine successful mechanisms.
    for mode,page,mixed,frontcode in [("row",1024,False,False),("row",512,True,True),("column",1024,True,True),("adaptive",512,True,True),("adaptive",1024,True,True)]:
        tag=f"{mode}-{page}-{mixed}-{frontcode}";p=tmp/(tag+".db");t=time.perf_counter()
        try:
            with sqlite3.connect(p) as db:
                db.executescript(f"PRAGMA page_size={page}; PRAGMA journal_mode=DELETE; CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE shop(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;")
                db.executemany("INSERT INTO dictionary VALUES(?,?)",((k,encode_blob(v,mixed,frontcode)) for k,v in d.items()))
                db.executemany("INSERT INTO shop VALUES(?,?)",((sid,shop_blob(s,h,mode)) for sid,(s,h) in raw.items()))
            build=time.perf_counter()-t
            db,dd,read=connect(p)
            assert wire(dd)==wire(d)
            assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
            assert {r[0] for r in db.execute("SELECT store FROM shop")}==set(ids)
            for sid in ids:assert wire(read(sid))==wire(raw[sid]),sid
            sizes=dict(db.execute("SELECT key,length(payload) FROM dictionary"))
            shop_bytes=db.execute("SELECT sum(length(payload)) FROM shop").fetchone()[0]
            db.close()
            metrics=measure(p,ids)
            # Actual isolated update: mutate one existing encoded numeric price, retain
            # every other field and record, verify in a fresh connection, restore.
            sid=next(k for k in ids if raw[k][0][1])
            state,h=unwire(wire(raw[sid]));pi=d["__product_columns"].index("price")
            old=state[1][0][pi];state[1][0][pi]=old+1 if type(old) in (int,float) else 1
            t=time.perf_counter()
            with sqlite3.connect(p) as db:db.execute("UPDATE shop SET payload=? WHERE store=?",(shop_blob(state,h,mode),sid))
            update_ms=(time.perf_counter()-t)*1000
            db,dd,read=connect(p)
            assert wire(read(sid))==wire((state,h))
            for other in ids:
                if other!=sid:assert wire(read(other))==wire(raw[other]),other
            db.close()
            with sqlite3.connect(p) as db:db.execute("UPDATE shop SET payload=? WHERE store=?",(shop_blob(*raw[sid],mode),sid))
            db,dd,read=connect(p);assert wire(read(sid))==wire(raw[sid]);db.close()
            emit("FR6_RESULT",{"mode":tag,"bytes":p.stat().st_size,"exact":True,"incremental_isolation":True,"build_s":round(build,3),"update_ms":round(update_ms,3),"dictionary_fields":sizes,"shop_payload_bytes":shop_bytes,**metrics})
        except Exception as e:
            emit("FR6_FAILURE",{"mode":tag,"error":repr(e)})
        finally:
            if p.exists():p.unlink()

def main():
    root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
    emit("FR6_BATCH",m["batch_id"])
    for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
        src=root/part["path"];assert hashlib.sha256(src.read_bytes()).hexdigest()==part["sha256"]
        emit("FR6_PART",part["path"])
        with tempfile.TemporaryDirectory() as tmp:benchmark(src,Path(tmp))
if __name__=="__main__":main()
