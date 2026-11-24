"""Read-only benchmark on restored production compact state. No deployment."""
import collections, hashlib, json, math, random, sqlite3, statistics, sys, tempfile, time
from pathlib import Path
import msgpack, zstandard as zstd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from compact_checkpoint import _unpack

def wire(x): return msgpack.packb(x,use_bin_type=True)
def pack(x,level=10): return zstd.ZstdCompressor(level=level,write_content_size=True).compress(wire(x))
def ro(p): return sqlite3.connect(p.resolve().as_uri()+"?mode=ro",uri=True)
def cols(rs,n): return [list(c) for c in zip(*rs)] if rs else [[] for _ in range(n)]
def rows(cs): return [list(r) for r in zip(*cs)]
def percentile(xs): return sorted(xs)[max(0,math.ceil(len(xs)*.95)-1)] if xs else 0
def open_reader(p,mode):
    db=ro(p)
    if mode in ("static","full"):
        shared,local,pool,hpool=_unpack(db.execute("SELECT payload FROM dictionary WHERE key='__experiment'").fetchone()[0])
    def read(sid):
        s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
        store,items=_unpack(s); hist=_unpack(h)
        if mode=="column": items=rows(items)
        elif mode=="constant":
            n,cs=items
            items=rows([[v]*n if flag else v for flag,v in cs])
        elif mode in ("static","full"):
            refs,lc=items; items=[]
            for ref,rs in zip(refs,rows(lc)):
                r=[None]*(len(shared)+len(local))
                for i,v in zip(shared,pool[ref] if isinstance(ref,int) else ref): r[i]=v
                for i,v in zip(local,rs): r[i]=v
                items.append(r)
            hist=[[i,hpool[v] if isinstance(v,int) else v] for i,v in hist]
        return [store,items],hist
    return db,read
LOCAL={"product_uuid","store_uuid","order_url","first_seen","last_seen","listing_started_at","status","missing_streak","is_open","delivery_available"}
STATIC={"product_name","category","description","catalog_web"}
def bench(source,td):
    with ro(source) as db:
        assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
        d=dict(db.execute("SELECT key,payload FROM dictionary"))
        assert "__experiment" not in d
        pc=_unpack(d["__product_columns"])
        orig=[(sid,_unpack(s),_unpack(h)) for sid,s,h in db.execute("SELECT s.store,s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) ORDER BY s.store")]
        assert len(orig)==db.execute("SELECT COUNT(*) FROM shop_state").fetchone()[0]==db.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
    assert orig and all(len(r)==len(pc) for _,(_,ps),_ in orig for r in ps)
    ids=[x[0] for x in orig]; random.Random(416).shuffle(ids)
    expected={sid:(s,h) for sid,s,h in orig}
    hc=collections.Counter(wire(v) for _,_,h in orig for _,v in h)
    def pool_for(c):
        keys=sorted((b for b,n in c.items() if n>1 and len(b)>=8),key=lambda b:(-c[b],b))
        return {b:i for i,b in enumerate(keys)},[msgpack.unpackb(b,raw=False,strict_map_key=False) for b in keys]
    def measure(p,mode):
        opens=[]; scans=[]; singles=[]
        for _ in range(3):
            t=time.perf_counter(); db,read=open_reader(p,mode); opens.append(time.perf_counter()-t)
            t=time.perf_counter()
            for sid in ids:
                q=time.perf_counter(); read(sid); singles.append(time.perf_counter()-q)
            scans.append(time.perf_counter()-t); db.close()
        return {"open_ms":round(statistics.median(opens)*1000,3),"scan_ms":round(statistics.median(scans)*1000,3),"shop_p95_us":round(percentile(singles)*1e6,3)}
    base={"mode":"original","bytes":source.stat().st_size,**measure(source,"row")}
    result=[base]; print("FR_BENCH="+json.dumps(base),flush=True)
    print("FR_INVENTORY="+json.dumps({"shops":len(ids),"products":sum(len(s[1]) for _,s,_ in orig),"history_groups":sum(hc.values()),"repeated_history_groups":sum(hc.values())-len(hc),"source":source.name}),flush=True)
    variants=[("row",512,10),("row",1024,3),("row",1024,19),("row",2048,10),("column",512,10),("column",1024,10),("constant",512,10),("constant",1024,10),("static",512,10),("full",512,10)]
    for mode,page,level in variants:
        p=td/f"{source.name}-{mode}-{page}-{level}.db"; t=time.perf_counter()
        intern=mode in ("static","full")
        if intern:
            shared=[i for i,c in enumerate(pc) if (c in STATIC if mode=="static" else c not in LOCAL)]
            local=[i for i in range(len(pc)) if i not in shared]
            counts=collections.Counter(); shopcounts=collections.Counter()
            for _,(_,ps),_ in orig:
                k=[wire([r[i] for i in shared]) for r in ps]; counts.update(k); shopcounts.update(set(k))
            pi,pp=pool_for(counts); hi,hp=pool_for(hc)
            print("FR_EXACT_DUPLICATES="+json.dumps({"mode":mode,"fields":[pc[i] for i in shared],"distinct":len(counts),"duplicate_rows":sum(counts.values())-len(counts),"cross_shop_rows":sum(counts[k] for k,v in shopcounts.items() if v>1),"cross_shop_groups":sum(v>1 for v in shopcounts.values())},ensure_ascii=False),flush=True)
        with sqlite3.connect(p) as db:
            db.executescript(f"PRAGMA page_size={page}; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;")
            db.executemany("INSERT INTO dictionary VALUES(?,?)",d.items())
            if intern: db.execute("INSERT INTO dictionary VALUES(?,?)",("__experiment",pack([shared,local,pp,hp],level)))
            for sid,(header,ps),h in orig:
                payload=ps; hist=h
                if mode=="column": payload=cols(ps,len(pc))
                elif mode=="constant":
                    payload=[len(ps),[[1,c[0]] if c and all(wire(v)==wire(c[0]) for v in c) else [0,c] for c in cols(ps,len(pc))]]
                elif intern:
                    refs=[]
                    for r in ps:
                        v=[r[i] for i in shared]; refs.append(pi.get(wire(v),v))
                    payload=[refs,cols([[r[i] for i in local] for r in ps],len(local))]
                    hist=[[i,hi.get(wire(v),v)] for i,v in h]
                db.execute("INSERT INTO shop_state VALUES(?,?)",(sid,pack([header,payload],level)))
                db.execute("INSERT INTO price_history VALUES(?,?)",(sid,pack(hist,level)))
        build=time.perf_counter()-t
        db,read=open_reader(p,mode)
        assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
        for sid in ids:
            assert wire(read(sid))==wire(expected[sid]),(mode,sid)
        db.close()
        v={"mode":mode,"page":page,"level":level,"bytes":p.stat().st_size,"build_s":round(build,3),"exact":True,**measure(p,mode)}
        result.append(v); print("FR_BENCH="+json.dumps(v),flush=True); p.unlink()
    return result

def main():
    root=Path(sys.argv[1]); m=json.loads((root/"manifest.json").read_text())
    assert m["format"] in ("foodradar-checkpoint-v3","foodradar-checkpoint-v4")
    parts=m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]
    print("FR_BATCH="+str(m["batch_id"]),flush=True)
    with tempfile.TemporaryDirectory() as tmp:
        results=[]
        for part in parts:
            p=root/part["path"]
            assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]
            results.append(bench(p,Path(tmp)))
        totals=[]
        for i,v in enumerate(results[0]):
            totals.append({**{k:v[k] for k in ("mode","page","level") if k in v},"total_bytes":sum(r[i]["bytes"] for r in results),"exact":all(r[i].get("exact",True) for r in results)})
        print("FR_TOTAL="+json.dumps(totals),flush=True)
if __name__=="__main__": main()
