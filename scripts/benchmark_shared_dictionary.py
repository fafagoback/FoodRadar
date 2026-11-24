"""Read-only experiment on real production state; no cache or publication changes."""
import hashlib,json,math,random,sqlite3,statistics,sys,tempfile,time
from pathlib import Path
import msgpack,zstandard as z
from benchmark_exact_storage import ro
TABLES=("shop_state","price_history")
def pct(xs):return sorted(xs)[max(0,math.ceil(len(xs)*.95)-1)]
def report(k,v):print(k+"="+json.dumps(v,ensure_ascii=False),flush=True)
def unpack(v):return msgpack.unpackb(v,raw=False,strict_map_key=False)
def samples(raw):
    a=[]
    for _,v in raw:
        for i in range(0,len(v),4096):
            s=v[i:i+4096]
            if len(s)>=64:a.append(s)
    random.Random(416).shuffle(a)
    return a[:1500]
def run(source,tmp):
    with ro(source) as db:
        d=list(db.execute("SELECT key,payload FROM dictionary"))
        old={t:list(db.execute(f"SELECT store,payload FROM {t} ORDER BY store")) for t in TABLES}
    dec=z.ZstdDecompressor()
    raw={t:[(sid,dec.decompress(v)) for sid,v in old[t]] for t in TABLES}
    ids=[sid for sid,_ in old[TABLES[0]]]; random.Random(416).shuffle(ids)
    expected={t:dict(raw[t]) for t in TABLES}
    assert set(expected[TABLES[0]])==set(expected[TABLES[1]])
    report("FR2_STORAGE_BREAKDOWN",{"file_bytes":source.stat().st_size,"dictionary_payload_bytes":sum(len(v) for _,v in d),**{t+"_payload_bytes":sum(len(v) for _,v in old[t]) for t in TABLES},"products":sum(len(unpack(v)[1]) for _,v in raw["shop_state"])})
    def measure(p,dicts):
        timings=[];scans=[];opens=[]
        for _ in range(5):
            start=time.perf_counter()
            db=ro(p); ds={t:z.ZstdDecompressor(dict_data=dicts.get(t)) for t in TABLES}
            # Include dictionary initialization as a separate startup cost.
            if dicts:
                ds={t:z.ZstdDecompressor(dict_data=z.ZstdCompressionDict(db.execute("SELECT payload FROM dictionary WHERE key=?",("__codec_"+t,)).fetchone()[0])) for t in TABLES}
            opens.append(time.perf_counter()-start)
            start=time.perf_counter()
            for sid in ids:
                q=time.perf_counter()
                s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
                unpack(ds["shop_state"].decompress(s));unpack(ds["price_history"].decompress(h))
                timings.append(time.perf_counter()-q)
            scans.append(time.perf_counter()-start);db.close()
        return {"open_ms":round(statistics.median(opens)*1000,3),"scan_ms":round(statistics.median(scans)*1000,3),"p95_us":round(pct(timings)*1e6,3)}
    base={"mode":"original","bytes":source.stat().st_size,**measure(source,{})}
    report("FR2_RESULT",base)
    for size in (16384,65536,131072):
        dicts={t:z.train_dictionary(size,samples(raw[t])) for t in TABLES}
        for page,level in ((512,3),(512,6),(512,10),(1024,3),(1024,6),(1024,10)):
            p=tmp/f"test-{size}-{page}-{level}.db";start=time.perf_counter()
            with sqlite3.connect(p) as db:
                db.executescript(f"PRAGMA page_size={page};PRAGMA journal_mode=OFF;PRAGMA synchronous=OFF; CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID; CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;")
                db.executemany("INSERT INTO dictionary VALUES(?,?)",d)
                for t in TABLES:
                    db.execute("INSERT INTO dictionary VALUES(?,?)",("__codec_"+t,dicts[t].as_bytes()))
                    c=z.ZstdCompressor(level=level,dict_data=dicts[t],write_content_size=True)
                    db.executemany(f"INSERT INTO {t} VALUES(?,?)",((sid,c.compress(v)) for sid,v in raw[t]))
            build=time.perf_counter()-start
            with ro(p) as db:
                assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
                for t in TABLES:
                    dc=z.ZstdDecompressor(dict_data=dicts[t])
                    got={sid:dc.decompress(v) for sid,v in db.execute(f"SELECT store,payload FROM {t}")}
                    assert got==expected[t]
                assert [(k,v) for k,v in db.execute("SELECT key,payload FROM dictionary ORDER BY key") if not k.startswith("__codec_")]==sorted(d)
            v={"mode":"shared_zstd","dictionary_size":size,"page":page,"level":level,"bytes":p.stat().st_size,"build_s":round(build,3),"exact":True,**measure(p,dicts)}
            report("FR2_RESULT",v);p.unlink()
def main():
    root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
    parts=m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]
    report("FR2_BATCH",m["batch_id"])
    for part in parts:
        p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]
        with tempfile.TemporaryDirectory() as t:run(p,Path(t))
if __name__=="__main__":main()
