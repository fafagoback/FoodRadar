"""Real-state byte inventory and random-access block compression. Never modifies source."""
import collections, hashlib, json, math, mmap, random, re, sqlite3, statistics, struct, sys, tempfile, time
from pathlib import Path
import msgpack,zstandard as z
from benchmark_exact_storage import ro,wire
def emit(k,v):print(k+"="+json.dumps(v,ensure_ascii=False),flush=True)
def unpack(x):return msgpack.unpackb(x,raw=False,strict_map_key=False)
def pct(x):return sorted(x)[math.ceil(.95*len(x))-1]
D=z.ZstdDecompressor()
def run(src,root):
    with ro(src) as db:
        dictionary=list(db.execute("SELECT key,payload FROM dictionary ORDER BY key"))
        rawd={k:unpack(D.decompress(v)) for k,v in dictionary}
        raw=[(sid,D.decompress(s),D.decompress(h)) for sid,s,h in db.execute("SELECT s.store,s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) ORDER BY s.store")]
    pc=rawd["__product_columns"];sc=rawd["__store_columns"]
    si=sc.index("name");pi=pc.index("product_uuid"); ci=pc.index("catalog_web")
    brand_re=re.compile(r"7[\s-]*eleven|統一超商|7-11|711|全家|familymart|家樂福|carrefour|全聯|px mart|好市多|costco|美廉社|萊爾富|ok超商",re.I)
    fields=[pc.index(k) for k in ("product_name","category","description","price","quantity","promo_type","effective_price") if k in pc]
    identity_keys={"section_uuid","section_uuids","subsection_uuid","subsection_uuids","product_uuid","store_uuid","item_uuid"}
    brandgroups=collections.defaultdict(lambda:{"shops":0,"rows":0,"seen":collections.Counter(),"content":collections.Counter(),"per_shop":collections.Counter()})
    ids=collections.Counter();catkeys=collections.Counter()
    for sid,s,h in raw:
        header,rs=unpack(s);name=header[si]
        if isinstance(name,int):name=rawd["name"][name]
        match=brand_re.search(str(name));brand=match.group().lower().replace(" ","").replace("-","") if match else "其他"
        group=brandgroups[brand];group["shops"]+=1;group["rows"]+=len(rs)
        shopset=set()
        for r in rs:
            ids[wire(r[pi])]+=1
            details=r[ci];kept=[]
            for k,v in details:
                key=rawd["catalog_key"][k];catkeys[key]+=1
                if key not in identity_keys:kept.append([k,v])
            k=wire([r[i] for i in fields]+[kept])
            group["seen"][wire([r[i] for i in fields]+[details])]+=1
            group["content"][k]+=1;shopset.add(k)
        group["per_shop"].update(shopset)
    emit("FR3_DICTIONARY",sorted([{"key":k,"bytes":len(v),"entries":len(rawd[k]) if isinstance(rawd[k],list) else None} for k,v in dictionary],key=lambda x:-x["bytes"]))
    emit("FR3_UUID",{"total":sum(ids.values()),"unique":len(ids),"repeated":sum(ids.values())-len(ids),"catalog_keys":dict(catkeys)})
    for brand,g in brandgroups.items():
        emit("FR3_CHAIN",{"brand":brand,"shops":g["shops"],"rows":g["rows"],"exact_with_category_ids":g["rows"]-len(g["seen"]),"exact_without_category_ids":g["rows"]-len(g["content"]),"cross_shop_groups":sum(v>1 for v in g["per_shop"].values()),"cross_shop_rows":sum(g["content"][k] for k,v in g["per_shop"].items() if v>1)})
    byid={sid:(s,h) for sid,s,h in raw};order=list(byid);random.Random(416).shuffle(order)
    # Baseline performs equivalent decoded reads. Dictionary is already part of disk size.
    def measure_baseline():
        times=[];alltimes=[]
        for _ in range(3):
            with ro(src) as db:
                start=time.perf_counter()
                for sid in order:
                    t=time.perf_counter()
                    s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
                    unpack(D.decompress(s));unpack(D.decompress(h));times.append(time.perf_counter()-t)
                alltimes.append(time.perf_counter()-start)
        return {"scan_ms":statistics.median(alltimes)*1000,"p95_us":pct(times)*1e6}
    emit("FR3_RESULT",{"mode":"original","bytes":src.stat().st_size,**measure_baseline()})
    # Group only real payloads, preserving per-shop identities. Test random-access
    # blocks with an on-disk index and self-contained dictionary. Not a production migration.
    for block,level in ((1,6),(1,10),(1,19),(4,10),(16,10),(64,10)):
        p=root/f"block-{block}-{level}.bin";t=time.perf_counter();c=z.ZstdCompressor(level=level)
        index=[]
        with p.open("wb") as f:
            f.write(b"FRB1"+b"\0"*8)
            meta=c.compress(wire([[k,D.decompress(v)] for k,v in dictionary]))
            f.write(struct.pack("<I",len(meta)));f.write(meta)
            for i in range(0,len(raw),block):
                chunk=raw[i:i+block]
                data=c.compress(wire([[s,h] for _,s,h in chunk]))
                off=f.tell();f.write(data)
                for j,(sid,_,_) in enumerate(chunk):index.append([sid,off,len(data),j])
            off=f.tell();f.write(wire(index));f.seek(4);f.write(struct.pack("<Q",off))
        build=time.perf_counter()-t;times=[];scans=[];opens=[]
        for attempt in range(3):
            t=time.perf_counter()
            with p.open("rb") as f,mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ) as mm:
                offset=struct.unpack_from("<Q",mm,4)[0];mlen=struct.unpack_from("<I",mm,12)[0]
                actual_dict=unpack(D.decompress(mm[16:16+mlen]));assert actual_dict==[[k,D.decompress(v)] for k,v in dictionary]
                lookup={sid:(o,n,j) for sid,o,n,j in unpack(mm[offset:])}
                assert set(lookup)==set(byid);opens.append(time.perf_counter()-t)
                start=time.perf_counter()
                for sid in order:
                    t=time.perf_counter();o,n,j=lookup[sid];s,h=unpack(D.decompress(mm[o:o+n]))[j]
                    unpack(s);unpack(h);times.append(time.perf_counter()-t)
                    if attempt==0:assert (s,h)==byid[sid]
                scans.append(time.perf_counter()-start)
        emit("FR3_RESULT",{"mode":"block","block":block,"level":level,"bytes":p.stat().st_size,"build_s":round(build,3),"open_ms":round(statistics.median(opens)*1000,3),"scan_ms":round(statistics.median(scans)*1000,3),"p95_us":round(pct(times)*1e6,3),"exact":True});p.unlink()
def main():
    root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
    emit("FR3_BATCH",m["batch_id"])
    for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
        p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]
        with tempfile.TemporaryDirectory() as tmp:run(p,Path(tmp))
if __name__=="__main__":main()
