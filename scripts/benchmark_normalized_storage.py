"""Experimental lossless normalized snapshot, independent shop reads.

Every pool uses exact MessagePack bytes as identity, never brand/name matching.
Original store/product IDs, category memberships, price observations and all
encoded field values survive. The production crawler and release path are untouched.
"""
from __future__ import annotations
import collections, hashlib, json, math, mmap, operator, random, re, sqlite3, statistics, struct, sys, tempfile, time, uuid
from pathlib import Path
import msgpack,zstandard as z

def wire(v):return msgpack.packb(v,use_bin_type=True)
def unwire(v):return msgpack.unpackb(v,raw=False,strict_map_key=False)
def ro(p):return sqlite3.connect(p.resolve().as_uri()+"?mode=ro",uri=True)
D=z.ZstdDecompressor()
def inflate(b):return unwire(D.decompress(b))
def emit(k,v):print(k+"="+json.dumps(v,ensure_ascii=False),flush=True)
def p95(xs):return sorted(xs)[math.ceil(len(xs)*.95)-1]
def packed(v,level):return z.ZstdCompressor(level=level).compress(wire(v))
class Pool:
    def __init__(self):self.values=[];self.index={}
    def put(self,v):
        k=wire(v)
        if k not in self.index:self.index[k]=len(self.values);self.values.append(v)
        return self.index[k]

# Transform only reversible literal tokens, not URLs that merely look similar.
TOKEN=re.compile(r"(?<![0-9A-Fa-f])(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32,}|[0-9A-F]{32,})(?![0-9A-Fa-f])")
def encode_text(s):
    if not isinstance(s,str):return s
    out=[];pos=0
    for m in TOKEN.finditer(s):
        v=m.group()
        if "-" in v:tok=msgpack.ExtType(1,uuid.UUID(v).bytes)
        elif len(v)%2:continue
        else:tok=msgpack.ExtType(3 if v!=v.lower() else 2,bytes.fromhex(v))
        if m.start()>pos:out.append(s[pos:m.start()])
        out.append(tok);pos=m.end()
    if not out:return s
    if pos<len(s):out.append(s[pos:])
    return out
def decode_text(v):
    if not isinstance(v,list):return v
    return "".join(x if isinstance(x,str) else str(uuid.UUID(bytes=x.data)) if x.code==1 else x.data.hex().upper() if x.code==3 else x.data.hex() for x in v)
def selected(k):return k.startswith("catalog:") and ("uuid" in k or k.endswith("image_url")) or k=="order_url"
def dict_encode(d,mode):
    return [[k,[encode_text(x) for x in v] if mode and selected(k) else v] for k,v in d.items()]
def dict_decode(d,mode):
    return {k:[decode_text(x) for x in v] if mode and selected(k) else v for k,v in d}

IDENTITY={"product_uuid","store_uuid","order_url"}
CONTENT={"product_name","category","description"}
LIFE={"first_seen","last_seen","listing_started_at","status","missing_streak","is_open","delivery_available","price_evidence_status"}
PRICE={"price","quantity","promo_type","effective_price","recent_prices","reference_price","discount_amount","discount_pct","price_novel_vs_previous_3","is_price_deal","price_direction","is_price_drop","price_change_amount","price_change_percent","menu_quote_price","menu_quote_effective_price","menu_quote_promo_type","menu_quote_quantity","verified_discount_pct"}
def schema_groups(pc):
    sets=(IDENTITY,CONTENT,PRICE,LIFE,{"catalog_web"})
    groups=[[i for i,c in enumerate(pc) if c in group] for group in sets]
    used={i for g in groups for i in g}
    groups.append([i for i in range(len(pc)) if i not in used])
    return [g for g in groups if g]
def catalog_kind(key):
    if "uuid" in key and ("section" in key or "subsection" in key):return 0
    if "promo" in key or "campaign" in key or "price" in key:return 1
    if key in ("is_available","is_sold_out","availability_state"):return 2
    return 3

class Normalizer:
    def __init__(self,d,mode):
        self.d=d;self.mode=mode;self.pc=d["__product_columns"];self.ci=self.pc.index("catalog_web")
        self.groups=schema_groups(self.pc)
        # UUID column independently interned, not bundled with store-specific URL.
        self.groups=[[i] for i in self.groups[0]]+self.groups[1:]
        self.pools=[Pool() for _ in self.groups]
        self.cparts=[Pool() for _ in range(4)];self.crecipes=Pool()
        self.hist=Pool()
        self.kinds=[catalog_kind(k) for k in d["catalog_key"]]
    def catalog(self,detail):
        pieces=[[] for _ in range(4)];order=[]
        for k,v in detail:
            i=self.kinds[k];pieces[i].append([k,v]);order.append(i)
        return self.crecipes.put([*[p.put(v) for p,v in zip(self.cparts,pieces)],order])
    def shop(self,state,history):
        header,rows=state;refs=[]
        for row in rows:
            r=list(row);r[self.ci]=self.catalog(row[self.ci])
            refs.append([p.put([r[i] for i in g]) for p,g in zip(self.pools,self.groups)])
        h=[[ordinal,self.hist.put(variants)] for ordinal,variants in history]
        return [header,refs,h]
    def metadata(self,trans):
        return [dict_encode(self.d,trans),trans,self.groups,[p.values for p in self.pools],
                [p.values for p in self.cparts],self.crecipes.values,self.hist.values]
    def inventory(self):
        return {",".join(self.pc[i] for i in g):len(p.values) for g,p in zip(self.groups,self.pools)}

class Reader:
    def __init__(self,path):
        self.f=path.open("rb");self.mm=mmap.mmap(self.f.fileno(),0,access=mmap.ACCESS_READ)
        if self.mm[:4]!=b"FRN1":raise ValueError("Unknown normalized format")
        off,mlen=struct.unpack_from("<QI",self.mm,4)
        d,trans,groups,pools,cparts,recipes,hist=inflate(self.mm[16:16+mlen])
        self.dictionary=dict_decode(d,trans);self.hist=hist
        # Preassemble catalog recipes, preserving exact ordering and identities.
        catalogs=[]
        for a,b,c,e,order in recipes:
            its=[iter(p[i]) for p,i in zip(cparts,(a,b,c,e))]
            catalogs.append([next(its[j]) for j in order])
        ci=self.dictionary["__product_columns"].index("catalog_web")
        for g,p in zip(groups,pools):
            if ci in g:
                j=g.index(ci)
                for v in p:v[j]=catalogs[v[j]]
        self.pools=[tuple(tuple(v) for v in p) for p in pools]
        flat=[i for g in groups for i in g]
        self.reorder=operator.itemgetter(*(flat.index(i) for i in range(len(flat))))
        self.lookup={sid:(o,n) for sid,o,n in unwire(self.mm[off:])}
    def read(self,sid):
        o,n=self.lookup[sid];header,refs,history=inflate(self.mm[o:o+n])
        pools=self.pools;reorder=self.reorder
        # No extra SQLite calls or per-product string comparisons.
        rows=[list(reorder(sum((p[i] for p,i in zip(pools,ref)),()))) for ref in refs]
        return [header,rows],[[i,self.hist[h]] for i,h in history]
    def close(self):self.mm.close();self.f.close()

def build(p,d,raw,level,trans):
    norm=Normalizer(d,"split")
    # Two-pass encoding only over the already independently crawled records.
    stores=[(sid,norm.shop(s,h)) for sid,(s,h) in raw.items()]
    meta=packed(norm.metadata(trans),level)
    c=z.ZstdCompressor(level=level)
    index=[]
    with p.open("wb") as f:
        f.write(b"FRN1"+b"\0"*12);f.write(meta)
        for sid,record in stores:
            b=c.compress(wire(record));off=f.tell();f.write(b);index.append([sid,off,len(b)])
        off=f.tell();f.write(wire(index));f.seek(4);f.write(struct.pack("<QI",off,len(meta)))
    return norm.inventory(),{"pool_bytes":len(meta),"shop_bytes":off-16-len(meta),"index_bytes":p.stat().st_size-off}

def load(source):
    with ro(source) as db:
        assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
        d={k:inflate(v) for k,v in db.execute("SELECT key,payload FROM dictionary ORDER BY key")}
        raw={sid:(inflate(s),inflate(h)) for sid,s,h in db.execute("SELECT s.store,s.payload,h.payload FROM shop_state s JOIN price_history h USING(store)")}
        assert len(raw)==db.execute("SELECT count(*) FROM shop_state").fetchone()[0]==db.execute("SELECT count(*) FROM price_history").fetchone()[0]
    return d,raw
def measure(source,normalized,order):
    times=[];scans=[];opens=[]
    for _ in range(3):
        start=time.perf_counter()
        if normalized:
            db=Reader(source);read=db.read
        else:
            db=ro(source)
            # Equivalent metadata initialization (production holds dictionary in memory).
            dictionary={k:inflate(v) for k,v in db.execute("SELECT key,payload FROM dictionary")}
            def read(sid):
                s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
                return inflate(s),inflate(h)
        opens.append(time.perf_counter()-start);start=time.perf_counter()
        for sid in order:
            q=time.perf_counter();read(sid);times.append(time.perf_counter()-q)
        scans.append(time.perf_counter()-start);db.close()
    return {"open_ms":round(statistics.median(opens)*1000,3),"scan_ms":round(statistics.median(scans)*1000,3),"shop_p95_us":round(p95(times)*1e6,3)}

def run(source,root):
    d,raw=load(source);order=list(raw);random.Random(416).shuffle(order)
    emit("FR4_RESULT",{"mode":"original","bytes":source.stat().st_size,**measure(source,False,order)})
    for level,trans in ((6,False),(10,False),(10,True),(19,True)):
        p=root/f"normalized-{level}-{trans}.bin";start=time.perf_counter()
        inventory,parts=build(p,d,raw,level,trans);build_s=time.perf_counter()-start
        rr=Reader(p)
        assert wire(rr.dictionary)==wire(d)
        assert set(rr.lookup)==set(raw)
        for sid in order:assert wire(rr.read(sid))==wire(raw[sid]),("roundtrip mismatch",sid)
        rr.close()
        emit("FR4_RESULT",{"mode":"normalized","level":level,"binary_text_tokens":trans,"bytes":p.stat().st_size,"build_s":round(build_s,3),"exact":True,**parts,**measure(p,True,order)})
        emit("FR4_POOL_COUNTS",inventory)
        # Each mutation re-encodes just the changed shop's fields; verify pools are
        # immutable on insert and other identical content references remain valid.
        n=Normalizer(d,"split");sid=next(k for k,v in raw.items() if v[0][1]);state,h=raw[sid]
        old=n.shop(state,h);newstate=unwire(wire(state));i=d["__product_columns"].index("price")
        newstate[1][0][i]+=1
        frozen=wire(old);changed=n.shop(newstate,h)
        assert wire(old)==frozen and old!=changed
        emit("FR4_MUTATION_ISOLATION",True);p.unlink()
def main():
    root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
    emit("FR4_BATCH",m["batch_id"])
    for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
        p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]
        with tempfile.TemporaryDirectory() as tmp:run(p,Path(tmp))
if __name__=="__main__":main()
