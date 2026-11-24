"""Lossless storage search using the existing, hash-verified production checkpoint.
No crawling, publishing, external data, or cache writes. Each candidate is a complete
self-contained file with independent shop lookups. All original encoded fields and
dictionary entries must round-trip byte-identically.
"""
from __future__ import annotations
import collections, hashlib, json, lzma, mmap, operator, random, resource, struct, sys, tempfile, time, statistics
from pathlib import Path
import msgpack,zstandard as z
from benchmark_normalized_storage import Normalizer,load,wire,unwire,dict_encode,dict_decode,emit,p95,measure as baseline_measure

def comp(b,codec):
    return lzma.compress(b,preset=6) if codec=="xz" else z.ZstdCompressor(level=int(codec)).compress(b)
def decomp(b,codec):
    return lzma.decompress(b) if codec=="xz" else z.ZstdDecompressor().decompress(b)
def codec_pack(b,codecs):
    opts=[(c,comp(b,c)) for c in codecs];c,p=min(opts,key=lambda x:len(x[1]))
    return [c,p]
def codec_unpack(v):return decomp(v[1],v[0])

# Preserve dictionary ordering, but encode UUID-like text as binary and share
# UUID tokens with the product ID pool only when exactly equal.
def unify(d,pools,groups,pc):
    pi=next(i for i,g in enumerate(groups) if g==[pc.index("product_uuid")])
    ids=pools[pi];freq=collections.Counter()
    for r in ids:
        if isinstance(r[0],bytes) and len(r[0])==16:freq[r[0]]+=1
    for k,vs in d:
        if not (k.startswith("catalog:") or k=="order_url"):continue
        for v in vs:
            if isinstance(v,list):
                for x in v:
                    if isinstance(x,msgpack.ExtType) and x.code==1:freq[x.data]+=1
    common=sorted(k for k,n in freq.items() if n>1)
    index={k:i for i,k in enumerate(common)}
    for k,vs in d:
        if not (k.startswith("catalog:") or k=="order_url"):continue
        for j,v in enumerate(vs):
            if isinstance(v,list):
                vs[j]=[msgpack.ExtType(4,index[x.data].to_bytes(4,"little")) if isinstance(x,msgpack.ExtType) and x.code==1 and x.data in index else x for x in v]
    mask=bytearray((len(ids)+7)//8);literal=bytearray();refs=[];other=[]
    for i,r in enumerate(ids):
        v=r[0]
        if isinstance(v,bytes) and len(v)==16:
            mask[i//8]|=1<<(i%8)
            if v in index:refs.append(index[v]+1)
            else:refs.append(0);literal.extend(v)
        else:other.append(v)
    pools[pi]=[len(ids),bytes(mask),bytes(literal),refs,other]
    return pi,b"".join(common),{"shared_uuid_count":len(common),"literal_uuid_count":len(literal)//16,"non_uuid_count":len(other),"uuid_refs":sum(bool(v) for v in refs)}
def reverse_unify(d,pools,pi,common):
    common=[common[i:i+16] for i in range(0,len(common),16)]
    for k,vs in d:
        if not (k.startswith("catalog:") or k=="order_url"):continue
        for j,v in enumerate(vs):
            if isinstance(v,list):
                vs[j]=[msgpack.ExtType(1,common[int.from_bytes(x.data,"little")]) if isinstance(x,msgpack.ExtType) and x.code==4 else x for x in v]
    n,mask,literal,refs,other=pools[pi];pos=0;ri=iter(refs);oi=iter(other);out=[]
    for i in range(n):
        if mask[i//8]&(1<<(i%8)):
            ref=next(ri)
            if ref:v=common[ref-1]
            else:v=literal[pos:pos+16];pos+=16
        else:v=next(oi)
        out.append([v])
    assert pos==len(literal)
    pools[pi]=out

# A dictionary's values are immutable. For string columns compare exact front
# coding against ordinary serialization. Original token order is restored.
def front(vs):
    if not vs or not all(isinstance(v,str) for v in vs):return None
    seq=sorted(enumerate(vs),key=lambda iv:iv[1].encode("utf-8"))
    last=b"";prefix=[];suffix=[];order=[]
    for i,v in seq:
        b=v.encode("utf-8");n=0
        while n<min(len(last),len(b)) and last[n]==b[n]:n+=1
        prefix.append(n);suffix.append(b[n:]);order.append(i);last=b
    return [order,prefix,suffix]
def unfront(v):
    order,prefix,suffix=v;last=b"";out=[None]*len(order)
    for i,n,b in zip(order,prefix,suffix):
        last=last[:n]+b;out[i]=last.decode("utf-8")
    return out
def dict_chunks(d,codecs,frontcode):
    chunks=[]
    for key,vs in d:
        original=codec_pack(wire(vs),codecs);best=[0,original]
        if frontcode and isinstance(vs,list):
            f=front(vs)
            if f is not None:
                v=codec_pack(wire(f),codecs)
                if len(wire(v))<len(wire(original)):best=[1,v]
        chunks.append([key,best])
    return chunks
def undict_chunks(chunks):
    return [[k,unfront(unwire(codec_unpack(b))) if mode else unwire(codec_unpack(b))] for k,(mode,b) in chunks]

# Reference columns can be represented by their first ID and signed deltas, or
# by bit-packed offsets. Choose independently per shop and per column.
def intcol(vs,mode):
    if not vs:return [0,[]]
    if mode=="delta":
        return [1,[vs[0],*[b-a for a,b in zip(vs,vs[1:])]]]
    lo=min(vs);span=max(vs)-lo;width=span.bit_length()
    if width==0:return [2,[lo,0,len(vs),b""]]
    out=bytearray();acc=bits=0
    for v in vs:
        acc|=(v-lo)<<bits;bits+=width
        while bits>=8:out.append(acc&255);acc>>=8;bits-=8
    if bits:out.append(acc&255)
    return [2,[lo,width,len(vs),bytes(out)]]
def uncol(x):
    mode,v=x
    if mode==0:return v
    if mode==1:
        import itertools
        return list(itertools.accumulate(v))
    lo,w,n,b=v
    if not w:return [lo]*n
    acc=bits=i=0;mask=(1<<w)-1;out=[]
    for _ in range(n):
        while bits<w:acc|=b[i]<<bits;i+=1;bits+=8
        out.append(lo+(acc&mask));acc>>=w;bits-=w
    return out
def transform(record,mode):
    header,refs,h=record
    if mode=="rows":return record
    cols=list(map(list,zip(*refs))) if refs else []
    return [header,[intcol(c,mode) for c in cols],h]
def untransform(record,mode):
    if mode=="rows":return record
    header,cols,h=record
    return [header,[list(r) for r in zip(*(uncol(c) for c in cols))],h]

# Reorder global reference pools by frequency then exact serialized bytes.
# Compact references are assigned to frequent values; all references remapped.
def remap(stores,pools,history):
    counts=[collections.Counter() for _ in pools];hc=collections.Counter()
    for _,(_,rs,h) in stores:
        for r in rs:
            for c,v in zip(counts,r):c[v]+=1
        for _,i in h:hc[i]+=1
    def reorder(values,c):
        order=sorted(range(len(values)),key=lambda i:(-c[i],wire(values[i])))
        inverse=[0]*len(order)
        for j,i in enumerate(order):inverse[i]=j
        return [values[i] for i in order],inverse
    out=[];maps=[]
    for p,c in zip(pools,counts):
        v,m=reorder(p,c);out.append(v);maps.append(m)
    hp,hm=reorder(history,hc)
    new=[(sid,[head,[[m[i] for m,i in zip(maps,r)] for r in rs],[[i,hm[h]] for i,h in hist]]) for sid,(head,rs,hist) in stores]
    return new,out,hp

class Reader:
    def __init__(self,p):
        self.f=p.open("rb");self.mm=mmap.mmap(self.f.fileno(),0,access=mmap.ACCESS_READ)
        assert self.mm[:4]==b"FRN2"
        offset,mlen=struct.unpack_from("<QI",self.mm,4)
        ver,groups,dchunks,pchunks,cc,cr,hh,pi,common,mode=unwire(self.mm[16:16+mlen])
        d=undict_chunks(dchunks);pools=[unwire(codec_unpack(x)) for x in pchunks]
        if pi>=0:reverse_unify(d,pools,pi,codec_unpack(common))
        self.dictionary=dict_decode(d,True)
        cparts=[unwire(codec_unpack(x)) for x in cc];recipes=unwire(codec_unpack(cr));self.hist=unwire(codec_unpack(hh))
        catalogs=[]
        for a,b,c,e,order in recipes:
            its=[iter(p[i]) for p,i in zip(cparts,(a,b,c,e))]
            catalogs.append([next(its[j]) for j in order])
        ci=self.dictionary["__product_columns"].index("catalog_web")
        for g,pool in zip(groups,pools):
            if ci in g:
                j=g.index(ci)
                for v in pool:v[j]=catalogs[v[j]]
        self.pools=[tuple(tuple(v) for v in p) for p in pools]
        flat=[i for g in groups for i in g];self.reorder=operator.itemgetter(*(flat.index(i) for i in range(len(flat))))
        self.lookup={sid:(o,n,codec) for sid,o,n,codec in unwire(self.mm[offset:])};self.mode=mode
    def read(self,sid):
        o,n,codec=self.lookup[sid];header,refs,h=untransform(unwire(decomp(self.mm[o:o+n],codec)),self.mode)
        pools=self.pools;reorder=self.reorder
        return [header,[list(reorder(sum((p[i] for p,i in zip(pools,r)),()))) for r in refs]],[[i,self.hist[v]] for i,v in h]
    def close(self):self.mm.close();self.f.close()

def make_meta(d,groups,pools,cparts,recipes,hist,pc,codecs,frontcode,shared,mode):
    d=unwire(wire(dict_encode(d,True)));pools=unwire(wire(pools))
    pi=-1;common=b"";stats={}
    if shared:pi,common,stats=unify(d,pools,groups,pc)
    dc=dict_chunks(d,codecs,frontcode);pp=[codec_pack(wire(v),codecs) for v in pools]
    cc=[codec_pack(wire(v),codecs) for v in cparts];cr=codec_pack(wire(recipes),codecs);hh=codec_pack(wire(hist),codecs);co=codec_pack(common,codecs)
    breakdown={"dictionary":sum(len(wire(x)) for x in dc),"pools":sum(len(wire(x)) for x in pp),"catalog":sum(len(wire(x)) for x in cc)+len(wire(cr)),"history":len(wire(hh)),"shared_uuid":len(wire(co))}
    return wire([2,groups,dc,pp,cc,cr,hh,pi,co,mode]),breakdown,stats

def run(src,tmp):
    d,raw=load(src);order=list(raw);random.Random(416).shuffle(order)
    emit("FR5_RESULT",{"mode":"original","bytes":src.stat().st_size,**baseline_measure(src,False,order)})
    n=Normalizer(d,"split");stores=[(sid,n.shop(s,h)) for sid,(s,h) in raw.items()]
    pools=[p.values for p in n.pools];cparts=[p.values for p in n.cparts];recipes=n.crecipes.values;hist=n.hist.values
    reordered,rp,rh=remap(stores,pools,hist)
    variants=[
        ("split19",stores,pools,hist,["19"],False,False,"rows"),
        ("uuid19",stores,pools,hist,["19"],True,True,"rows"),
        ("frequency19",reordered,rp,rh,["19"],True,True,"rows"),
        ("frequency_delta",reordered,rp,rh,["19"],True,True,"delta"),
        ("frequency_bits",reordered,rp,rh,["19"],True,True,"bits"),
        ("best_codec_delta",reordered,rp,rh,["19","xz"],True,True,"delta"),
        ("best_codec_bits",reordered,rp,rh,["19","xz"],True,True,"bits"),
    ]
    # Reuse metadata compression across reference encodings; only mode differs.
    cache={}
    for name,ss,ps,hs,codecs,frontcode,shared,mode in variants:
        start=time.perf_counter();key=(ss is reordered,tuple(codecs),frontcode,shared)
        if key not in cache:
            meta,breakdown,stats=make_meta(d,n.groups,ps,cparts,recipes,hs,n.pc,codecs,frontcode,shared,"rows")
            cache[key]=(unwire(meta),breakdown,stats)
        m,breakdown,stats=cache[key];meta=wire([*m[:-1],mode])
        p=tmp/(name+".bin");index=[];c=z.ZstdCompressor(level=19)
        with p.open("wb") as f:
            f.write(b"FRN2"+b"\0"*12);f.write(meta)
            for sid,r in ss:
                b=c.compress(wire(transform(r,mode)));off=f.tell();f.write(b);index.append([sid,off,len(b),"19"])
            off=f.tell();f.write(wire(index));f.seek(4);f.write(struct.pack("<QI",off,len(meta)))
        build=time.perf_counter()-start
        r=Reader(p);assert wire(r.dictionary)==wire(d);assert set(r.lookup)==set(raw)
        for sid in order:assert wire(r.read(sid))==wire(raw[sid]),("mismatch",name,sid)
        r.close()
        opens=[];scans=[];times=[]
        for _ in range(3):
            t=time.perf_counter();r=Reader(p);opens.append(time.perf_counter()-t);t=time.perf_counter()
            for sid in order:
                q=time.perf_counter();r.read(sid);times.append(time.perf_counter()-q)
            scans.append(time.perf_counter()-t);r.close()
        emit("FR5_RESULT",{"mode":name,"bytes":p.stat().st_size,"metadata_bytes":len(meta),"shop_bytes":off-16-len(meta),"index_bytes":p.stat().st_size-off,"build_s":round(build,3),"exact":True,"open_ms":round(statistics.median(opens)*1000,3),"scan_ms":round(statistics.median(scans)*1000,3),"shop_p95_us":round(p95(times)*1e6,3),"peak_rss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,"breakdown":breakdown,"uuid":stats})
        p.unlink()
def main():
    root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text());emit("FR5_BATCH",m["batch_id"])
    for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
        p=root/part["path"];assert hashlib.sha256(p.read_bytes()).hexdigest()==part["sha256"]
        with tempfile.TemporaryDirectory() as tmp:run(p,Path(tmp))
if __name__=="__main__":main()
