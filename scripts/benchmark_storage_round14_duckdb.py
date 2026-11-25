"""Round 14: real native column store, no per-shop compressed blobs.
Pinned production checkpoint; no crawl, HF write, publishing, or cache writes.
DuckDB stores individual typed columns and normalized history variants.
Exact original MessagePack values and ordering must survive reconstruction.
"""
from __future__ import annotations
import hashlib,json,math,random,resource,sqlite3,statistics,subprocess,sys,tempfile,time,shutil
from pathlib import Path
import duckdb,pyarrow as pa,zstandard as z
from benchmark_normalized_storage import wire,unwire,load,ro,emit,p95,inflate

FINGERPRINT="90af51959743bd61ecb897fae86be11b003e66c3b24d0f66bcb6a120570f7662"
def native_type(vals):
    non=[v for v in vals if v is not None]
    types={type(v) for v in non}
    if not non:return pa.int64()
    if types=={bool}:return pa.bool_()
    if types=={int} and min(non)>=-(1<<63) and max(non)<(1<<63):return pa.int64()
    if types=={float}:return pa.float64()
    if types=={str}:return pa.string()
    if types=={bytes}:return pa.binary()
    if types=={list}:
        typ=native_type([x for v in non for x in v])
        if typ is not None:return pa.list_(typ)
    return None

def make_table(prefix,columns,keys):
    out={k:pa.array(v,type=t) for k,v,t in keys};modes=[]
    for i,values in enumerate(columns):
        typ=native_type(values);mode="native" if typ is not None else "wire"
        if mode=="wire":values=[wire(v) for v in values];typ=pa.binary()
        out[f"{prefix}{i}"]=pa.array(values,type=typ)
        modes.append(mode)
    return pa.table(out),modes
def decode(values,modes):
    return [unwire(v) if m=="wire" else v for v,m in zip(values,modes)]

def rows_to_columns(rows,n):
    return [list(c) for c in zip(*rows)] if rows else [[] for _ in range(n)]

def dictionary_tables(d):
    meta=[];rows=[]
    for i,(key,value) in enumerate(d.items()):
        islist=type(value) is list;values=value if islist else [value]
        meta.append((i,key,islist,len(values)))
        for j,v in enumerate(values):
            t=type(v);tag=0;iv=fv=sv=bv=None
            if v is None:pass
            elif t is bool:tag=1;iv=int(v)
            elif t is int and -(1<<63)<=v<(1<<63):tag=2;iv=v
            elif t is float:tag=3;fv=v
            elif t is str:tag=4;sv=v
            elif t is bytes:tag=5;bv=v
            else:tag=6;bv=wire(v)
            rows.append((i,j,tag,iv,fv,sv,bv))
    def table(rs,names,types):
        return pa.table({name:pa.array([r[i] for r in rs],type=t) for i,(name,t) in enumerate(zip(names,types))})
    return (table(meta,["key_id","name","is_list","n"],[pa.int32(),pa.string(),pa.bool_(),pa.int32()]),
            table(rows,["key_id","ordinal","tag","iv","fv","sv","bv"],[pa.int32(),pa.int32(),pa.int8(),pa.int64(),pa.float64(),pa.string(),pa.binary()]))
def restore_dict(db):
    meta=db.execute("SELECT * FROM dictionary_keys ORDER BY key_id").fetchall()
    values=[[] for _ in meta]
    for k,j,tag,iv,fv,sv,bv in db.execute("SELECT * FROM dictionary_values ORDER BY key_id,ordinal").fetchall():
        v={0:lambda:None,1:lambda:bool(iv),2:lambda:iv,3:lambda:fv,4:lambda:sv,5:lambda:bv,6:lambda:unwire(bv)}[tag]()
        assert j==len(values[k]);values[k].append(v)
    result={}
    for i,key,islist,n in meta:
        assert n==len(values[i])
        result[key]=values[i] if islist else values[i][0]
    return result

def build(p,d,raw):
    ids=sorted(raw);headers=[];product_rows=[];product_shop=[];product_ord=[]
    group_shop=[];group_order=[];group_ord=[];variant_shop=[];variant_group=[];variant_order=[];variant_rows=[]
    for k,sid in enumerate(ids):
        (header,products),history=raw[sid];headers.append(header)
        for j,row in enumerate(products):
            product_shop.append(k);product_ord.append(j);product_rows.append(row)
        for j,(ordinal,variants) in enumerate(history):
            group_shop.append(k);group_order.append(j);group_ord.append(ordinal)
            for vpos,variant in enumerate(variants):
                variant_shop.append(k);variant_group.append(j);variant_order.append(vpos);variant_rows.append(variant)
    shops,hm=make_table("h",rows_to_columns(headers,len(d["__store_columns"])),[("shop_id",list(range(len(ids))),pa.int32()),("sid",ids,pa.binary())])
    products,pm=make_table("p",rows_to_columns(product_rows,len(d["__product_columns"])),[("shop_id",product_shop,pa.int32()),("ordinal",product_ord,pa.int32())])
    groups=pa.table({"shop_id":pa.array(group_shop,type=pa.int32()),"group_order":pa.array(group_order,type=pa.int32()),"product_ordinal":pa.array(group_ord,type=pa.int32())})
    # Native list-valued history fields where possible; only individually mixed cells
    # use exact typed serialization, never a compressed shop record.
    variants,vm=make_table("v",rows_to_columns(variant_rows,len(variant_rows[0]) if variant_rows else 0),[("shop_id",variant_shop,pa.int32()),("group_order",variant_group,pa.int32()),("variant_order",variant_order,pa.int32())])
    dk,dv=dictionary_tables(d)
    db=duckdb.connect(str(p));db.execute("SET threads=2")
    for name,tab in [("shops",shops),("products",products),("history_groups",groups),("history_variants",variants),("dictionary_keys",dk),("dictionary_values",dv)]:
        db.register("incoming",tab);db.execute(f"CREATE TABLE {name} AS SELECT * FROM incoming");db.unregister("incoming")
    db.execute("CREATE TABLE schema_modes(name VARCHAR, payload VARCHAR)")
    db.executemany("INSERT INTO schema_modes VALUES (?,?)",[(k,json.dumps(v)) for k,v in (("header",hm),("product",pm),("variant",vm))])
    db.execute("CHECKPOINT")
    inventory={}
    for name in ("shops","products","history_groups","history_variants","dictionary_keys","dictionary_values","schema_modes"):
        info=db.execute(f"PRAGMA storage_info('{name}')").fetchdf()
        blocks={int(x) for x in info["block_id"].dropna() if int(x)>=0}
        inventory[name]={"rows":db.execute(f"SELECT count(*) FROM {name}").fetchone()[0],"referenced_blocks":len(blocks)}
    db.close()
    emit("FR14_LAYOUT",{"tables":inventory,"product_fields":dict(zip(d["__product_columns"],pm)),"header_fields":dict(zip(d["__store_columns"],hm)),"variant_modes":vm,"note":"blocks may overlap across tables; total file bytes measured separately"})
    return pm

class Reader:
    def __init__(self,p,base=False,writable=False):
        self.base=base
        if base:
            self.db=ro(p);self.dictionary={k:inflate(v) for k,v in self.db.execute("SELECT key,payload FROM dictionary ORDER BY key")}
            self.ids={r[0]:r[0] for r in self.db.execute("SELECT store FROM shop_state")}
        else:
            self.db=duckdb.connect(str(p),read_only=not writable);self.db.execute("SET threads=2")
            self.dictionary=restore_dict(self.db)
            self.ids={sid:i for i,sid in self.db.execute("SELECT shop_id,sid FROM shops").fetchall()}
            self.modes={k:json.loads(v) for k,v in self.db.execute("SELECT * FROM schema_modes").fetchall()}
    def read(self,sid):
        db=self.db
        if self.base:
            s,h=db.execute("SELECT s.payload,h.payload FROM shop_state s JOIN price_history h USING(store) WHERE s.store=?",(sid,)).fetchone()
            return inflate(s),inflate(h)
        k=self.ids[sid];m=self.modes
        header=decode(db.execute("SELECT * FROM shops WHERE shop_id=?",(k,)).fetchone()[2:],m["header"])
        rows=[decode(r[2:],m["product"]) for r in db.execute("SELECT * FROM products WHERE shop_id=? ORDER BY ordinal",(k,)).fetchall()]
        groups=[[ordinal,[]] for _,_,ordinal in db.execute("SELECT * FROM history_groups WHERE shop_id=? ORDER BY group_order",(k,)).fetchall()]
        for row in db.execute("SELECT * FROM history_variants WHERE shop_id=? ORDER BY group_order,variant_order",(k,)).fetchall():
            groups[row[1]][1].append(decode(row[3:],m["variant"]))
        return [header,rows],groups
    def close(self):self.db.close()

def measure(p,base):
    opens=[];scans=[];queries=[];counts=[]
    for _ in range(3):
        t=time.perf_counter();r=Reader(p,base);opens.append((time.perf_counter()-t)*1000)
        ids=list(r.ids);random.Random(416).shuffle(ids);t=time.perf_counter()
        n=0
        for sid in ids:
            q=time.perf_counter();s,h=r.read(sid);queries.append((time.perf_counter()-q)*1e6);n+=len(s[1])
        scans.append((time.perf_counter()-t)*1000);counts.append(n);r.close()
    assert len(set(counts))==1
    return dict(open_ms=statistics.median(opens),scan_ms=statistics.median(scans),shop_p95_us=p95(queries),peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,products=counts[0])

def independent_measure(p,base):
    out=subprocess.check_output([sys.executable,__file__,"--measure",str(p),"1" if base else "0"],text=True)
    return json.loads(out)

def test(src,tmp):
    d,raw=load(src);p=tmp/"native.duckdb"
    ids=list(raw);random.Random(416).shuffle(ids)
    emit("FR14_BASE",{"bytes":src.stat().st_size,"shops":len(ids),"products":sum(len(s[1]) for s,h in raw.values()),**independent_measure(src,True)})
    t=time.perf_counter();pm=build(p,d,raw);build_s=time.perf_counter()-t
    size=p.stat().st_size
    r=Reader(p);assert wire(r.dictionary)==wire(d);assert set(r.ids)==set(raw)
    for sid in ids:assert wire(r.read(sid))==wire(raw[sid]),("roundtrip",sid)
    r.close()
    metrics=independent_measure(p,False)
    emit("FR14_READ_RESULT",{"bytes":size,"build_s":build_s,"exact":True,"shops":len(ids),**metrics})
    # True SQL transaction, not a whole-shop replacement: mutate one price cell,
    # read all shops, then restore. This is isolation only, not lifecycle regression.
    sid=next(k for k in ids if raw[k][0][1] and type(raw[k][0][1][0][d["__product_columns"].index("price")]) in (int,float))
    i=d["__product_columns"].index("price");s,h=unwire(wire(raw[sid]));s[1][0][i]+=1
    value=s[1][0][i];value=wire(value) if pm[i]=="wire" else value
    db=duckdb.connect(str(p));k=db.execute("SELECT shop_id FROM shops WHERE sid=?",(sid,)).fetchone()[0]
    t=time.perf_counter();db.execute("BEGIN");db.execute(f"UPDATE products SET p{i}=? WHERE shop_id=? AND ordinal=0",(value,k));db.execute("COMMIT");db.execute("CHECKPOINT");update_ms=(time.perf_counter()-t)*1000;db.close()
    r=Reader(p)
    for key in ids:assert wire(r.read(key))==wire((s,h) if key==sid else raw[key]),("update isolation",key)
    r.close()
    after_update_bytes=p.stat().st_size
    # Baseline transaction against temporary copy; never alter the actual checkpoint.
    bp=tmp/"baseline-copy.db";shutil.copyfile(src,bp)
    db=sqlite3.connect(bp)
    payload=z.ZstdCompressor(level=10).compress(wire(s))
    t=time.perf_counter()
    with db:db.execute("UPDATE shop_state SET payload=? WHERE store=?",(payload,sid))
    base_update_ms=(time.perf_counter()-t)*1000;db.close()
    rr=Reader(bp,True);assert wire(rr.read(sid))==wire((s,h));rr.close()
    emit("FR14_RESULT",{"bytes":size,"after_update_bytes":after_update_bytes,"build_s":build_s,"exact":True,"incremental_isolation":True,"update_ms":update_ms,"base_update_ms":base_update_ms,**metrics,"business_regression":False,"website_verified":False})
def main():
    root=Path(sys.argv[1]);m=json.loads((root/"manifest.json").read_text())
    for part in m.get("parts") or [{"path":"serving.compact.db","sha256":m["compact_sha256"]}]:
        src=root/part["path"];digest=hashlib.sha256(src.read_bytes()).hexdigest()
        assert digest==part["sha256"]==FINGERPRINT,("unexpected fixed checkpoint",digest)
        emit("FR14_HASH",digest)
        with tempfile.TemporaryDirectory() as t:test(src,Path(t))
if __name__=="__main__":
    if sys.argv[1]=="--measure":print(json.dumps(measure(Path(sys.argv[2]),sys.argv[3]=="1")))
    else:main()
