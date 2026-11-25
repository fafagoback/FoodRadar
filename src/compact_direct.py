"""Native compact state reader/writer. No full relational database expansion.

The on-disk layout is the deployed v3 dictionary + per-shop compressed records.
Only a single shop is decoded at a time for the existing lifecycle rules. The
relational scratch connection is in memory and never contains other shops.
Immutable HF Raw remains the source of price proof.
"""
from __future__ import annotations
import json, sqlite3, contextlib
import zstandard as zstd
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from fractions import Fraction
from pathlib import Path
from compact_checkpoint import (_pack,_unpack,binary_uuid,text_uuid,KINDS,OMITTED,
    STORE_HASH_FIELDS,PRODUCT_HASH_FIELDS)
from compact_templates import Templates
from compact_payload import PayloadCodec, KEY as PAYLOAD_KEY
from serving_state import ensure_schema_and_migrations,canonical_hash,apply_snapshot,store_identity
from verified_weekly import TW,_two_decimals,iter_weekly_price_evidence

COMPACT_SCHEMA="""PRAGMA page_size=1024;
CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
"""
MAX_BYTES=20_000_000

def _same_wire(old, new):
    if old == new:
        return True
    decoder=zstd.ZstdDecompressor()
    return decoder.decompress(old)==decoder.decompress(new)

class CompactStore:
    def __init__(self,path,*,empty=False):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        if empty and self.path.exists():
            raise ValueError("Refusing to overwrite compact state")
        self.conn=sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=MEMORY")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        if empty:
            self.conn.executescript(COMPACT_SCHEMA)
            with contextlib.closing(sqlite3.connect(":memory:")) as db:
                ensure_schema_and_migrations(db)
                sc=[r[1] for r in db.execute("PRAGMA table_info(stores)") if r[1] not in ("store_uuid","state_hash")]
                pc=[r[1] for r in db.execute("PRAGMA table_info(products)") if r[1] not in OMITTED]
            self.d={"__store_columns":sc,"__product_columns":pc,"__batch_keys":[],
                    "__crawl_batches":[],"__metadata":[]}
        else:
            self.d={k:_unpack(v) for k,v in self.conn.execute("SELECT key,payload FROM dictionary")}
        self.sc=self.d["__store_columns"];self.pc=self.d["__product_columns"]
        self.templates=Templates(self.d)
        self.payload_codec=PayloadCodec(self.d.get(PAYLOAD_KEY))
        self.batches=[tuple(x) for x in self.d["__batch_keys"]]
        self.bidx={x:i for i,x in enumerate(self.batches)}
        self.tokens={}
        self.dirty=set(self.d) if empty else set()
        self.ids={text_uuid(x[0]) for x in self.conn.execute("SELECT store FROM shop_state")}
        if self.conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]!=len(self.ids):
            raise ValueError("Compact shop and price inventory differs")
        self.max_shop_bytes=0
        self.max_shop_products=0
        self.max_shop_observations=0
        self.max_scratch_bytes=0
        self.writes=0
        self.flush()

    def remove_image_urls(self):
        """Migrate verified legacy encoded rows in place; never touch price history."""
        keys=self.d.get("catalog_key",[])
        if "image_url" not in keys:
            if "catalog:image_url" in self.d:
                raise ValueError("Orphan image dictionary")
            return 0
        index=keys.index("image_url")
        ci=self.pc.index("catalog_web")
        before=self.path.stat().st_size
        removed=0
        # Read one shop at a time; all other encoded values stay exactly equal.
        with self.conn:
            for sid in sorted(self.ids):
                header,rows=self.payload_codec.unpack(self._payload(sid,"shop_state"))
                rows=self.templates.rows(rows)
                changed=False
                for row in rows:
                    old=row[ci]
                    new=[[k-int(k>index),v] for k,v in old if k!=index]
                    removed+=len(old)-len(new)
                    if new!=old:
                        row[ci]=new;changed=True
                if changed:self._replace("shop_state",sid,[header,self.templates.encode_rows(rows)])
            self.d["catalog_key"]=keys[:index]+keys[index+1:]
            self.d.pop("catalog:image_url",None)
            self.tokens.pop("catalog_key",None)
            self.tokens.pop("catalog:image_url",None)
            self.dirty.discard("catalog:image_url")
            self.conn.execute("DELETE FROM dictionary WHERE key='catalog:image_url'")
            self.conn.execute("UPDATE dictionary SET payload=? WHERE key='catalog_key'",(_pack(self.d["catalog_key"]),))
        self.conn.execute("VACUUM")
        if self.conn.execute("PRAGMA quick_check").fetchone()[0]!="ok":
            raise ValueError("Image removal integrity check failed")
        print("FOODRADAR_IMAGE_REMOVAL="+json.dumps({"before_bytes":before,"after_bytes":self.path.stat().st_size,"removed_references":removed}),flush=True)
        return removed

    def close(self):
        self.conn.close()
    def metadata(self):
        return dict(self.d["__metadata"])
    def batch_rows(self):
        return [tuple(r) for r in self.d["__crawl_batches"]]
    def set_globals(self,meta,batch_rows):
        self.d["__metadata"]=[list(r) for r in sorted(meta.items())]
        self.d["__crawl_batches"]=[list(r) for r in batch_rows]
        self.dirty.update(("__metadata","__crawl_batches"))
    def decode(self,col,val):
        if col.endswith("_uuid"):return text_uuid(val)
        if col=="catalog_web":
            return json.dumps({self.d["catalog_key"][k]:json.loads(self.d["catalog:"+self.d["catalog_key"][k]][i])
                               for k,i in val},ensure_ascii=False)
        return self.d[col][val] if col in self.d and isinstance(val,int) and not isinstance(val,bool) else val
    def token(self,col,val):
        if val is None or not isinstance(val,str):return val
        if col not in self.tokens:
            self.tokens[col]={v:i for i,v in enumerate(self.d.get(col,[]))}
        index=self.tokens[col]
        if val not in index:
            values=self.d.setdefault(col,[])
            index[val]=len(values);values.append(val);self.dirty.add(col)
        return index[val]
    def encode(self,col,val):
        if col.endswith("_uuid"):return binary_uuid(val)
        if col=="catalog_web":
            value=json.loads(val or "{}")
            if not isinstance(value,dict):raise ValueError("Unexpected catalog data")
            return [[self.token("catalog_key",k),self.token("catalog:"+k,json.dumps(v,ensure_ascii=False,separators=(",",":")))]
                    for k,v in sorted(value.items()) if k!="image_url"]
        return self.token(col,val)
    def _payload(self,sid,table):
        row=self.conn.execute(f"SELECT payload FROM {table} WHERE store=?",(binary_uuid(sid),)).fetchone()
        if row is None:raise ValueError("Missing compact "+table+" "+sid)
        self.max_shop_bytes=max(self.max_shop_bytes,len(row[0]))
        return row[0]
    def store_header(self,sid):
        state,_=self.payload_codec.unpack(self._payload(sid,"shop_state"))
        s={c:self.decode(c,v) for c,v in zip(self.sc,state)}
        s["store_uuid"]=sid
        s["state_hash"]=canonical_hash({k:s[k] for k in STORE_HASH_FIELDS})
        return s
    def _state(self,sid):
        state,rows=self.payload_codec.unpack(self._payload(sid,"shop_state"))
        rows=self.templates.rows(rows)
        store={c:self.decode(c,v) for c,v in zip(self.sc,state)}
        store["store_uuid"]=sid
        store["state_hash"]=canonical_hash({k:store[k] for k in STORE_HASH_FIELDS})
        products=[]
        for values in rows:
            p={c:self.decode(c,v) for c,v in zip(self.pc,values)}
            p["store_uuid"]=sid
            p["state_hash"]=canonical_hash({k:p[k] for k in PRODUCT_HASH_FIELDS})
            p["price_evidence_source_hash"]=None
            p["menu_quote_history"]="[]"
            products.append(p)
        self.max_shop_products=max(self.max_shop_products,len(products))
        return store,products
    def _quotes(self,sid,ids):
        quotes=[]
        for ordinal,variants in self.payload_codec.unpack(self._payload(sid,"price_history")):
            if not 0<=ordinal<len(ids):raise ValueError("Invalid SKU ordinal")
            pid=ids[ordinal]
            for fields,mask in variants:
                menu,kind,buy,free,received,original,payable=fields
                if not (menu>0 and received>0 and 0<=kind<len(KINDS)):raise ValueError("Invalid quote")
                bits=int.from_bytes(mask,"little")
                while bits:
                    least=bits&-bits;pos=least.bit_length()-1;bits^=least
                    if pos>=len(self.batches):raise ValueError("Unknown compact batch")
                    batch,at=self.batches[pos]
                    quotes.append((batch,sid,pid,at,menu,KINDS[kind],buy,free,received,original,payable,""))
        quotes.sort(key=lambda r:(r[2],r[3],r[0]))
        if len({(q[0],q[2]) for q in quotes})!=len(quotes):
            raise ValueError("Duplicate compact observation")
        self.max_shop_observations=max(self.max_shop_observations,len(quotes))
        return quotes
    def quote_rows(self,sid):
        _,rows=self.payload_codec.unpack(self._payload(sid,"shop_state"))
        rows=self.templates.rows(rows)
        index=self.pc.index("product_uuid")
        return self._quotes(sid,[text_uuid(r[index]) for r in rows])
    def read_shop(self,sid,*,derive_history=False):
        store,products=self._state(sid)
        quotes=self._quotes(sid,[p["product_uuid"] for p in products])
        if derive_history:
            histories=defaultdict(list)
            for _,_,pid,at,menu,kind,buy,free,received,_,payable,_ in quotes:
                if payable>0:
                    label="無" if kind=="NONE" else f"買{buy}送{free}" if kind=="SAME_SKU_BXGY" else "直接折價"
                    histories[pid].append([at,round(float(Fraction(payable,100*received)),2),
                                           menu/100.0,label,received])
            for p in products:
                cutoff=(datetime.fromisoformat(p["last_seen"])-timedelta(days=7)).isoformat()
                p["menu_quote_history"]=json.dumps(
                    [x for x in histories[p["product_uuid"]] if cutoff<=x[0]<=p["last_seen"]],
                    ensure_ascii=False)
        return store,products,quotes
    def _replace(self,table,sid,value):
        payload=self.payload_codec.pack(value);key=binary_uuid(sid)
        old=self.conn.execute(f"SELECT payload FROM {table} WHERE store=?",(key,)).fetchone()
        if old is None or (old[0]!=payload and self.payload_codec.raw(old[0])!=self.payload_codec.raw(payload)):
            self.conn.execute(f"INSERT OR REPLACE INTO {table} VALUES(?,?)",(key,payload))
            self.writes+=1
    def put_shop(self,store,products,quotes=None):
        sid=store["store_uuid"]
        products=sorted(products,key=lambda p:p["product_uuid"])
        self._replace("shop_state",sid,[[self.encode(c,store[c]) for c in self.sc],
            self.templates.encode_rows([[self.encode(c,p[c]) for c in self.pc] for p in products])])
        if quotes is not None:
            ids={p["product_uuid"]:i for i,p in enumerate(products)}
            groups=defaultdict(lambda:defaultdict(int))
            for batch,s,pid,at,menu,kind,buy,free,received,original,payable,_ in quotes:
                if s!=sid:raise ValueError("Cross-shop quote")
                key=(batch,at)
                if key not in self.bidx:
                    self.bidx[key]=len(self.batches);self.batches.append(key)
                    self.dirty.add("__batch_keys")
                v=(menu,KINDS.index(kind),buy,free,received,original,payable)
                mask=1<<self.bidx[key]
                if groups[ids[pid]][v]&mask:raise ValueError("Duplicate quote")
                groups[ids[pid]][v]|=mask
            self._replace("price_history",sid,[[i,[[list(v),m.to_bytes((m.bit_length()+7)//8,"little")]
                for v,m in sorted(g.items())]] for i,g in sorted(groups.items())])
        elif sid not in self.ids:
            raise ValueError("New shop missing price history")
        self.ids.add(sid)
    def flush(self):
        self.d["__batch_keys"]=[list(x) for x in self.batches]
        for k in sorted(self.dirty):
            payload=_pack(self.d[k])
            previous=self.conn.execute("SELECT payload FROM dictionary WHERE key=?",(k,)).fetchone()
            if previous is None or not _same_wire(previous[0],payload):
                self.conn.execute("INSERT OR REPLACE INTO dictionary VALUES(?,?)",(k,payload))
        self.conn.commit();self.dirty.clear()
    def update_shop(self,sid,doc,batch,previous,baseline):
        """Apply the same lifecycle code to one shop in a memory-only connection.

        Old observations are not copied into SQL: lifecycle reads current fields
        and bounded recent quotes; the new native observations append to the
        compact bitsets. Global weekly rules are recalculated separately.
        """
        old=self.read_shop(sid,derive_history=True) if sid in self.ids else None
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            from working_quotes import initialize
            db.row_factory=sqlite3.Row
            db.execute("PRAGMA temp_store=MEMORY")
            initialize(db);ensure_schema_and_migrations(db)
            if previous:db.execute("INSERT INTO metadata VALUES('latest_batch',?)",(previous,))
            def insert(table,row):
                cols=list(row)
                db.execute(f"INSERT INTO {table}({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                           tuple(row[c] for c in cols))
            if old:
                insert("stores",old[0])
                for p in old[1]:insert("products",p)
            counts=apply_snapshot(db,[doc],batch,baseline=baseline,refresh_search=False)
            store=db.execute("SELECT * FROM stores").fetchone()
            if store is not None:
                products=[dict(r) for r in db.execute("SELECT * FROM products ORDER BY product_uuid")]
                new_quotes=[tuple(r) for r in db.execute("SELECT * FROM product_price_observations")]
                self.put_shop(dict(store),products,(old[2] if old else [])+new_quotes)
            scratch=db.execute("PRAGMA page_count").fetchone()[0]*db.execute("PRAGMA page_size").fetchone()[0]
            self.max_scratch_bytes=max(self.max_scratch_bytes,scratch)
            return counts

class QuoteWindow:
    """Only the canonical evidence cursor, not a SQL database."""
    def __init__(self,quotes):self.quotes=quotes
    def execute(self,sql,args):
        if "FROM product_price_observations" not in sql or len(args)!=2:raise ValueError("Unknown quote query")
        start,end=args
        return ((q[1],q[2],q[0],q[3],q[10],q[8]) for q in self.quotes if start<=q[3]<=end)

def weekly_shop(store,products,quotes,batch):
    """Calculate the same exact native seven-day/14-day ABA projection."""
    now=datetime.strptime(batch,"%Y%m%d%H%M%S").replace(tzinfo=TW).isoformat()
    evidence={pid:(cur,prior,blocked) for _,pid,cur,prior,blocked in
              iter_weekly_price_evidence(QuoteWindow(quotes),batch)}
    suppressed=sum(v[2] for v in evidence.values());deals=0;changed=False
    defaults={"seven_day_reference_price":None,"seven_day_discount_pct":0.0,
        "seven_day_discount_amount":0.0,"seven_day_is_price_deal":0,
        "archive_reference_price":None,"archive_discount_pct":0.0,
        "archive_discount_amount":0.0,"archive_evidence_count":0,"archive_is_price_deal":0}
    for p in products:
        update=dict(defaults)
        current=evidence.get(p["product_uuid"])
        if (current and p["last_seen"]==now and p["price_evidence_status"]=="VERIFIED_MENU"
            and p["delivery_available"]==1 and p["is_open"]==1 and p["status"]=="active"
            and store["status"]=="active" and store["is_open"]==1 and store["last_seen"]==p["last_seen"]):
            unit,prior,blocked=current
            if not blocked:
                highest=max(prior);difference=max(Fraction(0),highest-unit)
                pct=_two_decimals(100*difference/highest)
                update.update(seven_day_reference_price=_two_decimals(highest),seven_day_discount_pct=pct,
                    seven_day_discount_amount=_two_decimals(difference),seven_day_is_price_deal=int(difference>0 and pct>=30))
        changed|=any(p[k]!=v for k,v in update.items())
        p.update(update);deals+=update["seven_day_is_price_deal"]
    return changed,{"qualified_comparisons":len(evidence),"oscillation_suppressed":suppressed,"deals":deals}

class CompactCollection:
    """One logical compact database, optionally split into bounded files."""
    def __init__(self,parts):
        if not parts:raise ValueError("No compact partitions")
        self.parts=parts;self.lookup={}
        self.meta=parts[0].metadata();self.batches=parts[0].batch_rows()
        for part in parts:
            if part.metadata()!=self.meta or part.batch_rows()!=self.batches:raise ValueError("Partition batch metadata differs")
            for sid in part.ids:
                if sid in self.lookup:raise ValueError("Duplicate partition store")
                self.lookup[sid]=part
    def close(self):
        for part in self.parts:part.close()
    def ids(self):return sorted(self.lookup)
    def stores(self):
        return (self.lookup[sid].store_header(sid) for sid in self.ids())
    def shop(self,sid,*,derive_history=False):return self.lookup[sid].read_shop(sid,derive_history=derive_history)
    def products(self,sid):return self.lookup[sid]._state(sid)[1]
    def quote_shops(self):
        for sid in self.ids():yield self.lookup[sid].quote_rows(sid)
    def shops(self):
        for sid in self.ids():yield self.shop(sid)
    def metadata(self):return self.meta
    def batch_rows(self):return self.batches
    def apply_batch(self,docs,batch,excluded=()):
        previous=self.meta.get("latest_batch","")
        if previous and batch<=previous:raise ValueError("Nonmonotonic compact update")
        by_store={}
        for doc in docs:
            sid,_=store_identity(doc)
            by_store.setdefault(sid,doc)
        excluded=set(excluded)
        if excluded&set(by_store):raise ValueError("Verified and excluded shop overlap")
        from serving_state import DEFAULT_MISSING_THRESHOLD
        counts=Counter()
        for sid,doc in by_store.items():
            part=self.lookup.get(sid)
            if part is None:
                # Until the current partition reaches its byte budget, add
                # new shops there. Oversized partitions split at flush.
                part=self.parts[-1]
            counts.update(part.update_shop(sid,doc,batch,previous,not self.batches))
            if sid in part.ids:self.lookup[sid]=part
        for sid in self.ids():
            if sid in by_store or sid in excluded:continue
            part=self.lookup[sid]
            store=part.store_header(sid)
            if store["status"]=="active":
                store,products=part._state(sid)
                store["missing_streak"]+=1
                if store["missing_streak"]>=DEFAULT_MISSING_THRESHOLD:
                    store["status"]="inactive";counts["store_removed"]+=1
                part.put_shop(store,products)
        now=datetime.strptime(batch,"%Y%m%d%H%M%S").replace(tzinfo=TW).isoformat()
        self.batches.append((batch,now,counts["stores"],counts["products"],counts["fallback_stores"],counts["fallback_products"]))
        self.meta["latest_batch"]=batch
        self.flush()
        return dict(counts)
    def refresh_weekly(self,batch):
        results=Counter()
        for store,products,quotes in self.shops():
            changed,summary=weekly_shop(store,products,quotes,batch)
            results.update(summary)
            if changed:self.lookup[store["store_uuid"]].put_shop(store,products)
        self.flush()
        return dict(results)
    def flush(self):
        for part in self.parts:
            part.set_globals(self.meta,self.batches)
            part.flush()
    def stats(self):
        counts=Counter()
        for sid in self.ids():
            part=self.lookup[sid]
            _,rows=part.payload_codec.unpack(part._payload(sid,"shop_state"))
            quotes=part.payload_codec.unpack(part._payload(sid,"price_history"))
            counts["stores"]+=1;counts["products"]+=len(rows)
            counts["observations"]+=sum(int.from_bytes(mask,"little").bit_count()
                    for _,variants in quotes for _,mask in variants)
        return dict(counts)
    def metrics(self):
        return {"compact_bytes":sum(p.path.stat().st_size for p in self.parts),
                "max_shop_scratch_bytes":max(p.max_scratch_bytes for p in self.parts),
                "max_shop_products":max(p.max_shop_products for p in self.parts),
                "max_shop_observations":max(p.max_shop_observations for p in self.parts),
                "modified_payloads":sum(p.writes for p in self.parts)}
    def rebuild_partitions(self,folder):
        """Drop unused dictionary tokens; split by actual bytes when needed.

        Each output partition is bounded by MAX_BYTES. Only compact source and
        compact output coexist; no complete normalized SQL database is made.
        """
        import shutil,tempfile
        folder=Path(folder)
        with tempfile.TemporaryDirectory(dir=folder) as tmp:
            tmp=Path(tmp);out=[]
            # Stable geographic order, with UUID tie breaker.
            sorted_ids=[s["store_uuid"] for s in sorted(self.stores(),key=lambda s:(
                int((s.get("latitude") or 0)*10),int((s.get("longitude") or 0)*10),s["store_uuid"]))]
            def emit(ids):
                path=tmp/f"part-{len(out):05d}.db"
                part=CompactStore(path,empty=True)
                for sid in ids:
                    s,p,q=self.shop(sid);part.put_shop(s,p,q)
                part.set_globals(self.meta,self.batches);part.flush()
                part.conn.execute("VACUUM");part.close()
                if path.stat().st_size>=MAX_BYTES:
                    path.unlink()
                    if len(ids)==1:raise ValueError("One shop exceeds compact byte budget")
                    mid=len(ids)//2;emit(ids[:mid]);emit(ids[mid:])
                else:out.append(path)
            emit(sorted_ids)
            old_metrics=self.metrics()
            self.close()
            for p in self.parts:p.path.unlink(missing_ok=True)
            target=folder/"shards"
            shutil.rmtree(target,ignore_errors=True)
            if len(out)==1:
                dest=folder/"serving.compact.db";out[0].replace(dest);paths=[dest]
            else:
                target.mkdir();paths=[]
                for path in out:
                    dest=target/path.name;path.replace(dest);paths.append(dest)
            replacement=CompactCollection([CompactStore(p) for p in paths])
            replacement.parts[0].max_scratch_bytes=old_metrics["max_shop_scratch_bytes"]
            replacement.parts[0].max_shop_products=old_metrics["max_shop_products"]
            replacement.parts[0].max_shop_observations=old_metrics["max_shop_observations"]
            replacement.parts[0].writes=old_metrics["modified_payloads"]
            return replacement

def open_collection(paths):
    return CompactCollection([CompactStore(p) for p in paths])
