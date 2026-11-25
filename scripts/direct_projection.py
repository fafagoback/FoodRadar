"""Bounded per-shop binary spool; no intermediate compression or SQLite."""
import tempfile
import msgpack

CARD_COLUMNS=("store_uuid","product_uuid","card","checksum","search_text","city","category",
    "discount_pct","savings","unit_price","rating","new_item","promo","price_drop","platform_promo",
    "latitude","longitude")
STORE_COLUMNS=("store_id","store_uuid","name","city","locality","rating","review_count","chunk_count",
    "latitude","longitude","address","order_url","first_seen","last_seen","status","is_open")

class DirectProjection:
    def __init__(self):
        self.cards={};self.stores={};self.current=[];self.metadata={}
        self.count=0
        self.spool=tempfile.TemporaryFile(mode="w+b")
        self.validation_report=None
        self.spool_bytes=0
    def add_card(self,row):
        if len(row)!=len(CARD_COLUMNS):raise ValueError("Invalid card contract")
        self.validation_report=None
        self.current.append(tuple(row));self.count+=1
    def add_store(self,row):
        sid=row[1]
        if sid in self.stores:raise ValueError("Duplicate web storefront")
        if any(r[0]!=sid for r in self.current):raise ValueError("Cross-store card batch")
        self.stores[sid]=dict(zip(STORE_COLUMNS,row))
        self.validation_report=None
        if self.current:
            data=msgpack.packb(sorted(self.current,key=lambda x:x[1]),use_bin_type=True)
            offset=self.spool.seek(0,2)
            self.spool.write(data)
            self.cards[sid]=(offset,len(data))
            self.spool_bytes+=len(data)
        self.current=[]
    def rows(self):
        for sid,(offset,size) in sorted(self.cards.items()):
            self.spool.seek(offset)
            data=self.spool.read(size)
            if len(data)!=size:raise ValueError("Truncated projection spool")
            for row in msgpack.unpackb(data,raw=False):yield dict(zip(CARD_COLUMNS,row))
    def close(self):
        self.spool.close()
    def store_rows(self):
        return [dict(row) for _,row in sorted(self.stores.items())]
    def checksums(self):
        for row in self.rows():yield row["store_uuid"],row["product_uuid"],row["checksum"]
    def verified_checksums(self):
        import hashlib,json
        count=0
        last=None
        for row in self.rows():
            key=(row["store_uuid"],row["product_uuid"])
            if last is not None and key<=last:raise ValueError("Duplicate/out-of-order web card")
            last=key
            if key[0] not in self.stores:raise ValueError("Missing card storefront")
            card=row["card"]
            if hashlib.sha256(card.encode("utf-8")).hexdigest()!=row["checksum"]:
                raise ValueError("Card checksum mismatch")
            fields=json.loads(card)
            if (fields["store_uuid"],fields["product_uuid"])!=key:raise ValueError("Card identity mismatch")
            count+=1
            yield key[0],key[1],row["checksum"]
        if count!=self.count or self.current:raise ValueError("Incomplete direct projection")
    def validate(self):
        for _ in self.verified_checksums():pass
        self.validation_report={"store_directory":len(self.stores),"web_products":self.count,
            "bytes":self.spool_bytes,"projection":"direct-spool",
            "checksum_round_trip":"passed","metadata":self.metadata}
        return self.validation_report
