"""Compressed, read-on-demand web projection. Not a second SQLite DB.

Each shop's final card rows are compressed once and retained until the static
compiler consumes them. Browser blobs and checksums are identical to the
previous SQL-backed publisher.
"""
from compact_checkpoint import _pack,_unpack

CARD_COLUMNS=("store_uuid","product_uuid","card","checksum","search_text","city","category",
    "discount_pct","savings","unit_price","rating","new_item","promo","price_drop","platform_promo",
    "latitude","longitude")
STORE_COLUMNS=("store_id","store_uuid","name","city","locality","rating","review_count","chunk_count",
    "latitude","longitude","address","order_url","first_seen","last_seen","status","is_open")

class DirectProjection:
    def __init__(self):
        self.cards={};self.stores={};self.current=[];self.metadata={}
        self.count=0
    def add_card(self,row):
        if len(row)!=len(CARD_COLUMNS):raise ValueError("Invalid card contract")
        self.current.append(tuple(row));self.count+=1
    def add_store(self,row):
        sid=row[1]
        if sid in self.stores:raise ValueError("Duplicate web storefront")
        if any(r[0]!=sid for r in self.current):raise ValueError("Cross-store card batch")
        self.stores[sid]=dict(zip(STORE_COLUMNS,row))
        if self.current:self.cards[sid]=_pack(sorted(self.current,key=lambda x:x[1]))
        self.current=[]
    def rows(self):
        for sid,blob in sorted(self.cards.items()):
            for row in _unpack(blob):yield dict(zip(CARD_COLUMNS,row))
    def store_rows(self):
        return [dict(row) for _,row in sorted(self.stores.items())]
    def checksums(self):
        for row in self.rows():yield row["store_uuid"],row["product_uuid"],row["checksum"]
    def validate(self):
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
        if count!=self.count or self.current:raise ValueError("Incomplete direct projection")
        return {"store_directory":len(self.stores),"web_products":count,
            "bytes":sum(len(b) for b in self.cards.values()),"projection":"direct-compact",
            "checksum_round_trip":"passed","metadata":self.metadata}
