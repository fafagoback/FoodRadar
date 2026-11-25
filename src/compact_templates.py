"""Exact shared immutable catalogue text, never shared branch price or SKU history.

Rows stay in their original shop order. A pooled row is shorter by two columns;
the first element is an integer reference to a tuple of existing dictionary
tokens. Full-length legacy rows are always readable. Only exact tuples occurring
in multiple different shops are eligible. An update never changes a pool entry.
"""
from collections import Counter

KEY="__catalog_templates_v1"
FIELDS=("product_name","category","description")


class Templates:
    def __init__(self,d):
        self.columns=d.get("__product_columns",[])
        self.width=len(self.columns)
        self.pool=d.get(KEY,[])
        self.shared=tuple(self.columns.index(c) for c in FIELDS) if all(c in self.columns for c in FIELDS) else ()
        self.local=tuple(i for i in range(self.width) if i not in self.shared)
        self.contiguous=(len(self.shared)==3 and self.shared==tuple(range(self.shared[0],self.shared[0]+3)))
        if self.pool and (len(self.shared)!=3 or any(len(v)!=3 for v in self.pool)):
            raise ValueError("Invalid compact product template pool")
        self.index={tuple(v):i for i,v in enumerate(self.pool)}
        if len(self.index)!=len(self.pool):
            raise ValueError("Duplicate compact template")

    def key(self,row):
        return tuple(row[i] for i in self.shared)

    def decode(self,row):
        if len(row)==self.width:
            return row
        if (not self.pool or len(row)!=len(self.local)+1 or type(row[0]) is not int
                or not 0<=row[0]<len(self.pool)):
            raise ValueError("Invalid compact catalogue reference")
        if self.contiguous:
            i=self.shared[0]+1
            return row[1:i]+self.pool[row[0]]+row[i:]
        out=[None]*self.width
        for i,v in zip(self.shared,self.pool[row[0]]):out[i]=v
        for i,v in zip(self.local,row[1:]):out[i]=v
        return out

    def rows(self,rows):
        return [self.decode(row) for row in rows] if self.pool else rows

    def encode(self,row):
        if not self.pool:
            return row
        ref=self.index.get(self.key(row))
        if ref is None:return row
        if self.contiguous:
            i=self.shared[0];return [ref]+row[:i]+row[i+3:]
        return [ref]+[row[i] for i in self.local]

    def encode_rows(self,rows):
        return [self.encode(row) for row in rows] if self.pool else rows


def choose_pool(codec,shop_rows,min_shops=2):
    if not codec.shared:return [],{"shared_groups":0,"cross_shop_rows":0}
    occurrences=Counter()
    shops=Counter()
    for rows in shop_rows:
        keys=[codec.key(r) for r in codec.rows(rows)]
        occurrences.update(keys)
        shops.update(set(keys))
    # Each field is an existing string dictionary index or None. Count only
    # tuples shared across stores, not repeated rows within one store.
    eligible=[k for k,n in shops.items() if n>=min_shops]
    eligible.sort(key=lambda k:(-occurrences[k],repr(k)))
    return [list(k) for k in eligible],{"shared_groups":len(eligible),
           "cross_shop_rows":sum(occurrences[k] for k in eligible)}
