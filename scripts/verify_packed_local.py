"""Stream every packed blob, verify checksums and references before upload."""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
import msgpack
import zstandard


def verify_static_projection(conn, meta, path):
    """Audit all static cards and store IDs; no obsolete Turso bundle checks."""
    seen=set()
    for (sid,) in conn.execute("SELECT store_uuid FROM store_directory"):
        if sid in seen:raise ValueError("Duplicate storefront in directory")
        seen.add(sid)
    expected=meta.get("published_product_cards")
    rows=0
    for sid,pid,card,checksum in conn.execute(
            "SELECT store_uuid,product_uuid,card,checksum FROM web_products ORDER BY store_uuid,product_uuid"):
        if sid not in seen or hashlib.sha256(card.encode("utf-8")).hexdigest()!=checksum:
            raise ValueError("Invalid store reference or product card checksum")
        values=json.loads(card)
        if values.get("store_uuid")!=sid or values.get("product_uuid")!=pid:
            raise ValueError("Wrong product identity in card")
        rows+=1
    if rows!=expected or len(seen)!=meta["source_counts"]["stores"]:
        raise ValueError("Static projection source/card counts mismatch")
    index_names={r[1] for r in conn.execute("PRAGMA index_list(web_products)")}
    if "sqlite_autoindex_web_products_1" not in index_names:
        raise ValueError("Missing indexed product identity")
    return {"store_directory":len(seen),"web_products":rows,"bytes":Path(path).stat().st_size,
            "projection":"static-only","checksum_round_trip":"passed","metadata":meta}


def verify(path):
    conn = sqlite3.connect(f'{Path(path).resolve().as_uri()}?mode=ro', uri=True)
    try:
        if conn.execute('pragma integrity_check').fetchone()[0] != 'ok':
            raise ValueError('SQLite integrity check failed')
        meta = {k: json.loads(v) for k, v in conn.execute('select key,value from metadata')}
        for key in ('definition_version', 'source_revision', 'latest_batch'):
            if not meta.get(key):
                raise ValueError(f'Missing release metadata: {key}')
        if meta.get("projection")=="static-only":
            return verify_static_projection(conn,meta,path)
        dec = zstandard.ZstdDecompressor()
        counts = {}
        product_counts = {}
        bundle_counts = {}
        store_columns = {}
        total_products = 0
        for table in ('store_bundles', 'search_buckets', 'auxiliary_bundles'):
            counts[table] = 0
            order = 'store_id,chunk_no' if table == 'store_bundles' else '1'
            for row in conn.execute(f'select * from {table} order by {order}'):
                payload, checksum, raw_bytes = row[-1], row[-2], row[-3]
                raw = dec.decompress(payload)
                if len(raw) != raw_bytes or hashlib.sha256(raw).hexdigest() != checksum:
                    raise ValueError(f'{table}: payload checksum/length mismatch')
                value = msgpack.unpackb(raw, raw=False)
                if table == 'store_bundles':
                    sid, chunk = row[:2]
                    if chunk != bundle_counts.get(sid, 0):
                        raise ValueError('Non-contiguous store chunks')
                    bundle_counts[sid] = chunk + 1
                    if chunk == 0:
                        store_columns[sid] = value['product_columns']
                        if not value.get('store'):
                            raise ValueError('Store missing from chunk zero')
                    cols = store_columns[sid]
                    if not cols or any(len(pair[1]) != len(cols) for pair in value['products']):
                        raise ValueError('Product column contract mismatch')
                    product_counts[sid] = product_counts.get(sid, 0) + len(value['products'])
                    total_products += len(value['products'])
                elif table == 'search_buckets':
                    for refs in value.values():
                        for ref in refs:
                            if ref >> 20 not in product_counts or (ref & ((1 << 20)-1)) >= product_counts[ref >> 20]:
                                raise ValueError('Dangling product search reference')
                counts[table] += 1
        stores = conn.execute('select count(*) from store_directory').fetchone()[0]
        for sid, chunks in conn.execute('select store_id,chunk_count from store_directory'):
            if bundle_counts.get(sid) != chunks:
                raise ValueError('Store directory chunk count mismatch')
        if total_products != meta['products'] or counts['search_buckets'] != meta['buckets']:
            raise ValueError('Metadata count mismatch')
        report = {**counts, 'stores': stores, 'products': total_products,
                  'bytes': Path(path).stat().st_size, 'metadata': meta, 'checksum_round_trip': 'passed'}
        return report
    finally:
        conn.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('database'); p.add_argument('--report', required=True)
    a = p.parse_args(); report = verify(a.database)
    Path(a.report).write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))
