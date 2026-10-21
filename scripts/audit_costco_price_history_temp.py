"""Read-only live Turso audit for one Costco product, no database writes."""
import os,json,base64,urllib.request,datetime
import msgpack,zstandard
target='c05d9d19-631b-50bf-a275-b9827c0b014e'
url=os.environ['TURSO_DATABASE_URL'].replace('libsql://','https://').rstrip('/')
token=os.environ['TURSO_READONLY_TOKEN']
def query(sql):
    body=json.dumps({'requests':[{'type':'execute','stmt':{'sql':sql}},{'type':'close'}]}).encode()
    req=urllib.request.Request(url+'/v2/pipeline',body,{'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=90) as resp:data=json.load(resp)
    ans=data['results'][0]
    if ans['type']!='ok':raise RuntimeError(str(ans.get('error')))
    res=ans['response']['result']
    cols=[x['name'] for x in res['cols']]
    return [dict(zip(cols,[(base64.b64decode(v['base64']) if v['type']=='blob' else v.get('value')) for v in row])) for row in res['rows']]
directory=query("SELECT store_id,store_uuid,name,last_seen,is_open,status FROM store_directory WHERE name LIKE '%COSTCO%' OR name LIKE '%好市多%' ORDER BY store_id")
print("STORES",json.dumps(directory,ensure_ascii=False),flush=True)
for store in directory:
    id=int(store['store_id'])
    blocks=query(f"SELECT chunk_no,payload FROM store_bundles WHERE store_id={id} ORDER BY chunk_no")
    products=[];events=[];pcols=ecols=None
    for row in blocks:
        d=msgpack.unpackb(zstandard.ZstdDecompressor().decompress(row['payload']),raw=False)
        pcols=d.get('product_columns') or pcols
        ecols=d.get('event_columns') or ecols
        products.extend(dict(zip(pcols,p[1])) for p in d.get('products',[]))
        events.extend(dict(zip(ecols,e)) for e in d.get('events',[]))
    found=[p for p in products if str(p['product_uuid'])==target]
    if not found: continue
    print("TARGET_STORE",json.dumps(store,ensure_ascii=False),flush=True)
    for p in found:
        print("TARGET_PRODUCT",json.dumps(p,ensure_ascii=False,default=str),flush=True)
        hits=sorted((e for e in events if str(e.get('product_uuid'))==target),key=lambda x:(x['event_time'],x['id']))
        print("EVENT_TOTAL",len(hits),flush=True)
        for e in hits:
            old=json.loads(e.get('old_state') or '{}')
            new=json.loads(e.get('new_state') or '{}')
            print("EVENT",json.dumps({'id':e['id'],'timestamp':e['event_time'],'type':e['event_type'],
                                     'old_price':old.get('price'),'new_price':new.get('price'),
                                     'valid':new.get('price_valid'),'is_open':new.get('is_open'),
                                     'old_valid':old.get('price_valid'),'category':new.get('category')},
                                     ensure_ascii=False),flush=True)
