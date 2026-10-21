"""Read-only Turso and HF Raw audit for reported noodle-combo price oscillation."""
import os,json,base64,urllib.request,collections,datetime,tarfile,pathlib
import msgpack,zstandard
url=os.environ['TURSO_DATABASE_URL'].replace('libsql://','https://').rstrip('/')
token=os.environ['TURSO_READONLY_TOKEN']
def query(sql):
    body=json.dumps({'requests':[{'type':'execute','stmt':{'sql':sql}},{'type':'close'}]}).encode()
    req=urllib.request.Request(url+'/v2/pipeline',body,{'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=90) as resp: data=json.load(resp)
    ans=data['results'][0]
    if ans['type']!='ok':raise RuntimeError(str(ans.get('error')))
    res=ans['response']['result']
    keys=[v['name'] for v in res['cols']]
    return [dict(zip(keys,[(base64.urlsafe_b64decode(v['base64']+'='*(-len(v['base64'])%4)) if v['type']=='blob' else v.get('value')) for v in row])) for row in res['rows']]
rows=query("SELECT store_id,store_uuid,name FROM store_directory WHERE name LIKE '%麵線%' AND name LIKE '%士林%' LIMIT 80")
print('TARGET_STORE_CANDIDATES',[(r['store_id'],r['name']) for r in rows],flush=True)
if not rows: rows=query("SELECT store_id,store_uuid,name FROM store_directory WHERE name LIKE '%麵線%' LIMIT 100")
targets=[]
for store in rows:
    print('STORE',store['store_id'],store['name'],flush=True)
    blocks=query(f"SELECT chunk_no,payload FROM store_bundles WHERE store_id={int(store['store_id'])} ORDER BY chunk_no")
    pc=ec=None;prods=[];events=[]
    for block in blocks:
        b=msgpack.unpackb(zstandard.ZstdDecompressor().decompress(block['payload']),raw=False)
        pc=b.get('product_columns') or pc
        ec=b.get('event_columns') or ec
        prods += [dict(zip(pc,row[1])) for row in b.get('products',[])]
        events += [dict(zip(ec,row)) for row in b.get('events',[])]
    found=[p for p in prods if ('肉圓' in p.get('product_name','') and ('飲品' in p.get('product_name','') or '大苑子' in p.get('product_name',''))) or ('大苑子' in p.get('product_name','') and '麵線' in p.get('product_name',''))]
    for p in found:
        key=str(p['product_uuid'])
        hits=sorted([e for e in events if str(e.get('product_uuid'))==key],key=lambda e:(e['event_time'],e['id']))
        print('PRODUCT',p['product_uuid'],p['product_name'],'CURRENT',p['price'],'CATEGORY',p.get('category'),'DESCRIPTION',str(p.get('description'))[:180],'EVENT_COUNT',len(hits),flush=True)
        prices=[]
        for e in hits:
            if e['event_time'][:10]<'2026-10-01':continue
            old=json.loads(e.get('old_state') or '{}');new=json.loads(e.get('new_state') or '{}')
            print('EVENT',e.get('id'),e['event_time'],e['event_type'],'OLDPRICE',old.get('price'),'NEWPRICE',new.get('price'),'PROMO',new.get('promo_type'),'CATEGORY',str(new.get('category'))[:70],'NAME',str(new.get('product_name'))[:90],'DESCRIPTION',str(new.get('description'))[:100],flush=True)
            if e['event_type']=='PRICE_CHANGED':prices.append((e['event_time'],new.get('price')))
        print('CHANGES_TIMELINE',json.dumps(prices,ensure_ascii=False),flush=True)
        targets.append((store,p,hits))
print('MATCHED_PRODUCTS',len(targets),flush=True)
# Verify corresponding archived JSON from original crawler, independent of Turso's derived event states.
from huggingface_hub import HfApi,hf_hub_download
hf_token=os.environ['HF_TOKEN']
api=HfApi(token=hf_token)
files=set(api.list_repo_files(repo_id='hub-google/UberEat',repo_type='dataset'))
print('HF_ARCHIVE_MANIFESTS',sum(f.startswith('FoodRadar/snapshots/') and f.endswith('/complete.json') for f in files),flush=True)
for store,p,hits in targets:
    recent=[e for e in hits if e['event_type']=='PRICE_CHANGED' and e['event_time'][:10] in ('2026-10-14','2026-10-20')]
    sample=[]
    for e in recent:
        batch=datetime.datetime.fromisoformat(e['event_time']).strftime('%Y%m%d%H%M%S')
        if batch not in sample:sample.append(batch)
    sample=sample[:5]
    print('RAW_BATCHES_TO_VERIFY',sample,flush=True)
    for batch in sample:
        mf=f'FoodRadar/snapshots/{batch}/complete.json'
        if mf not in files:
            print('RAW_MANIFEST_MISSING',batch,flush=True)
            continue
        try:
            manifest=json.loads(pathlib.Path(hf_hub_download('hub-google/UberEat',mf,repo_type='dataset',token=hf_token)).read_text(encoding='utf8'))
            archive=hf_hub_download('hub-google/UberEat',manifest['archive_path'],repo_type='dataset',token=hf_token)
            matches=[]
            with tarfile.open(archive,'r:gz') as tar:
                for member in tar:
                    if not member.isfile() or not member.name.endswith('.json') or not member.name.startswith('Json/'):continue
                    file=tar.extractfile(member)
                    if file is None:continue
                    doc=json.load(file)
                    if str(doc.get('store_uuid'))!=str(store['store_uuid']):continue
                    print('RAW_HOURS',batch,'isOpen',doc.get('isOpen'),'workingHoursTagline',doc.get('workingHoursTagline'),'closedMessage',doc.get('closedMessage'),'openingHoursSpecification',json.dumps(doc.get('openingHoursSpecification',[]),ensure_ascii=False),flush=True)
                    for sec in (doc.get('hasMenu') or {}).get('hasMenuSection',[]):
                        for item in sec.get('hasMenuItem',[]):
                            if str(item.get('identifier'))==str(p['product_uuid']) or str(item.get('name'))==str(p['product_name']):
                                matches.append({'id':item.get('identifier'),'name':item.get('name'),'category':sec.get('name'),'price':(item.get('offers') or {}).get('price'),'description':str(item.get('description'))[:110]})
                    print('RAW_BATCH',batch,'STATUS',doc.get('menu_status'),'ISOPEN',doc.get('isOpen'),'MATCHES',json.dumps(matches,ensure_ascii=False),flush=True)
                    break
            if not matches:print('RAW_TARGET_MISSING',batch,flush=True)
        except Exception as exc:
            print('RAW_VERIFY_ERROR',batch,type(exc).__name__,str(exc)[:300],flush=True)
