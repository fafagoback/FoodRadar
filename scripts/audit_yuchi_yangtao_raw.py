"""Forensic inspection: original HF RAW menus for 魚池貳壹 台北士林店 楊桃甘露.

Keep the original section labels and offered prices; never infer that a
historical 27.5 unit quote is a payable price until its BOGO evidence is seen.
"""
import json
import os
import sys
import tarfile
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from price_observation import _schedule_open, observed_at, price_observation_valid
from json_to_db import extract_promo_info, calculate_effective_price
from serving_state import product_identity

DATASET='hub-google/UberEat'
FOLDER='FoodRadar/snapshots'
TARGET='楊桃甘露'

def main():
    api=HfApi(token=os.environ.get('HF_TOKEN'))
    files=api.list_repo_files(repo_id=DATASET,repo_type='dataset')
    manifests=sorted(p for p in files if p.startswith(FOLDER+'/') and p.endswith('/complete.json') and
                     '20261021'<=p.split('/')[2][:8]<='20261027')
    checked=0
    found=0
    for manifest in manifests:
        meta=json.loads(Path(hf_hub_download(DATASET,manifest,repo_type='dataset')).read_text())
        batch=meta['batch_id']
        archive=hf_hub_download(DATASET,meta['archive_path'],repo_type='dataset')
        matched=False
        with tarfile.open(archive,'r:gz') as tar:
            for member in tar:
                if not member.isfile() or not member.name.endswith('.json'):continue
                stream=tar.extractfile(member)
                if stream is None:continue
                doc=json.load(stream)
                store_name=str(doc.get('name') or '')
                if not ('魚池' in store_name and ('士林' in store_name or '台北' in store_name)):
                    continue
                sections=(doc.get('hasMenu') or {}).get('hasMenuSection') or []
                items=[]
                for section in sections:
                    category=str(section.get('name') or '')
                    for item in section.get('hasMenuItem') or []:
                        name=str(item.get('name') or '')
                        if TARGET not in name:continue
                        desc=str(item.get('description') or '')
                        inferred,qty=extract_promo_info(category,name,desc)
                        promo=str(item.get('promo_type') or inferred)
                        quantity=max(1,int(item.get('quantity') or qty))
                        raw=(item.get('offers') or {}).get('price')
                        effective=calculate_effective_price(float(raw or 0),promo,quantity)
                        items.append({'name':name,'identifier':item.get('identifier'),
                           'section':category,'description':desc[:180],
                           'raw_price':raw,'raw_offers':item.get('offers'),
                           'raw_promo_type':item.get('promo_type'),
                           'promo_inferred':promo,'quantity':quantity,'effective_price':effective})
                if not items:
                    print('YUCHI_RAW_STORE_NO_MATCH='+json.dumps({'batch':batch,'store':store_name,
                         'isOpen':doc.get('isOpen'),'menu_status':doc.get('menu_status'),
                         'total_sections':len(sections)},ensure_ascii=False),flush=True)
                    matched=True
                    continue
                sid=str(doc.get('store_uuid') or '')
                identities={}
                for item in items:
                    identity=item.get('identifier') or 'name:'+item['name']
                    identities.setdefault(identity,[]).append(item)
                when=observed_at(doc,batch)
                result={'batch':batch,'store_name':store_name,'store_uuid':sid,
                  'captured_at':doc.get('captured_at'),'observed_at':when.isoformat(),
                  'isOpen':doc.get('isOpen'),'closedMessage':doc.get('closedMessage'),
                  'workingHoursTagline':doc.get('workingHoursTagline'),
                  'schedule_open':_schedule_open(doc,when),
                  'price_delivery_verified':price_observation_valid(doc,batch),
                  'menu_status':doc.get('menu_status'),
                  'schedule':doc.get('openingHoursSpecification'),
                  'items':items,'identity_duplicates':{k:len(v) for k,v in identities.items()}}
                print('YUCHI_YANGTAO_RAW='+json.dumps(result,ensure_ascii=False,default=str),flush=True)
                matched=True
                found+=1
        checked+=1
        if not matched:
            print('YUCHI_RAW_NOT_IN_BATCH='+json.dumps({'batch':batch},ensure_ascii=False),flush=True)
    print('YUCHI_RAW_SUMMARY='+json.dumps({'batches_checked':checked,'matching_store_and_sku_batches':found},ensure_ascii=False),flush=True)

if __name__=='__main__':main()
