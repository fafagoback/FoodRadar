"""Inspect raw Hugging Face snapshots for a known BOGO zigzag case.

Unlike the derived Events table, this checks the original captured restaurant
opening signals and the exact menu category from which promo inference arose.
"""
import io
import json
import os
import sys
import tarfile
from pathlib import Path
from huggingface_hub import hf_hub_download

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / 'src'))
from price_observation import price_observation_valid
from json_to_db import extract_promo_info

DATASET = 'hub-google/UberEat'
BATCHES = ('20261016083101','20261020090813','20261021084227','20261022073318')
CASES = (
    ('f54d8541-8603-5ddb-9e04-ecd69c93f741','fc9808e6-6e3e-4a5a-bccb-798fef8523ed','西瓜汁'),
    ('0fd40e62-4a52-49a5-8620-0706906896e1','dd9a9597-c24e-5895-a570-c2e8df852535','Target Pro'),
)

def inspect():
    for batch in BATCHES:
        archive_path=f'FoodRadar/snapshots/{batch}/taiwan_menus_{batch}.tar.gz'
        try:
            archive=hf_hub_download(DATASET,archive_path,repo_type='dataset',
                                    token=os.environ.get('HF_TOKEN'))
        except Exception as e:
            print('SOURCE_SNAPSHOT_UNAVAILABLE='+json.dumps({
                'batch':batch,'reason':type(e).__name__+': '+str(e)[:150]
            },ensure_ascii=False),flush=True)
            continue
        seen=set()
        with tarfile.open(archive,'r:gz') as files:
            for member in files:
                if not member.isfile() or not member.name.endswith('.json'):continue
                stream=files.extractfile(member)
                if not stream:continue
                try:doc=json.load(stream)
                except (ValueError,UnicodeDecodeError):continue
                sid=str(doc.get('store_uuid') or '')
                for wanted_sid,pid,hint in CASES:
                    if sid != wanted_sid:continue
                    seen.add(wanted_sid)
                    items=[]
                    for sec in (doc.get('hasMenu') or {}).get('hasMenuSection') or []:
                        for item in sec.get('hasMenuItem') or []:
                            if str(item.get('identifier') or '')==pid or hint.lower() in str(item.get('name') or '').lower():
                                name=str(item.get('name') or '')
                                desc=str(item.get('description') or '')
                                inferred,quantity=extract_promo_info(sec.get('name'),name,desc)
                                items.append({'name':name,'section':sec.get('name'),
                                              'description':desc[:120],
                                              'raw_price':(item.get('offers') or {}).get('price'),
                                              'inferred_promo':inferred,'quantity':quantity,
                                              'identifier':item.get('identifier')})
                    print('RAW_MENU_PRICE_EVIDENCE='+json.dumps({
                        'batch':batch,'store_uuid':wanted_sid,'store':doc.get('name'),
                        'captured_at':doc.get('captured_at'),
                        'valid_price':price_observation_valid(doc,batch),
                        'isOpen':doc.get('isOpen'),'closedMessage':doc.get('closedMessage'),
                        'workingHoursTagline':doc.get('workingHoursTagline'),
                        'menu_status':doc.get('menu_status'),'items':items
                    },ensure_ascii=False),flush=True)
        for sid,_,name in CASES:
            if sid not in seen:
                print('RAW_MENU_CASE_NOT_IN_BATCH='+json.dumps({
                    'batch':batch,'store_uuid':sid,'hint':name
                },ensure_ascii=False),flush=True)

if __name__=='__main__':
    inspect()
