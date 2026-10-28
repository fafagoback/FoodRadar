"""Exact HF RAW audit for 世界豆漿大王 民族店 / 冰豆漿(冰有糖).

No mutations. Report each captured menu item as-is, along with its category,
original Uber price, promo inference, business hours and same-SKU duplicates.
"""
import json
import os
import sys
import tarfile
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from price_observation import _schedule_open, observed_at, price_observation_valid, menu_price_observation_valid
from json_to_db import extract_promo_info, calculate_effective_price

DATASET='hub-google/UberEat'
FOLDER='FoodRadar/snapshots'


def main():
    api=HfApi(token=os.environ.get('HF_TOKEN'))
    files=api.list_repo_files(repo_id=DATASET,repo_type='dataset')
    manifests=sorted(p for p in files if p.startswith(FOLDER+'/') and p.endswith('/complete.json')
                     and '20261021'<=p.split('/')[2][:8]<='20261027')
    seen=[]
    for manifest in manifests:
        metadata=json.loads(Path(hf_hub_download(DATASET,manifest,repo_type='dataset')).read_text())
        batch=metadata['batch_id']
        archive=hf_hub_download(DATASET,metadata['archive_path'],repo_type='dataset')
        had_store=False
        with tarfile.open(archive,'r:gz') as tar:
            for member in tar:
                if not member.isfile() or not member.name.endswith('.json'):
                    continue
                file=tar.extractfile(member)
                if file is None:continue
                doc=json.load(file)
                name=str(doc.get('name') or '')
                if '世界豆漿大王' not in name or '民族' not in name:
                    continue
                had_store=True
                matched=[]
                sections=(doc.get('hasMenu') or {}).get('hasMenuSection') or []
                for section in sections:
                    category=str(section.get('name') or '')
                    for product in section.get('hasMenuItem') or []:
                        title=str(product.get('name') or '')
                        if '冰豆漿' not in title:continue
                        desc=str(product.get('description') or '')
                        promo,quantity=extract_promo_info(category,title,desc)
                        promo=str(product.get('promo_type') or promo)
                        quantity=max(1,int(product.get('quantity') or quantity))
                        raw=(product.get('offers') or {}).get('price')
                        price=float(raw or 0)
                        matched.append({
                            'name':title,'identifier':product.get('identifier'),
                            'category':category,'description':desc[:220],
                            'original_price':raw,'offers':product.get('offers'),
                            'raw_promo_type':product.get('promo_type'),
                            'inferred_promo':promo,'quantity':quantity,
                            'effective_price':calculate_effective_price(price,promo,quantity),
                            'other_item_keys':sorted(k for k in product if k not in
                                ('name','identifier','description','offers','promo_type','quantity'))
                        })
                exact=[i for i in matched if '冰有糖' in i['name']]
                if not exact:
                    print('SOYMILK_SHOP_NO_TARGET='+json.dumps({
                        'batch':batch,'shop':name,'ice_soymilk_variants':matched
                    },ensure_ascii=False),flush=True)
                    continue
                when=observed_at(doc,batch)
                targetids={str(i['identifier']) for i in exact}
                duplicate=[i for i in matched if str(i['identifier']) in targetids]
                evidence={'batch':batch,'shop':name,'store_uuid':doc.get('store_uuid'),
                    'captured_at':doc.get('captured_at'),'time_local':when.isoformat(),
                    'isOpen':doc.get('isOpen'),'workingHoursTagline':doc.get('workingHoursTagline'),
                    'closedMessage':doc.get('closedMessage'),
                    'actual_hours_open':_schedule_open(doc,when),
                    'open_hour_quote_valid':menu_price_observation_valid(doc,batch),
                    'delivery_verified':price_observation_valid(doc,batch),
                    'menu_status':doc.get('menu_status'),
                    'openingHoursSpecification':doc.get('openingHoursSpecification'),
                    'target':exact,'all_same_sku':duplicate,
                    'other_soymilk_variants':len(matched)-len(duplicate)}
                seen.append(evidence)
                print('SOYMILK_RAW_EVIDENCE='+json.dumps(evidence,ensure_ascii=False,default=str),flush=True)
        if not had_store:
            print('SOYMILK_SHOP_ABSENT='+json.dumps({'batch':batch},ensure_ascii=False),flush=True)
    summary=[{'date':v['time_local'][:16],'open':v['actual_hours_open'],
       'raw_price':sorted({i['original_price'] for i in v['target']}),
       'promo':sorted({i['inferred_promo'] for i in v['all_same_sku']}),
       'category':sorted({i['category'] for i in v['all_same_sku']}),
       'per_item':sorted({i['effective_price'] for i in v['all_same_sku']}),
       'delivery':v['delivery_verified']} for v in seen]
    print('SOYMILK_RAW_TIMELINE='+json.dumps(summary,ensure_ascii=False),flush=True)
    print('SOYMILK_AUDIT_SUMMARY='+json.dumps({
        'batch_count':len(manifests),'shop_target_observations':len(seen)},ensure_ascii=False),flush=True)


if __name__=='__main__':
    main()
