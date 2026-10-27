"""Seven-day HF raw-data audit before changing trusted price history.
Checks archived raw menu documents and reports exclusion reasons without
silently treating undeliverable prices as valid observations.
"""
import collections
import hashlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys
import tarfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from huggingface_hub import HfApi, hf_hub_download
from price_observation import price_observation_valid, _schedule_open, observed_at
from json_to_db import calculate_effective_price, extract_promo_info
from serving_state import product_identity

def main():
    cfg=json.loads((ROOT/'config.json').read_text(encoding='utf-8'))
    api=HfApi(token=os.environ.get('HF_TOKEN'))
    folder=cfg['hf_folder'].strip('/')
    files=api.list_repo_files(repo_id=cfg['hf_repo_id'],repo_type='dataset')
    latest=(ROOT/'data/latest.txt').read_text().strip()
    anchor=datetime.strptime(latest,'%Y%m%d%H%M%S')
    window=anchor-timedelta(days=7)
    manifests=sorted(p for p in files if p.startswith(folder+'/snapshots/') and p.endswith('/complete.json'))
    totals=collections.Counter()
    per_batch=[]
    source_prices=collections.defaultdict(list)
    legacy_quotes=collections.defaultdict(list)
    for manifest in manifests:
        batch=manifest.split('/')[-2]
        try:when=datetime.strptime(batch,'%Y%m%d%H%M%S')
        except ValueError:continue
        if not window<=when<=anchor:continue
        meta=json.loads(Path(hf_hub_download(cfg['hf_repo_id'],manifest,repo_type='dataset')).read_text())
        archive=Path(hf_hub_download(cfg['hf_repo_id'],meta['archive_path'],repo_type='dataset'))
        if hashlib.sha256(archive.read_bytes()).hexdigest()!=meta['sha256']:
            raise ValueError('Raw archive SHA mismatch: '+batch)
        counts=collections.Counter()
        docs=[]
        with tarfile.open(archive,'r:gz') as tar:
            for member in tar:
                if member.isfile() and member.name.startswith('Json/') and member.name.endswith('.json'):
                    stream=tar.extractfile(member)
                    if stream is not None: docs.append(json.load(stream))
        if len(docs)!=meta['store_count']:raise ValueError('Raw archive count mismatch: '+batch)
        for doc in docs:
            counts['raw_stores']+=1
            valid=price_observation_valid(doc,batch)
            if valid:counts['valid_stores']+=1
            if doc.get('isOpen') is not True:counts['isopen_not_true']+=1
            if str(doc.get('closedMessage') or '').strip():counts['closed_message']+=1
            tagline=str(doc.get('workingHoursTagline') or '').lower().strip()
            if tagline.startswith(('opens ','opening ','closed','temporarily closed','currently closed','休息中','已打烊')):counts['closed_tagline']+=1
            hours=_schedule_open(doc, observed_at(doc,batch))
            if hours is False:counts['schedule_closed']+=1
            if not valid and doc.get('isOpen') is True and not doc.get('closedMessage') and hours is not False:
                counts['needs_manual_review']+=1
            legacy_possible = bool(
                not valid and doc.get('isOpen') is True
                and str(doc.get('closedMessage') or '').strip().lower() == 'delivery unavailable'
                and hours is True
                and (doc.get('menu_status') == 'present')
                and not tagline.startswith(('opens ', 'opening ', 'closed ', 'temporarily closed', 'currently closed', '休息中', '已打烊'))
            )
            if not valid and not legacy_possible:continue
            sid=str(doc.get('store_uuid') or '')
            # Match serving_state.apply_snapshot's cross-category canonical
            # promotion resolution: a SKU is counted ONCE per batch, with
            # BOGO precedence over a general "Featured items" duplicate.
            skus={}
            for section in (doc.get('hasMenu') or {}).get('hasMenuSection') or []:
                category=str(section.get('name') or '')
                for item in section.get('hasMenuItem') or []:
                    try:
                        pid,_=product_identity(item,sid)
                        raw=float((item.get('offers') or {}).get('price') or 0)
                        promo,qty=extract_promo_info(category,str(item.get('name') or ''),str(item.get('description') or ''))
                        promo=str(item.get('promo_type') or promo)
                        qty=max(1,int(item.get('quantity') or qty))
                        if raw<=0:continue
                        previous=skus.get(pid)
                        rank=(int(qty>1),promo)
                        if previous is None or rank>previous[0]:
                            skus[pid]=(rank,raw,promo,qty)
                    except (ValueError,TypeError,OverflowError):pass
            for pid,(_,raw,promo,qty) in skus.items():
                unit=calculate_effective_price(raw,promo,qty)
                if unit<=0:continue
                if valid:
                    source_prices[(sid,pid)].append((batch,round(unit,2)))
                    counts['valid_menu_items']+=1
                else:
                    legacy_quotes[(sid,pid)].append((batch,round(unit,2)))
                    counts['legacy_open_hours_quotes']+=1
        totals.update(counts)
        per_batch.append({'batch':batch,**counts})
        print('HF_HISTORY_BATCH_AUDIT='+json.dumps(per_batch[-1],ensure_ascii=False),flush=True)
    current={k:v[-1] for k,v in source_prices.items() if v and v[-1][0]==latest}
    qualified=[]
    prior_available=0
    for key,(batch,now_price) in current.items():
        earlier=[price for t,price in source_prices[key] if t<batch and datetime.strptime(t,'%Y%m%d%H%M%S')>=window]
        if not earlier:continue
        prior_available+=1
        high=max(earlier)
        if now_price>0 and high>=now_price/0.7:
            qualified.append({'store':key[0],'product':key[1],'high':high,'current':now_price,'pct':round(100*(high-now_price)/high,2)})
    legacy_proposals=[]
    candidates_with_legacy=0
    for key,(batch,now_price) in current.items():
        options=legacy_quotes.get(key,[])
        by_price=collections.defaultdict(set)
        for t,p in options:
            if t<batch:by_price[p].add(t)
        corroborated=[p for p,observations in by_price.items() if len(observations)>=2]
        if not corroborated:continue
        candidates_with_legacy+=1
        high=max(corroborated)
        if high>0 and (high-now_price)/high>=.30:
            legacy_proposals.append({
                'store':key[0], 'product':key[1], 'archive_quote_high':high,
                'current_verified':now_price,
                'discount_pct':round(100*(high-now_price)/high,2),
                'archive_batches':len(by_price[high]),
                'previous_verified':max(
                    [p for t,p in source_prices[key] if t<batch],default=None
                )
            })
    print('HF_LEGACY_MENU_QUOTE_AUDIT='+json.dumps({
        'products_with_2_legacy_quotes':candidates_with_legacy,
        'legacy_high_30pct_candidates':len(legacy_proposals),
        'sample':legacy_proposals[:12]
    },ensure_ascii=False),flush=True)
    import sqlite3
    serving=ROOT/'data'/latest/'serving.db'
    if serving.exists():
        con=sqlite3.connect(serving)
        con.row_factory=sqlite3.Row
        for suspect in qualified[:20]:
            sid,pid=suspect['store'],suspect['product']
            row=con.execute(
              "SELECT product_name,price,promo_type,quantity,effective_price,seven_day_reference_price,seven_day_discount_pct,seven_day_is_price_deal,last_seen FROM products WHERE store_uuid=? AND product_uuid=?",
              (sid,pid)).fetchone()
            events=con.execute(
              "SELECT event_time,event_type,new_state FROM events WHERE store_uuid=? AND product_uuid=? AND event_type IN ('NEW','PRICE_OBSERVED','PRICE_CHANGED','PROMOTION_CHANGED') ORDER BY id DESC LIMIT 9",
              (sid,pid)).fetchall()
            print('RAW_SQL_COMPARE='+json.dumps({
               'raw':suspect,'stored':dict(row) if row else None,
               'events':[{'time':e['event_time'],'type':e['event_type'],'state':json.loads(e['new_state'] or '{}')} for e in events]
            },ensure_ascii=False),flush=True)
        con.close()
    print('HF_SEVEN_DAY_RAW_AUDIT='+json.dumps({
        'window_start':window.isoformat(),'window_end':anchor.isoformat(),
        'batches':len(per_batch),'raw_stores':totals['raw_stores'],
        'valid_stores':totals['valid_stores'],'closed_messages':totals['closed_message'],
        'schedule_closed':totals['schedule_closed'],
        'needs_manual_review':totals['needs_manual_review'],
        'raw_last_batch_products':len(current),
        'prior_comparable_products':prior_available,'raw_valid_deals_30':len(qualified),
        'examples':qualified[:10]
    },ensure_ascii=False),flush=True)

if __name__=='__main__':main()
