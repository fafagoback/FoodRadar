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
from price_observation import price_observation_valid, menu_price_observation_valid, _schedule_open, observed_at
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
    product_labels={}
    raw_quote_history=collections.defaultdict(list)
    raw_batch_statistics=[]
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
        raw_quote_this_batch={}
        eligible_quote_this_batch={}
        raw_quoted_stores=set()
        for doc in docs:
            counts['raw_stores']+=1
            sid_raw=str(doc.get('store_uuid') or doc.get('@id') or '')
            store_quotes={}
            for sec in (doc.get('hasMenu') or {}).get('hasMenuSection') or []:
                category_raw=str(sec.get('name') or '')
                for item in sec.get('hasMenuItem') or []:
                    try:
                        name=str(item.get('name') or '').strip()
                        if not name:continue
                        pid,_=product_identity(item,sid_raw)
                        price=float((item.get('offers') or {}).get('price') or 0)
                        if not 0<price<1000000:continue
                        promo,qty=extract_promo_info(category_raw,name,str(item.get('description') or ''))
                        promo=str(item.get('promo_type') or promo)
                        qty=max(1,int(item.get('quantity') or qty))
                        unit=calculate_effective_price(price,promo,qty)
                        if not 0<unit<1000000:continue
                        rank=(int(qty>1),promo)
                        prior=store_quotes.get(pid)
                        if prior is None or rank>prior[0]:store_quotes[pid]=(rank,round(unit,2))
                    except (ValueError,TypeError,OverflowError):pass
            if store_quotes:raw_quoted_stores.add(sid_raw)
            for pid,(_,unit) in store_quotes.items():raw_quote_this_batch[(sid_raw,pid)]=unit
            if menu_price_observation_valid(doc,batch):
                for pid,(_,unit) in store_quotes.items():eligible_quote_this_batch[(sid_raw,pid)]=unit
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
                            product_labels[(sid,pid)]={
                                'store_name':str(doc.get('name') or doc.get('store_name') or ''),
                                'product_name':str(item.get('name') or ''),
                                'category':category,
                            }
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
        # Compare only open-hours RAW menu quotes. Delivery unavailable is
        # independent, but a closed-hours quote cannot create a false baseline.
        comparable=0
        prior_samples=0
        positive_drops=0
        large_drops=0
        for key,unit in eligible_quote_this_batch.items():
            prior=[p for date,p in raw_quote_history[key] if window<=datetime.strptime(date,'%Y%m%d%H%M%S')<when]
            if not prior:continue
            comparable+=1
            prior_samples+=len(prior)
            high=max(prior)
            if unit<high:
                positive_drops+=1
                if (high-unit)/high>=0.30:large_drops+=1
        quote_audit={'batch':batch,'raw_stores':len(docs),
                     'stores_with_priced_menu':len(raw_quoted_stores),
                     'raw_unique_priced_skus':len(raw_quote_this_batch),
                     'open_hours_eligible_skus':len(eligible_quote_this_batch),
                     'seven_day_comparable_skus':comparable,
                     'positive_price_drops':positive_drops,
                     'thirty_pct_price_drops':large_drops,
                     'prior_quote_samples':prior_samples}
        raw_batch_statistics.append(quote_audit)
        print('HF_RAW_QUOTE_COMPARE_BATCH='+json.dumps(quote_audit,ensure_ascii=False),flush=True)
        for key,unit in eligible_quote_this_batch.items():raw_quote_history[key].append((batch,unit))
        totals.update(counts)
        per_batch.append({'batch':batch,**counts})
        print('HF_HISTORY_BATCH_AUDIT='+json.dumps(per_batch[-1],ensure_ascii=False),flush=True)
    by_day=collections.defaultdict(lambda:collections.Counter())
    for b in raw_batch_statistics:
        k=b['batch'][:8]
        for name in ('raw_stores','stores_with_priced_menu','raw_unique_priced_skus','open_hours_eligible_skus','seven_day_comparable_skus','positive_price_drops','thirty_pct_price_drops'):
            by_day[k][name]+=b[name]
        by_day[k]['batches']+=1
    for day,stat in sorted(by_day.items()):
        d=dict(stat)
        d['day']=day
        d['menu_ingest_store_pct']=round(100*d['stores_with_priced_menu']/d['raw_stores'],2) if d['raw_stores'] else 0
        d['price_comparable_pct']=round(100*d['seven_day_comparable_skus']/d['open_hours_eligible_skus'],2) if d['open_hours_eligible_skus'] else 0
        print('HF_RAW_DAILY_INGEST_COMPARE='+json.dumps(d,ensure_ascii=False),flush=True)
    current={k:v[-1] for k,v in source_prices.items() if v and v[-1][0]==latest}
    qualified=[]
    prior_available=0
    thresholds={str(n):0 for n in (0,1,5,10,20,30,40,50)}
    all_verified_price_drops=[]
    for key,(batch,now_price) in current.items():
        earlier=[price for t,price in source_prices[key] if t<batch and datetime.strptime(t,'%Y%m%d%H%M%S')>=window]
        if not earlier:continue
        prior_available+=1
        high=max(earlier)
        pct=round(100*(high-now_price)/high,2) if high>0 else 0
        if pct>0:
            drop={'store':key[0], 'product':key[1],
                  **product_labels.get(key,{}),
                  'high':high,'current':now_price,'pct':pct,
                  'valid_price_samples':source_prices[key][-12:]}
            all_verified_price_drops.append(drop)
            for threshold in thresholds:
                if pct>=float(threshold):thresholds[threshold]+=1
        if pct>=30:
            qualified.append(drop)
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
                'store':key[0], 'product':key[1],
                **product_labels.get(key,{}),
                'archive_quote_high':high,
                'current_verified':now_price,
                'discount_pct':round(100*(high-now_price)/high,2),
                'archive_batches':len(by_price[high]),
                'archive_quote_dates':sorted(by_price[high]),
                'verified_price_samples':source_prices[key][-12:],
                'previous_verified':max(
                    [p for t,p in source_prices[key] if t<batch],default=None
                )
            })
    print('HF_RAW_PRICE_DROP_DISTRIBUTION='+json.dumps({
        'comparable_latest_products':prior_available,
        'positive_deals_by_minimum_percent':thresholds,
        'top_verified_drops':sorted(all_verified_price_drops,key=lambda x:x['pct'],reverse=True)[:20],
    },ensure_ascii=False),flush=True)
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
        for suspect in (qualified[:20]+legacy_proposals[:20]):
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
        counts_sql=con.execute("""SELECT
          COUNT(*) AS all_products,
          SUM(CASE WHEN seven_day_is_price_deal=1 THEN 1 ELSE 0 END) AS stored_seven_day_deal30,
          SUM(CASE WHEN archive_is_price_deal=1 THEN 1 ELSE 0 END) AS stored_archive_candidates
          FROM products""").fetchone()
        print('HF_NORMALIZED_DATABASE_AUDIT='+json.dumps({
          'batch':latest,'counts':dict(counts_sql)
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
