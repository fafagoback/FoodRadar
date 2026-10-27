"""Release audit: determine WHY the latest verified batch produces zero discounts.

Read-only; only verified in-business-hours observations qualify.
"""
from collections import Counter, defaultdict
import json
import sqlite3
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from price_change import PriceChange
from json_to_db import calculate_effective_price

def report(path):
    con=sqlite3.connect(path); con.row_factory=sqlite3.Row
    latest=con.execute('SELECT MAX(processed_at) FROM crawl_batches').fetchone()[0]
    current=con.execute("""SELECT p.store_uuid,p.product_uuid,p.product_name,p.price,p.effective_price,
        p.promo_type,p.quantity,p.discount_pct,p.reference_price,p.is_price_drop,p.is_price_deal,
        p.last_seen,s.last_seen AS store_last_seen,s.name AS store_name,s.is_open AS store_open
        FROM products p JOIN stores s USING(store_uuid)
        WHERE p.last_seen=? AND s.last_seen=? AND p.is_open=1 AND s.is_open=1
          AND p.status='active' AND s.status='active' AND p.effective_price>0""",(latest,latest)).fetchall()
    counters=Counter(); counter_dates=Counter(); histories=defaultdict(list)
    for ev in con.execute("""SELECT store_uuid,product_uuid,event_time,id,event_type,new_state
        FROM events WHERE event_type IN ('NEW','PRICE_OBSERVED','PRICE_CHANGED','PROMOTION_CHANGED')
        ORDER BY store_uuid,product_uuid,event_time,id"""):
        try:
            s=json.loads(ev['new_state'] or '{}')
            if s.get('price_valid') is not True or s.get('is_open') not in (True,1):continue
            unit=calculate_effective_price(float(s.get('price') or 0),
                                           s.get('promo_type') or '無',
                                           int(s.get('quantity') or 1))
        except (ValueError,TypeError,json.JSONDecodeError,OverflowError):continue
        if unit<=0:continue
        key=(ev['store_uuid'],ev['product_uuid'])
        sample=(ev['event_time'],round(unit,2))
        if histories[key] and histories[key][-1][0]==sample[0]:
            histories[key][-1]=sample
        else:histories[key].append(sample)
        counter_dates[sample[0][:10]]+=1
    examples=[]; discrepancies=[]
    for p in current:
        counters['current_products']+=1
        key=p['store_uuid'],p['product_uuid']
        hist=histories.get(key,[])
        if len(hist)<2:
            counters['no_prior_valid_observation']+=1
        else:
            counters['has_prior_valid_observation']+=1
            before=hist[-2][1]; after=hist[-1][1]
            calculated=PriceChange.compare(before,after)
            if calculated is None: counters['invalid_comparison']+=1
            elif calculated['direction']=='flat': counters['flat']+=1
            elif calculated['direction']=='up': counters['up']+=1
            else:
                counters['down']+=1
                if calculated['discount_pct']>=30:
                    counters['qualified30']+=1
                    if len(examples)<12:
                        examples.append({'store':p['store_name'],'product':p['product_name'],
                                         'previous':before,'current':after,
                                         'percent':calculated['discount_pct']})
            if calculated and (p['is_price_drop'] != int(calculated['is_price_drop']) or
                 abs(float(p['discount_pct'])-calculated['discount_pct'])>.02 or
                 abs(float(p['effective_price'])-after)>.02):
                if len(discrepancies)<12:
                    discrepancies.append({'store':p['store_name'],'product':p['product_name'],
                                          'history':hist[-3:],'stored':{
                                              'drop':p['is_price_drop'],'pct':p['discount_pct'],
                                              'price':p['effective_price']}})
        if p['is_price_drop']:counters['stored_current_drop']+=1
        if p['is_price_deal']:counters['stored_current_deal30']+=1
        if p['reference_price'] is not None:counters['stored_reference_exists']+=1
    output={'batch':latest,'current_products':len(current),
      'comparisons':dict(counters),'valid_event_count_by_date':dict(sorted(counter_dates.items())),
      'qualified_examples':examples,'discrepancies':discrepancies}
    print('FOODRADAR_DEAL_PIPELINE_AUDIT='+json.dumps(output,ensure_ascii=False),flush=True)
    # This warning makes a logical 0 distinct from a real market with no price changes.
    if not counters['has_prior_valid_observation']:
        print('FOODRADAR_WARNING_NO_PRICE_COMPARISON_BASELINES: No product can become a valid current price drop.',flush=True)
    if discrepancies:
        raise ValueError('Published discount flags disagree with the two most recent valid prices.')
    con.close()
    return output

if __name__=='__main__':
    if len(sys.argv)!=2:raise SystemExit('Usage: audit_deal_pipeline.py path/to/serving.db')
    report(sys.argv[1])
