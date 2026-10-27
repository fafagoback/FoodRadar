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
        p.seven_day_reference_price,p.seven_day_discount_pct,
        p.seven_day_discount_amount,p.seven_day_is_price_deal,
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
    examples=[]; discrepancies=[]; week_discrepancies=[]
    seven_day_examples=[]
    for p in current:
        counters['current_products']+=1
        key=p['store_uuid'],p['product_uuid']
        hist=histories.get(key,[])
        # Verify the PUBLISHED seven-day comparison independently of the
        # legacy previous-crawl comparison. Previously this audit could pass
        # while every seven_day_* field was silently zero or stale.
        week=PriceChange.seven_day_high(hist,latest,p['effective_price'])
        expected_ref=week['reference'] if week else None
        calc=week['comparison'] if week else None
        expected_pct=calc['discount_pct'] if calc and calc['is_price_drop'] else 0.0
        expected_savings=calc['savings'] if calc and calc['is_price_drop'] else 0.0
        expected_deal=int(PriceChange.qualifies(calc,30))
        actual_ref=p['seven_day_reference_price']
        ref_ok=((expected_ref is None and actual_ref is None) or
                (expected_ref is not None and actual_ref is not None and
                 abs(float(expected_ref)-float(actual_ref))<=0.02))
        if expected_ref is not None:counters['seven_day_comparable']+=1
        else:counters['seven_day_no_reference']+=1
        if expected_deal:
            counters['seven_day_verified_deal30']+=1
            if len(seven_day_examples)<12:
                seven_day_examples.append({
                    'store':p['store_name'],'product':p['product_name'],
                    'seven_day_high':expected_ref,'current':p['effective_price'],
                    'percent':expected_pct
                })
        if p['seven_day_is_price_deal']:counters['stored_seven_day_deal30']+=1
        if (not ref_ok or
            abs(float(p['seven_day_discount_pct'] or 0)-expected_pct)>.02 or
            abs(float(p['seven_day_discount_amount'] or 0)-expected_savings)>.02 or
            int(p['seven_day_is_price_deal'] or 0)!=expected_deal):
            if len(week_discrepancies)<12:
                week_discrepancies.append({
                    'store':p['store_name'],'product':p['product_name'],
                    'history':hist[-6:],
                    'expected':{'reference':expected_ref,'pct':expected_pct,
                                'savings':expected_savings,'deal30':expected_deal},
                    'stored':{'reference':actual_ref,
                              'pct':p['seven_day_discount_pct'],
                              'savings':p['seven_day_discount_amount'],
                              'deal30':p['seven_day_is_price_deal']}
                })
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
      'qualified_examples':examples,'seven_day_examples':seven_day_examples,
      'discrepancies':discrepancies,'seven_day_discrepancies':week_discrepancies}
    print('FOODRADAR_DEAL_PIPELINE_AUDIT='+json.dumps(output,ensure_ascii=False),flush=True)
    # This warning makes a logical 0 distinct from a real market with no price changes.
    if not counters['has_prior_valid_observation']:
        print('FOODRADAR_WARNING_NO_PRICE_COMPARISON_BASELINES: No product can become a valid current price drop.',flush=True)
    if discrepancies:
        raise ValueError('Published previous-crawl discount flags disagree with verified history.')
    if week_discrepancies:
        raise ValueError('Published seven-day maximum-price flags disagree with verified history.')
    con.close()
    return output

if __name__=='__main__':
    if len(sys.argv)!=2:raise SystemExit('Usage: audit_deal_pipeline.py path/to/serving.db')
    report(sys.argv[1])
