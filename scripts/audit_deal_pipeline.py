"""Independently verify published 7-day prices against saved RAW menu quote history."""
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from price_change import PriceChange


def report(path):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    latest = con.execute('SELECT MAX(processed_at) FROM crawl_batches').fetchone()[0]
    current = con.execute("""SELECT p.store_uuid,p.product_uuid,p.product_name,
        p.menu_quote_effective_price,p.menu_quote_history,p.seven_day_reference_price,
        p.seven_day_discount_pct,p.seven_day_discount_amount,p.seven_day_is_price_deal,
        p.delivery_available,s.name AS store_name
        FROM products p JOIN stores s USING(store_uuid)
        WHERE p.last_seen=? AND s.last_seen=?
          AND p.status='active' AND s.status='active'
          AND p.menu_quote_effective_price>0""", (latest,latest))
    result = {'current_menu_quotes':0,'comparable_menu_quotes':0,
              'raw_menu_discounts_30pct':0,'confirmed_delivery_discounts_30pct':0}
    mismatches = []
    examples = []
    for p in current:
        result['current_menu_quotes'] += 1
        try:
            history = json.loads(p['menu_quote_history'] or '[]')
        except (TypeError,ValueError):
            history = []
        week = PriceChange.seven_day_high(
            history,latest,p['menu_quote_effective_price'])
        expected = PriceChange.persisted_seven_day(week)
        if week:
            result['comparable_menu_quotes'] += 1
        if expected['seven_day_is_price_deal']:
            result['raw_menu_discounts_30pct'] += 1
            if p['delivery_available']:
                result['confirmed_delivery_discounts_30pct'] += 1
            if len(examples) < 12:
                examples.append({'store':p['store_name'],'product':p['product_name'],
                   'highest_7d_quote':week['reference'],
                   'latest_menu_quote':p['menu_quote_effective_price'],
                   'discount_pct':expected['seven_day_discount_pct'],
                   'delivery_available':bool(p['delivery_available'])})
        errors = []
        for name in ('seven_day_reference_price','seven_day_discount_pct',
                     'seven_day_discount_amount','seven_day_is_price_deal'):
            actual = p[name]
            wanted = expected[name]
            if wanted is None:
                if actual is not None: errors.append(name)
            elif actual is None or abs(float(actual)-float(wanted)) > .02:
                errors.append(name)
        if errors and len(mismatches)<12:
            mismatches.append({'store':p['store_name'],'product':p['product_name'],
                'columns':errors,'expected':expected,
                'actual':{name:p[name] for name in errors}})
    con.close()
    output = {'batch':latest,'comparisons':result,
              'seven_day_examples':examples,'seven_day_discrepancies':mismatches}
    print('FOODRADAR_DEAL_PIPELINE_AUDIT='+json.dumps(output,ensure_ascii=False),flush=True)
    if mismatches:
        raise ValueError('Published menu-price discount fields differ from RAW quote history.')
    return output


if __name__ == '__main__':
    if len(sys.argv)!=2: raise SystemExit('Usage: audit_deal_pipeline.py path/to/serving.db')
    report(sys.argv[1])
