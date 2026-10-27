"""Diagnose effective-price zigzags using saved verified observations.

Does not change price histories or promotion decisions. Run on the restored
HF serving state to distinguish raw-price jumps from promo-recognition jumps.
"""
import collections
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from json_to_db import calculate_effective_price


def audit(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    events = conn.execute("""
        SELECT store_uuid,product_uuid,event_time,id,event_type,new_state
        FROM events
        WHERE event_type IN ('NEW','PRICE_OBSERVED','PRICE_CHANGED','PROMOTION_CHANGED')
        ORDER BY store_uuid,product_uuid,event_time,id
    """)
    histories = collections.defaultdict(list)
    invalid = 0
    for event in events:
        try:
            state = json.loads(event['new_state'] or '{}')
            if state.get('price_valid') is not True or state.get('is_open') not in (1, True):
                invalid += 1
                continue
            raw = round(float(state.get('price') or 0), 2)
            promo = str(state.get('promo_type') or '無')
            qty = max(1, int(state.get('quantity') or 1))
            unit = calculate_effective_price(raw, promo, qty)
            if unit <= 0:
                invalid += 1
                continue
        except (ValueError, TypeError, json.JSONDecodeError):
            invalid += 1
            continue
        key = (event['store_uuid'], event['product_uuid'])
        observation = {'time':event['event_time'],'raw':raw,'promo':promo,'qty':qty,
                       'unit':unit,'kind':event['event_type']}
        recent = histories[key]
        if recent and recent[-1]['time'] == observation['time']:
            recent[-1] = observation
        else:
            recent.append(observation)
    samples = []
    promo_oscillations = raw_oscillations = 0
    for (sid,pid), observations in histories.items():
        for a,b,c in zip(observations, observations[1:], observations[2:]):
            if not (min(a['unit'],b['unit'],c['unit']) > 0):continue
            deviation=abs(a['unit'] - c['unit'])/a['unit']
            swing=abs(a['unit'] - b['unit'])/max(a['unit'],b['unit'])
            if deviation > .02 or swing < .30:
                continue
            promo_toggle = a['raw'] == b['raw'] == c['raw'] and len({a['promo'],b['promo'],c['promo']}) > 1
            if promo_toggle: promo_oscillations += 1
            else: raw_oscillations += 1
            samples.append((swing, sid, pid, observations[-8:], promo_toggle))
    samples.sort(reverse=True)
    print('PRICE_OSCILLATION_AUDIT=' + json.dumps({
        'products_with_observations':len(histories),
        'invalid_events_skipped':invalid,
        'promo_label_toggle_abas':promo_oscillations,
        'other_price_abas':raw_oscillations,
        'top_samples': len(samples)
    },ensure_ascii=False),flush=True)
    for swing,sid,pid,observations,promo_toggle in samples[:12]:
        item = conn.execute(
            "SELECT p.product_name,s.name AS store_name FROM products p JOIN stores s USING(store_uuid) WHERE p.store_uuid=? AND p.product_uuid=?",
            (sid,pid)).fetchone()
        print('PRICE_OSCILLATION_SAMPLE='+json.dumps({
            'store':item['store_name'] if item else sid,
            'name':item['product_name'] if item else pid,
            'product_uuid':pid,'store_uuid':sid,
            'swing_pct':round(swing*100,1),'promo_label_toggle':promo_toggle,
            'recent':observations,
        },ensure_ascii=False),flush=True)
    for pid in ('dd9a9597-c24e-5895-a570-c2e8df852535',):
        for (sid,current_pid),observations in histories.items():
            if current_pid != pid:continue
            item = conn.execute(
                "SELECT p.product_name,s.name FROM products p JOIN stores s USING(store_uuid) WHERE p.store_uuid=? AND p.product_uuid=?",
                (sid,pid)).fetchone()
            print('TARGETED_HISTORY_AUDIT='+json.dumps({
                'name':item[0] if item else '',
                'store':item[1] if item else sid,
                'store_uuid':sid,'product_uuid':pid,
                'recent':observations[-30:]
            },ensure_ascii=False),flush=True)
    conn.close()


if __name__ == '__main__':
    if len(sys.argv)!=2: raise SystemExit('Usage: audit_price_oscillations.py serving.db')
    audit(sys.argv[1])
