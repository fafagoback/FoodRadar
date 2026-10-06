"""Single delivery-location crawler. Never imports the Taiwan coordinator."""
import argparse
import csv
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path

from feed_core import scan_single_point
from menu_core import fetch_single_store, normalize_store_uuid

ROOT = Path(__file__).resolve().parents[1]


def distance_km(lat, lon, target_lat, target_lon):
    values = [float(v) for v in (lat, lon, target_lat, target_lon)]
    if not all(math.isfinite(v) for v in values):
        raise ValueError('non-finite coordinates')
    if abs(values[0]) > 90 or abs(values[2]) > 90 or abs(values[1]) > 180 or abs(values[3]) > 180:
        raise ValueError('invalid coordinates')
    a, b, c, d = map(math.radians, values)
    h = math.sin((c-a)/2)**2 + math.cos(a)*math.cos(c)*math.sin((d-b)/2)**2
    return 6371.0088 * 2 * math.asin(min(1, math.sqrt(h)))


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def crawl(config):
    distance_km(config['latitude'], config['longitude'], config['latitude'], config['longitude'])
    if not 1 <= config['workers'] <= 5 or not 0 <= config['max_pages'] <= 100:
        raise ValueError('workers must be 1..5; max_pages must be 0 (unlimited) or 1..100')
    batch = datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d%H%M%S')
    output = ROOT / 'data' / batch
    menus = output / 'menus'
    menus.mkdir(parents=True)
    point = dict(id='foodradar', latitude=config['latitude'], longitude=config['longitude'], county='台北市')
    print(f'Scanning only {point["latitude"]}, {point["longitude"]}', flush=True)
    _, discovered, pages = scan_single_point(point, max_pages=config['max_pages'])
    print(f'Discovered {len(discovered)} stores across {pages} pages', flush=True)
    if not discovered:
        raise RuntimeError('Empty feed; keep the previous published snapshot')
    unique = {}
    excluded = 0
    for store in discovered:
        key = normalize_store_uuid(store['store_uuid'], store['store_url'])
        store['store_uuid'] = key
        unique.setdefault(key, store)
    stores = list(unique.values())
    print(f'Fetching all {len(stores)} unique menus from this delivery coordinate', flush=True)
    with ThreadPoolExecutor(max_workers=config['workers']) as pool:
        results = list(pool.map(lambda s: fetch_single_store(s, str(menus), batch+'_'), stores))
    retained, documents, failures = [], [], []
    for store, result in zip(stores, results):
        if result['status'] == 'FAILED':
            failures.append(result)
            continue
        path = Path(result['file_path'])
        doc = json.loads(path.read_text(encoding='utf-8'))
        retained.append(store)
        documents.append(doc)
    report = dict(batch_id=batch, center=point, scope='single_delivery_coordinate', pages=pages,
                  discovered=len(discovered), retained=len(retained), excluded=excluded,
                  failed=len(failures), failures=failures)
    write_json(output / 'report.json', report)
    if failures or not retained:
        raise RuntimeError(f'Incomplete snapshot: {len(failures)} failed, {len(retained)} retained; report: {output}')
    write_json(output / 'stores.json', retained)
    with (output / 'stores.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        columns = list(retained[0])
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(retained)
    from pipeline import build_snapshot
    build_snapshot(output, config)
    (ROOT / 'data' / 'latest.txt').write_text(batch, encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k != 'failures'}, ensure_ascii=False))
    return output


def upload(output, config):
    from pipeline import upload_snapshot
    upload_snapshot(output, config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--upload', action='store_true')
    parser.add_argument('--check-config', action='store_true')
    args = parser.parse_args()
    config = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
    if args.check_config:
        print(json.dumps(config, ensure_ascii=False, indent=2))
    else:
        if args.upload:
            from pipeline import restore_history
            restore_history(config)
        output = crawl(config)
        if args.upload:
            upload(output, config)
