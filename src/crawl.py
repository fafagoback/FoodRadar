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


def nearby(store, config, document=None):
    geo = (document or {}).get('geo', {})
    lat = geo.get('latitude')
    lon = geo.get('longitude')
    if lat is None or lon is None:
        lat, lon = store.get('store_lat'), store.get('store_lon')
    try:
        distance = distance_km(lat, lon, config['latitude'], config['longitude'])
    except (TypeError, ValueError):
        return None
    return distance if distance <= config['radius_km'] else None


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def crawl(config):
    if not 0 < config['radius_km'] <= 10:
        raise ValueError('radius_km must be within (0, 10]')
    distance_km(config['latitude'], config['longitude'], config['latitude'], config['longitude'])
    if not 1 <= config['workers'] <= 5 or not 1 <= config['max_pages'] <= 100:
        raise ValueError('workers must be 1..5; max_pages must be 1..100')
    batch = datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%dT%H%M%S%f')
    output = ROOT / 'data' / batch
    menus = output / 'menus'
    menus.mkdir(parents=True)
    point = dict(id='foodradar', latitude=config['latitude'], longitude=config['longitude'], county='台北市')
    _, discovered, pages = scan_single_point(point, max_pages=config['max_pages'])
    if not discovered:
        raise RuntimeError('Empty feed; keep the previous published snapshot')
    unique = {}
    excluded = 0
    for store in discovered:
        key = normalize_store_uuid(store['store_uuid'], store['store_url'])
        store['store_uuid'] = key
        # Unknown coordinates are resolved from the menu before inclusion.
        if store.get('store_lat') is not None and store.get('store_lon') is not None and nearby(store, config) is None:
            excluded += 1
            continue
        unique.setdefault(key, store)
    stores = list(unique.values())
    with ThreadPoolExecutor(max_workers=config['workers']) as pool:
        results = list(pool.map(lambda s: fetch_single_store(s, str(menus), batch+'_'), stores))
    retained, documents, failures = [], [], []
    for store, result in zip(stores, results):
        if result['status'] == 'FAILED':
            failures.append(result)
            continue
        path = Path(result['file_path'])
        doc = json.loads(path.read_text(encoding='utf-8'))
        distance = nearby(store, config, doc)
        if distance is None:
            path.unlink()
            excluded += 1
            continue
        store['distance_km'] = round(distance, 3)
        doc['distance_km'] = store['distance_km']
        write_json(path, doc)
        retained.append(store)
        documents.append(doc)
    report = dict(batch_id=batch, center=point, radius_km=config['radius_km'], pages=pages,
                  discovered=len(discovered), retained=len(retained), excluded=excluded,
                  failed=len(failures), failures=failures)
    write_json(output / 'report.json', report)
    if failures or not retained:
        raise RuntimeError(f'Incomplete snapshot: {len(failures)} failed, {len(retained)} retained; report: {output}')
    write_json(output / 'stores.json', retained)
    write_json(output / 'snapshot.json', dict(report=report, stores=documents))
    with (output / 'stores.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        columns = ['store_uuid', 'name', 'store_url', 'store_lat', 'store_lon', 'distance_km']
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(retained)
    # Current is updated only after complete discovery, menus and radius checks.
    (ROOT / 'data' / 'latest.txt').write_text(batch, encoding='utf-8')
    write_json(ROOT / 'web' / 'data.json', dict(report=report, stores=documents))
    print(json.dumps({k:v for k,v in report.items() if k != 'failures'}, ensure_ascii=False))
    return output


def upload(output, config):
    from huggingface_hub import HfApi
    api = HfApi(token=os.environ['HF_TOKEN'])
    repo, folder = config['hf_repo_id'], config['hf_folder'].strip('/')
    api.upload_folder(repo_id=repo, repo_type='dataset', folder_path=str(output),
                      path_in_repo=f'{folder}/snapshots/{output.name}')
    api.upload_file(repo_id=repo, repo_type='dataset', path_or_fileobj=str(output / 'snapshot.json'),
                    path_in_repo=f'{folder}/latest.json')
    print(f'HF uploaded: {repo}/{folder}/snapshots/{output.name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--upload', action='store_true')
    parser.add_argument('--check-config', action='store_true')
    args = parser.parse_args()
    config = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
    if args.check_config:
        print(json.dumps(config, ensure_ascii=False, indent=2))
    else:
        output = crawl(config)
        if args.upload:
            upload(output, config)
