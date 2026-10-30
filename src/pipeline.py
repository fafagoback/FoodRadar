"""Use UberEat's archive and ETL contracts for a single delivery coordinate."""
import contextlib
from collections import Counter
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def assert_useful_price_batch(documents, batch):
    """Do not overwrite a healthy published catalogue with a broken geo crawl.

    Zero open stores may legitimately occur at night, but that is not a
    verifiable current-price refresh. A daytime >90% closure response is the
    signature of the previous missing-location-cookie bug.
    """
    from price_observation import price_observation_valid, menu_catalog_valid
    total = len(documents)
    valid = sum(bool(price_observation_valid(doc, batch)) for doc in documents)
    menu_stores = sum(bool(menu_catalog_valid(doc, batch)) for doc in documents)
    banner = sum(bool(str(doc.get("closedMessage") or "").strip()) for doc in documents)
    hour = int(batch[8:10])
    if total >= 50 and menu_stores == 0:
        raise RuntimeError(
            f"Unusable menu crawl: 0/{total} open-hours menu stores; "
            "rejecting batch and preserving previous published prices"
        )
    if total >= 100 and 9 <= hour <= 22 and valid * 20 < total and banner * 10 >= total * 9:
        if menu_stores * 20 < total:
            raise RuntimeError(
                f"Suspected broken delivery-location capture: only {menu_stores}/{total} "
                "usable menu stores; preserving previous catalogue"
            )
        print(f"DELIVERY_UNAVAILABLE_WARNING verified={valid}/{total} menu_catalog={menu_stores}/{total} banners={banner}", flush=True)
    return valid


def restore_history(config, rebuild_all=False):
    """Build serving.db from immutable FoodRadar/Raw alone.

    Intentionally never read legacy FoodRadar/snapshots, HF state/latest.json,
    Parquet, or pre-reset SQLite state. A clean Turso reset must stay clean on
    every subsequent scheduled crawl.
    """
    from huggingface_hub import HfApi, hf_hub_download
    import tarfile
    import sqlite3
    from serving_state import apply_snapshot
    from raw_capture import documents_from_hf_raw
    from verified_weekly import update_verified_weekly

    folder = config['hf_folder'].strip('/')
    api = HfApi(token=os.environ.get('HF_TOKEN'))
    files = api.list_repo_files(repo_id=config['hf_repo_id'], repo_type='dataset')
    manifests = sorted(p for p in files
                       if p.startswith(f'{folder}/Raw/') and p.endswith('/manifest.json'))
    if not manifests:
        raise RuntimeError(f'No immutable snapshots under {folder}/Raw; refusing DB update')

    previous = ROOT/'data/latest.txt'
    previous.parent.mkdir(parents=True, exist_ok=True)
    temporary_db = ROOT/'data/raw-only-reconstructed.db'
    if temporary_db.exists():
        temporary_db.unlink()
    conn = sqlite3.connect(temporary_db)
    conn.row_factory = sqlite3.Row
    replayed = []
    batch = ''
    try:
        print('FOODRADAR_RAW_ONLY_REPLAY_START='+json.dumps({
            'raw_manifests':len(manifests),
            'legacy_snapshots_read':0,
            'legacy_checkpoints_read':0,
            'path': f'{folder}/Raw/',
        },ensure_ascii=False),flush=True)
        for manifest_path in manifests:
            meta = json.loads(Path(hf_hub_download(
                config['hf_repo_id'], manifest_path, repo_type='dataset'
            )).read_text(encoding='utf-8'))
            current_batch = str(meta.get('batch_id') or '')
            if not meta.get('complete') or not (len(current_batch) == 14 and current_batch.isdigit()):
                raise ValueError(f'Incomplete or invalid immutable Raw batch: {manifest_path}')
            if batch and current_batch <= batch:
                raise ValueError(f'Nonmonotonic immutable Raw batch: {manifest_path}')
            if (meta.get('format') != 'foodradar-immutable-http-body-v1'
                    or not str(meta.get('archive_path') or '').startswith(
                        f'{folder}/Raw/{current_batch}/')):
                raise ValueError(f'Invalid immutable Raw source path: {manifest_path}')
            downloaded = Path(hf_hub_download(
                config['hf_repo_id'], meta['archive_path'], repo_type='dataset'
            ))
            digest = hashlib.sha256(downloaded.read_bytes()).hexdigest()
            if digest != meta.get('archive_sha256'):
                raise ValueError('HF immutable Raw archive SHA-256 mismatch')
            with tarfile.open(downloaded, 'r:gz') as raw_archive:
                docs = documents_from_hf_raw(raw_archive, meta)
            if len(docs) != len(meta['stores']):
                raise ValueError('HF original store count mismatch')
            apply_snapshot(conn, docs, current_batch)
            audit = update_verified_weekly(conn, current_batch)
            replayed.append(manifest_path + ':' + digest)
            batch = current_batch
            print('HF_RAW_TO_DB='+json.dumps({
                'batch':batch,
                'stores':len(docs),
                'qualified_comparisons':audit['qualified_comparisons'],
                'deals':audit['deals'],
                'legacy_batches':0,
            },ensure_ascii=False),flush=True)
        conn.commit()
        destination = ROOT/'data'/batch
        destination.mkdir(parents=True,exist_ok=True)
        published_copy = sqlite3.connect(destination/'serving.db')
        try:
            conn.backup(published_copy)
        finally:
            published_copy.close()
        evidence = {
            'format':'foodradar-immutable-raw-only-rebuild-v1',
            'batch_id':batch,
            'raw_manifest_count':len(replayed),
            'verified_manifest_count':len(replayed),
            'manifest_digest':hashlib.sha256(
                '\n'.join(replayed).encode('utf-8')).hexdigest(),
            'source_prefix':f'{folder}/Raw/',
            'legacy_snapshot_count':0,
            'source_counts':{
                'stores':conn.execute('SELECT COUNT(*) FROM stores').fetchone()[0],
                'products':conn.execute('SELECT COUNT(*) FROM products').fetchone()[0],
                'crawl_batches':conn.execute('SELECT COUNT(*) FROM crawl_batches').fetchone()[0],
            },
        }
        (destination/'verified-rebuild.json').write_text(
            json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')
        previous.write_text(batch, encoding='utf-8')
        print('FOODRADAR_RAW_ONLY_REBUILT='+json.dumps(
            evidence,ensure_ascii=False),flush=True)
    finally:
        conn.close()



def build_snapshot(output, config):
    batch = output.name
    # Remove fields added by the earlier LITE implementation; keep the original converter output.
    for path in (output/'menus').glob('*.json'):
        doc=json.loads(path.read_text(encoding='utf-8'))
        if 'distance_km' in doc or 'batch_id' in doc:
            for key in ('distance_km', 'batch_id'):
                doc.pop(key, None)
            path.write_text(json.dumps(doc,ensure_ascii=False,indent=2),encoding='utf-8')
    summary={'benchmark_address':'FoodRadar 固定配送座標',
             'coordinates':{'latitude':config['latitude'],'longitude':config['longitude']},
             'total_discovered':len(list((output/'menus').glob('*.json'))),
             'success_count':len(list((output/'menus').glob('*.json'))),'fail_count':0}
    summary_path=output/f'{batch}_nearby_stores_summary.json'
    summary_path.write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    # Avoid building an unused second DB containing non-operating Raw menus.
    # Hugging Face receives the untouched JSON archive below.
    import sqlite3
    from serving_state import apply_snapshot
    serving=output/'serving.db'
    previous=ROOT/'data/latest.txt'
    if previous.exists():
        prior=ROOT/'data'/previous.read_text(encoding='utf-8').strip()/'serving.db'
        if prior.exists() and prior != serving:
            shutil.copy2(prior, serving)
    conn=sqlite3.connect(serving)
    conn.row_factory=sqlite3.Row
    documents=[json.loads(p.read_text(encoding='utf-8')) for p in sorted((output/'menus').glob('*.json'))]
    # Run the release gate before mutating normalized prices / advancing batch.
    assert_useful_price_batch(documents, batch)
    counts=apply_snapshot(conn, documents, batch)
    from verified_weekly import update_verified_weekly
    weekly_audit = update_verified_weekly(conn, batch)
    print("VERIFIED_V2_WEEKLY_AUDIT=" + json.dumps(weekly_audit), flush=True)
    conn.close()
    # Distinguish successful menu downloads from *valid payable-price*
    # observations. A run containing only rejected observations is not a
    # successful price refresh; report the rejection signature explicitly.
    from price_observation import price_observation_valid, menu_catalog_valid
    audit = {
        "captured_stores": len(documents),
        "valid_price_stores": sum(bool(price_observation_valid(doc, batch)) for doc in documents),
        "catalog_menu_stores": counts["stores"],
        "raw_only_stores": counts["skipped_closed_stores"],
        "is_open": dict(Counter(
            "true" if doc.get("isOpen") is True else
            "false" if doc.get("isOpen") is False else "missing"
            for doc in documents)),
        "menu_status": dict(Counter(str(doc.get("menu_status")) for doc in documents)),
        "closed_message_present": sum(bool(str(doc.get("closedMessage") or "").strip()) for doc in documents),
        "hours_spec_present": sum(bool(doc.get("openingHoursSpecification")) for doc in documents),
        "tagline": dict(Counter(
            "open" if str(doc.get("workingHoursTagline") or "").lower().startswith(("open ", "currently open", "營業中", "營業至", "營業到")) else
            "opens_later" if str(doc.get("workingHoursTagline") or "").lower().startswith(("opens ", "opening ", "尚未營業", "休息中")) else
            "other_or_empty"
            for doc in documents)),
    }
    assert sum(bool(menu_catalog_valid(doc, batch)) for doc in documents) == counts["stores"]
    print("PRICE_OBSERVATION_AUDIT=" + json.dumps(audit, ensure_ascii=False), flush=True)
    report_path = output / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["price_observation_audit"] = audit
    # Preserve new/unknown category names for maintainers; no unknown
    # category may silently trigger a guessed BOGO unit-price reduction.
    import re
    from json_to_db import extract_promo_info
    unknown_sections = Counter()
    native_recognized = 0
    for doc in documents:
        for sec in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
            section = str(sec.get("name") or "").strip()
            if (section and re.search(r"優惠|特惠|促銷|折扣|限時|買.*送|offer|deal|promotion|sale|bonus|free", section, re.I)
                    and extract_promo_info(section, "", "")[0] == "無"):
                unknown_sections[section] += 1
            native_recognized += sum(
                item.get("promotion_source") == "native_structured"
                for item in sec.get("hasMenuItem") or [])
    api_raw_files = sorted((output/'api_raw').glob('*.json'))
    field_paths = Counter()
    # A bounded sample of the *actual* API responses exposes new metadata
    # paths without leaking raw values into public workflow logs.
    def scan_native_keys(value, prefix="", depth=0):
        if depth > 12:
            return
        if isinstance(value, dict):
            for key, child in value.items():
                path = f"{prefix}.{key}" if prefix else key
                if re.search(r"promo|offer|discount|benefit|reward|deal|bogo", key, re.I):
                    field_paths[path] += 1
                if isinstance(child, (dict, list)):
                    scan_native_keys(child, path, depth + 1)
        elif isinstance(value, list):
            for child in value[:5]:
                if isinstance(child, (dict, list)):
                    scan_native_keys(child, prefix + "[]", depth + 1)
    for raw_path in api_raw_files[:40]:
        with raw_path.open(encoding="utf-8") as source:
            raw_response = json.load(source)
        scan_native_keys(raw_response.get("data") or {})
    report["promotion_source_audit"] = {
        "api_raw_response_count": len(api_raw_files),
        "native_structured_item_occurrences": native_recognized,
        "unrecognized_promotion_sections": dict(unknown_sections.most_common(50)),
        "sampled_api_responses": min(40, len(api_raw_files)),
        "observed_native_promotion_field_paths": dict(field_paths.most_common(60)),
        "interpretation": "Unknown promotions are retained, not treated as BOGO."
    }
    print("PROMOTION_SOURCE_AUDIT=" + json.dumps(report["promotion_source_audit"], ensure_ascii=False), flush=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    # V2 no longer runs legacy heuristic repair / archive restoration.
    # Historical quotes are reconstructed exclusively from native ApiRaw evidence.
    print(f"VALID_OPEN_STORES={counts['stores']} RAW_ONLY_CLOSED_STORES={counts['skipped_closed_stores']}", flush=True)
    subprocess.run([sys.executable,str(ROOT/'scripts/build_web_db.py'),
                    str(serving),str(output/'packed-serving.db')],check=True)
    subprocess.run([sys.executable, str(ROOT/'src/package_and_upload_menu_snapshot.py'),
                    '--src-dir', str(output/'menus'), '--stores-file', str(output/'stores.json'),
                    '--api-raw-dir', str(output/'api_raw'),
                    '--output-dir', str(output/'archive'), '--batch-id', batch,
                    '--expected-workers', '1', '--actual-workers', '1', '--minimum-stores', '1',
                    '--repo-id', config['hf_repo_id'], '--offline'], check=True)
    from export_static_snapshots import export_all_static_snapshots
    site = output/'site'
    stats = export_all_static_snapshots(src_dir=str(output/'menus'), output_dir=str(site),
                                       db_path=str(output/'export.db'), batch_id=batch,
                                       stores_file=str(output/'stores.json'), scope='regional', upload_hf=False)
    # Replace legacy Parquet-derived static offers and historical discounts with
    # the same certified numeric V2 prices used in Turso.
    from export_verified_public import export_verified_public
    public_audit = export_verified_public(serving, site, batch)
    print("V2_PUBLIC_EXPORT_AUDIT="+json.dumps(public_audit),flush=True)
    archive = output/'archive'/f'taiwan_menus_{batch}.tar.gz'
    manifest = json.loads((output/'archive/manifest.json').read_text(encoding='utf-8'))
    manifest.update(complete=True, schema_version=1,
                    sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), archive_bytes=archive.stat().st_size,
                    source_sha=os.environ.get('GITHUB_SHA','local'),
                    archive_path=f"{config['hf_folder']}/snapshots/{batch}/{archive.name}")
    (output/'complete.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    subprocess.run([sys.executable,str(ROOT/'scripts/verify_snapshot.py'),str(output)],check=True)
    # Publish only after archive validation and successful ETL. Never ship the build DB.
    target=ROOT/'web/data'
    target.mkdir(exist_ok=True)
    for name in ['stats.json','discounts.json','new_stores.json','new_products.json','promotions.json','products.json','history.json']:
        shutil.copy2(site/name,target/name)
    shutil.copy2(site/'version.json', ROOT/'web/version.json')


def upload_snapshot(output, config):
    from huggingface_hub import HfApi, CommitOperationAdd
    folder=config['hf_folder'].strip('/')
    batch=output.name
    manifest=json.loads((output/'complete.json').read_text(encoding='utf-8'))
    archive_name=f'taiwan_menus_{batch}.tar.gz'
    base=f'{folder}/snapshots/{batch}'
    manifest['archive_path']=f'{base}/{archive_name}'
    payload=json.dumps(manifest,ensure_ascii=False).encode('utf-8')
    operations=[CommitOperationAdd(path_in_repo=f'{base}/{archive_name}',path_or_fileobj=str(output/'archive'/archive_name)),
                CommitOperationAdd(path_in_repo=f'{base}/stores.json',path_or_fileobj=str(output/'stores.json')),
                CommitOperationAdd(path_in_repo=f'{base}/complete.json',path_or_fileobj=payload),
                CommitOperationAdd(path_in_repo=f'{folder}/latest.json',path_or_fileobj=payload)]
    for path in (output/'site').glob('*.parquet'):
        subfolder='' if path.name.endswith('_latest.parquet') else 'history/'
        operations.append(CommitOperationAdd(path_in_repo=f'{folder}/Parquet/{subfolder}{path.name}',path_or_fileobj=str(path)))
    # Durable normalized state retains inactive identities independently of Raw
    # retention. Publish state and its pointer atomically with the complete Raw.
    state_path=output/'serving-state.db.gz'
    with (output/'serving.db').open('rb') as source, gzip.open(state_path,'wb') as dest:
        shutil.copyfileobj(source,dest)
    state_remote=f'{folder}/state/{batch}/serving-state.db.gz'
    state_manifest={'schema_version':2,'batch_id':batch,'state_path':state_remote,
                    'sha256':hashlib.sha256(state_path.read_bytes()).hexdigest()}
    operations.extend([
        CommitOperationAdd(path_in_repo=state_remote,path_or_fileobj=str(state_path)),
        CommitOperationAdd(path_in_repo=f'{folder}/state/latest.json',
                           path_or_fileobj=json.dumps(state_manifest).encode('utf-8'))])
    api=HfApi(token=os.environ['HF_TOKEN'])
    result=api.create_commit(repo_id=config['hf_repo_id'],repo_type='dataset',operations=operations,
                             commit_message=f'FoodRadar complete snapshot {batch}')
    print(f'HF complete snapshot committed: {result.oid}',flush=True)
