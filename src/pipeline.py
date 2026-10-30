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
    """Rebuild this point's Current/Events state on ephemeral Actions runners."""
    from huggingface_hub import HfApi, hf_hub_download
    import tarfile
    import sqlite3
    from serving_state import apply_snapshot
    folder=config['hf_folder'].strip('/')
    api=HfApi(token=os.environ.get('HF_TOKEN'))
    files=api.list_repo_files(repo_id=config['hf_repo_id'],repo_type='dataset')
    manifests=sorted(p for p in files if p.startswith(f'{folder}/snapshots/') and p.endswith('/complete.json'))
    previous=ROOT/'data/latest.txt'
    local_batch=previous.read_text(encoding='utf-8').strip() if previous.exists() else ''
    conn=None
    verified_manifest_paths=[]
    checkpoint_path=f'{folder}/state/latest.json'
    if not rebuild_all and not local_batch and checkpoint_path in files:
        checkpoint=json.loads(Path(hf_hub_download(config['hf_repo_id'],checkpoint_path,repo_type='dataset')).read_text(encoding='utf-8'))
        # Never restore V1 text-inferred effective prices as V2 historical truth.
        if checkpoint.get('schema_version') != 2:
            raise RuntimeError('Legacy HF checkpoint is V1; rebuild V2 from ApiRaw first')
        packed=Path(hf_hub_download(config['hf_repo_id'],checkpoint['state_path'],repo_type='dataset')).read_bytes()
        if hashlib.sha256(packed).hexdigest()!=checkpoint['sha256']:
            raise ValueError('HF permanent state checksum mismatch')
        local_batch=checkpoint['batch_id']
        target=ROOT/'data'/local_batch
        target.mkdir(parents=True,exist_ok=True)
        (target/'serving.db').write_bytes(gzip.decompress(packed))
        with contextlib.closing(sqlite3.connect(target/'serving.db')) as verified:
            if verified.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                raise ValueError('HF permanent state integrity check failed')
            if verified.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()[0]!=local_batch:
                raise ValueError('HF permanent state batch mismatch')
        previous.write_text(local_batch,encoding='utf-8')
    try:
        replay_count = 0
        print(f'V2_HF_REPLAY_START archives={len(manifests)} full_rebuild={int(bool(rebuild_all))}',flush=True)
        for manifest_path in manifests:
            meta=json.loads(Path(hf_hub_download(config['hf_repo_id'],manifest_path,repo_type='dataset')).read_text(encoding='utf-8'))
            batch=meta['batch_id']
            if not meta.get('complete') or not (len(batch)==14 and batch.isdigit()) or batch<=local_batch:
                continue
            archive_path=hf_hub_download(config['hf_repo_id'],meta['archive_path'],repo_type='dataset')
            if hashlib.sha256(Path(archive_path).read_bytes()).hexdigest()!=meta['sha256']:
                raise ValueError('HF snapshot checksum mismatch')
            target=ROOT/'data'/batch
            target.mkdir(parents=True,exist_ok=True)
            if conn is None:
                conn=sqlite3.connect(ROOT/'data/restored-serving.db')
                conn.row_factory=sqlite3.Row
                if local_batch and (ROOT/'data'/local_batch/'serving.db').exists():
                    prior=sqlite3.connect(ROOT/'data'/local_batch/'serving.db')
                    prior.backup(conn)
                    prior.close()
            with tarfile.open(archive_path,'r:gz') as tar:
                from archive_pairing import archived_documents_with_native
                members = {m.name: m for m in tar.getmembers()
                           if m.isfile() and m.name.endswith('.json')}
                docs, pairing_audit = archived_documents_with_native(
                    tar, members, batch, meta)
                if pairing_audit["raw_files"]:
                    print("V2_RAW_PAIRING="+json.dumps(pairing_audit),flush=True)
            if len(docs)!=meta['store_count']:
                raise ValueError('HF store count mismatch')
            apply_snapshot(conn,docs,batch)
            from verified_weekly import update_verified_weekly
            weekly = update_verified_weekly(conn, batch)
            replay_count += 1
            counts_by_kind = dict(conn.execute(
                'SELECT promotion_type, COUNT(*) FROM product_price_observations '
                'GROUP BY promotion_type').fetchall())
            print('V2_HF_REPLAY_PROGRESS='+json.dumps({
                'completed':replay_count,'total':len(manifests),
                'batch':batch,'stores':len(docs),
                'verified_price_types':counts_by_kind,
                'weekly_comparisons':weekly['qualified_comparisons'],
                'weekly_deals':weekly['deals'],
            },ensure_ascii=False),flush=True)
            if rebuild_all:
                verified_manifest_paths.append(manifest_path + ':' + meta['sha256'])
            if not rebuild_all:
                saved=sqlite3.connect(target/'serving.db')
                conn.backup(saved)
                saved.close()
            previous.write_text(batch,encoding='utf-8')
            local_batch=batch
        # A clean rebuild does not trust the old HF checkpoint: every archived
        # menu is replayed to eliminate previously recorded closed-hour prices.
        # Save only the final state to avoid dozens of huge SQLite copies.
        if rebuild_all:
            if conn is None:
                raise RuntimeError('Cannot rebuild prices without complete archived snapshots')
            target=ROOT/'data'/local_batch
            saved=sqlite3.connect(target/'serving.db')
            conn.backup(saved)
            saved.close()
            # This attestation is written ONLY by an uninterrupted replay
            # after each complete Raw archive passed SHA256 and store-count
            # verification. The publisher may accept a deliberate catalogue
            # shrink only with this evidence, never with an arbitrary flag.
            if len(verified_manifest_paths) != len(manifests):
                raise ValueError('Incomplete HF Raw replay: cannot authorize replacement')
            evidence = {
                'format': 'foodradar-full-raw-replay-v2',
                'batch_id': local_batch,
                'verified_manifest_count': len(verified_manifest_paths),
                'manifest_digest': hashlib.sha256(
                    '\\n'.join(verified_manifest_paths).encode()).hexdigest(),
                'source_counts': {
                    'stores': conn.execute('SELECT COUNT(*) FROM stores').fetchone()[0],
                    'products': conn.execute('SELECT COUNT(*) FROM products').fetchone()[0],
                    'crawl_batches': conn.execute('SELECT COUNT(*) FROM crawl_batches').fetchone()[0],
                },
            }
            (target/'verified-rebuild.json').write_text(
                json.dumps(evidence,indent=2),encoding='utf-8')
            print(f'REBUILT_VALID_PRICE_HISTORY batch={local_batch} snapshots={len(manifests)}',flush=True)
        # V2 price and discount history is reconstructed from ApiRaw exclusively.
        # Never download obsolete Parquet pricing history; those inferred values
        # are no longer consumed and only slow down full HF reconstruction.
    finally:
        if conn is not None:
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
