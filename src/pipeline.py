"""Use UberEat's archive and ETL contracts for a single delivery coordinate."""
import contextlib
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def restore_history(config):
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
    checkpoint_path=f'{folder}/state/latest.json'
    if not local_batch and checkpoint_path in files:
        checkpoint=json.loads(Path(hf_hub_download(config['hf_repo_id'],checkpoint_path,repo_type='dataset')).read_text(encoding='utf-8'))
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
                docs=[json.load(tar.extractfile(m)) for m in tar.getmembers() if m.name.startswith('Json/') and m.name.endswith('.json')]
            if len(docs)!=meta['store_count']:
                raise ValueError('HF store count mismatch')
            apply_snapshot(conn,docs,batch)
            saved=sqlite3.connect(target/'serving.db')
            conn.backup(saved)
            saved.close()
            previous.write_text(batch,encoding='utf-8')
            local_batch=batch
        for path in files:
            if path.startswith(f'{folder}/Parquet/history/') and path.endswith('.parquet'):
                batch=Path(path).stem.removeprefix('taiwan_catalog_')
                target=ROOT/'data'/batch/'site'
                target.mkdir(parents=True,exist_ok=True)
                shutil.copy2(hf_hub_download(config['hf_repo_id'],path,repo_type='dataset'),target/Path(path).name)
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
    from json_to_db import UberEatsDBImporter
    importer=UberEatsDBImporter(str(output/'ubereats.db'), str(output/'menus'))
    importer.init_database()
    shutil.copy2(summary_path,output/'menus'/summary_path.name)
    importer.import_all_data()
    importer.close()
    (output/'menus'/summary_path.name).unlink()
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
    counts=apply_snapshot(conn, documents, batch)
    conn.close()
    subprocess.run([sys.executable,str(ROOT/'scripts/build_web_db.py'),
                    str(serving),str(output/'packed-serving.db')],check=True)
    subprocess.run([sys.executable, str(ROOT/'src/package_and_upload_menu_snapshot.py'),
                    '--src-dir', str(output/'menus'), '--stores-file', str(output/'stores.json'),
                    '--output-dir', str(output/'archive'), '--batch-id', batch,
                    '--expected-workers', '1', '--actual-workers', '1', '--minimum-stores', '1',
                    '--repo-id', config['hf_repo_id'], '--offline'], check=True)
    from export_static_snapshots import export_all_static_snapshots
    site = output/'site'
    stats = export_all_static_snapshots(src_dir=str(output/'menus'), output_dir=str(site),
                                       db_path=str(output/'export.db'), batch_id=batch,
                                       stores_file=str(output/'stores.json'), scope='regional', upload_hf=False)
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
    state_manifest={'schema_version':1,'batch_id':batch,'state_path':state_remote,
                    'sha256':hashlib.sha256(state_path.read_bytes()).hexdigest()}
    operations.extend([
        CommitOperationAdd(path_in_repo=state_remote,path_or_fileobj=str(state_path)),
        CommitOperationAdd(path_in_repo=f'{folder}/state/latest.json',
                           path_or_fileobj=json.dumps(state_manifest).encode('utf-8'))])
    api=HfApi(token=os.environ['HF_TOKEN'])
    result=api.create_commit(repo_id=config['hf_repo_id'],repo_type='dataset',operations=operations,
                             commit_message=f'FoodRadar complete snapshot {batch}')
    print(f'HF complete snapshot committed: {result.oid}',flush=True)
