"""One-time recovery of a validated pre-rebuild HF state, without touching Raw.

An incomplete 38-snapshot replay discarded 2,149 valid historical stores.
Restore the immutable original crawl checkpoint, not the destructive derived
checkpoint, and validate its contents before changing the HF latest pointer.
"""
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download

ROOT = Path(__file__).resolve().parents[1]
config = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
folder = config['hf_folder'].strip('/')
repo = config['hf_repo_id']
batch = '20261022073318'
token = os.environ['HF_TOKEN']
api = HfApi(token=token)
files = set(api.list_repo_files(repo_id=repo, repo_type='dataset'))
checkpoint_path = f'{folder}/state/latest.json'
raw_checkpoint = f'{folder}/state/{batch}/serving-state.db.gz'
if raw_checkpoint not in files or checkpoint_path not in files:
    raise RuntimeError(f'Pre-rebuild immutable state not found: {raw_checkpoint}')

old_pointer = json.loads(Path(hf_hub_download(
    repo, checkpoint_path, repo_type='dataset', token=token
)).read_text(encoding='utf-8'))
if old_pointer.get('batch_id') != batch:
    raise RuntimeError('Checkpoint batch moved; never roll back a newer live crawl')

compressed = Path(hf_hub_download(
    repo, raw_checkpoint, repo_type='dataset', token=token
))
checksum = hashlib.sha256(compressed.read_bytes()).hexdigest()
with tempfile.TemporaryDirectory() as scratch:
    recovered = Path(scratch) / 'recovered.db'
    with gzip.open(compressed, 'rb') as src, recovered.open('wb') as dest:
        shutil.copyfileobj(src, dest)
    with sqlite3.connect(recovered) as conn:
        if conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise RuntimeError('Original state integrity check failed')
        stores = conn.execute('SELECT COUNT(*) FROM stores').fetchone()[0]
        products = conn.execute('SELECT COUNT(*) FROM products').fetchone()[0]
        latest = conn.execute(
            "SELECT value FROM metadata WHERE key='latest_batch'"
        ).fetchone()[0]
if stores < 1000 or products < 50000 or latest != batch:
    raise RuntimeError(
        f'Recovery refused: {stores=} {products=} {latest=}; '
        'original full data not proven')

pointer = {
    'schema_version': 1,
    'batch_id': batch,
    'state_path': raw_checkpoint,
    'sha256': checksum,
}
commit = api.create_commit(
    repo_id=repo, repo_type='dataset',
    operations=[CommitOperationAdd(
        path_in_repo=checkpoint_path,
        path_or_fileobj=json.dumps(pointer).encode('utf-8')
    )],
    commit_message=f'FoodRadar restore last complete state {batch}: {stores} stores, {products} products'
)
print('HF_RECOVERED_ORIGINAL_STATE ' + json.dumps({
    'batch': batch, 'stores': stores, 'products': products,
    'checkpoint': raw_checkpoint, 'commit': commit.oid
}), flush=True)
