"""Publish rebuilt normalized state checkpoint only after a verified Turso write.

Raw HF archives are immutable; the previous checkpoint remains intact under its
existing path. Update the latest pointer atomically with the new gzip backup.
"""
import gzip
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi

ROOT = Path(__file__).resolve().parents[1]
conf = json.loads((ROOT/'config.json').read_text(encoding='utf-8'))
folder = conf['hf_folder'].strip('/')
batch = (ROOT/'data/latest.txt').read_text(encoding='utf-8').strip()
source = ROOT/'data'/batch/'serving.db'
if not source.is_file():
    raise RuntimeError('Rebuilt serving state missing; refuse to update HF pointer')
gzip_path = ROOT/'data'/batch/'serving-valid-open-observations-v3.db.gz'
with source.open('rb') as orig, gzip.open(gzip_path,'wb') as archive:
    shutil.copyfileobj(orig,archive)
compressed = gzip_path.read_bytes()
remote = f'{folder}/state/{batch}/serving-valid-open-observations-v3.db.gz'
manifest = {
    'schema_version':1,
    'batch_id':batch,
    'state_path':remote,
    'sha256':hashlib.sha256(compressed).hexdigest()
}
api=HfApi(token=os.environ['HF_TOKEN'])
commit=api.create_commit(
    repo_id=conf['hf_repo_id'],repo_type='dataset',
    operations=[
        CommitOperationAdd(path_in_repo=remote,path_or_fileobj=str(gzip_path)),
        CommitOperationAdd(path_in_repo=f'{folder}/state/latest.json',
                           path_or_fileobj=json.dumps(manifest).encode('utf-8'))
    ],
    commit_message=f'FoodRadar verified-open store/product history repair {batch}'
)
print('HF_VALID_PRICE_CHECKPOINT '+json.dumps({'batch_id':batch,'sha256':manifest['sha256'],'commit':commit.oid}),flush=True)
