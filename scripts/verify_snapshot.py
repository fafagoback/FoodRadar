"""Verify the raw archive, original menu schema and complete front-end catalog."""
import hashlib,json,sys,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from snapshot_validation import validate_document
output=Path(sys.argv[1])
docs={validate_document(json.loads(p.read_text(encoding='utf-8'))):json.loads(p.read_text(encoding='utf-8')) for p in (output/'menus').glob('*.json')}
report=json.loads((output/'report.json').read_text(encoding='utf-8'))
assert report['discovered']==report['retained']==len(docs)
assert report['excluded']==report['failed']==0
archive=output/'archive'/f'taiwan_menus_{output.name}.tar.gz'
with tarfile.open(archive,'r:gz') as tar:
    archived=[json.load(tar.extractfile(m)) for m in tar.getmembers() if m.name.startswith('Json/') and m.name.endswith('.json')]
assert len(archived)==len(docs)
for doc in archived:
    assert doc==docs[validate_document(doc)]
    assert 'batch_id' not in doc and 'distance_km' not in doc
catalog=json.loads((output/'site/products.json').read_text(encoding='utf-8'))
assert catalog['total']==len(catalog['items'])
assert len({(p['store_id'],p['product_id']) for p in catalog['items']})==catalog['total']
assert all(p['price']>0 for p in catalog['items'])
result={'batch':output.name,'stores':len(docs),'raw_menu_items':sum(len(sec['hasMenuItem']) for d in docs.values() for sec in d['hasMenu']['hasMenuSection']),
        'searchable_products':catalog['total'],'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'archive_roundtrip':True}
print(json.dumps(result,ensure_ascii=False,indent=2))
