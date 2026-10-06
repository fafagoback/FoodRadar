"""Pages hosts the frontend, not the product database or history lake."""
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
data = root/'web/data'
data.mkdir(exist_ok=True)
stats = data/'stats.json'
if stats.exists():
    value = json.loads(stats.read_text(encoding='utf-8'))
    value['query_source'] = 'FoodRadar Turso'
    value['fallback_available'] = False
    stats.write_text(json.dumps(value,ensure_ascii=False),encoding='utf-8')
for path in data.iterdir():
    if path.name == 'stats.json':
        continue
    if path.is_dir():
        raise ValueError('Unexpected directory in Pages data; refusing to package it')
    else:
        path.unlink()
for name in ['discounts','new_stores','new_products','promotions','products']:
    (data/f'{name}.json').write_text(json.dumps({'status':'unavailable','items':[],
        'message':'請連線至查詢資料庫；靜態網頁不保存完整查詢資料。'}),encoding='utf-8')
(data/'history.json').write_text('{"status":"unavailable","history":{}}',encoding='utf-8')
