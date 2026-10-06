"""Emit only a database-scoped read-only credential for GitHub Pages."""
import base64
import json
import os
from pathlib import Path
from publish_turso_delta import Remote

root = Path(__file__).resolve().parents[1]
url = os.environ['TURSO_DATABASE_URL'].replace('libsql://','https://')
token = os.environ['TURSO_READONLY_TOKEN']
claims = json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '==='))
if claims.get('a') != 'ro':
    raise ValueError('Browser token must be read-only')
remote = Remote(url, token)
try:
    meta = {r['key']: json.loads(r['value']) for r in remote.query('SELECT key,value FROM metadata')}
    if meta.get('application') != 'FoodRadar':
        raise ValueError('Browser database is not FoodRadar')
finally:
    remote.close()
config = dict(API_BASE_URL='./data', ENABLE_TURSO=True, ENABLE_DUCKDB=False,
              SINGLE_POINT=True, TURSO_DATABASE_URL=url, TURSO_READONLY_TOKEN=token)
(root/'web/config.js').write_text('window.UBER_RADAR_CONFIG = '+json.dumps(config,indent=2)+';\n',encoding='utf-8')
