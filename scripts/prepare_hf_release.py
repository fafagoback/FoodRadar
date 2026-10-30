"""Initialize/retry serving from cloud Raw without running a local crawler."""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from pipeline import restore_history
from build_web_db import build_web_db
from verified_weekly import update_verified_weekly

restore_history(json.loads((ROOT/'config.json').read_text(encoding='utf-8')),
                rebuild_all=os.environ.get('FOODRADAR_REBUILD_VALID_PRICE_HISTORY') == '1')
batch = (ROOT/'data/latest.txt').read_text(encoding='utf-8').strip()
# Only rebuild the seven-day comparison from V2 certified native quotes.
import sqlite3
with sqlite3.connect(ROOT/'data'/batch/'serving.db') as db:
    result = update_verified_weekly(db,batch)
    print('V2_VERIFIED_PRICE_AUDIT='+json.dumps(result),flush=True)
build_web_db(ROOT/'data'/batch/'serving.db',ROOT/'data'/batch/'packed-serving.db')
