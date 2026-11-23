"""Initialize/retry serving from cloud Raw without running a local crawler."""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from pipeline import restore_history
from build_web_db import build_web_db
from time import perf_counter
from state_checkpoint import save as save_checkpoint
from audit_store_identity import audit_stores

started=perf_counter()
restore_history(json.loads((ROOT/'config.json').read_text(encoding='utf-8')),
                rebuild_all=os.environ.get('FOODRADAR_REBUILD_VALID_PRICE_HISTORY') == '1')
batch = (ROOT/'data/latest.txt').read_text(encoding='utf-8').strip()
# Only rebuild the seven-day comparison from V2 certified native quotes.
import sqlite3
with sqlite3.connect(ROOT/'data'/batch/'serving.db') as db:
    result={'deals':db.execute('SELECT COUNT(*) FROM products WHERE seven_day_is_price_deal=1').fetchone()[0]}
    print('V2_VERIFIED_PRICE_AUDIT='+json.dumps(result),flush=True)
    identity_report = audit_stores(db)
    print('FOODRADAR_STORE_HISTORY_AUDIT='+json.dumps(identity_report,ensure_ascii=False),flush=True)
    (ROOT/'data'/batch/'store-identity-report.json').write_text(
        json.dumps(identity_report,ensure_ascii=False,indent=2),encoding='utf-8')
build_web_db(ROOT/'data'/batch/'serving.db',ROOT/'data'/batch/'packed-serving.db')

print('FOODRADAR_WEB_PACK_AND_AUDIT_SECONDS='+str(round(perf_counter()-started,2)),flush=True)
# Prepared now; workflow saves to cache only AFTER Turso live audit succeeds.
try:
    save_checkpoint(ROOT/'data'/batch/'serving.db',
                    ROOT/'data'/batch/'raw-manifest-index.json')
except (OSError, ValueError, sqlite3.DatabaseError) as exc:
    # Acceleration is optional; never prevent an otherwise verified publication.
    import shutil
    shutil.rmtree(ROOT/'data'/'state-checkpoint', ignore_errors=True)
    print('FOODRADAR_CHECKPOINT_PREPARE_WARNING=' + str(exc)[:200], flush=True)
