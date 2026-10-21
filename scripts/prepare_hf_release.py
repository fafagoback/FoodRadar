"""Initialize/retry serving from cloud Raw without running a local crawler."""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from pipeline import restore_history
from build_web_db import build_web_db
from repair_effective_prices import repair

restore_history(json.loads((ROOT/'config.json').read_text(encoding='utf-8')),
                rebuild_all=os.environ.get('FOODRADAR_REBUILD_VALID_PRICE_HISTORY') == '1')
batch = (ROOT/'data/latest.txt').read_text(encoding='utf-8').strip()
repair(ROOT/'data'/batch/'serving.db')
build_web_db(ROOT/'data'/batch/'serving.db',ROOT/'data'/batch/'packed-serving.db')
