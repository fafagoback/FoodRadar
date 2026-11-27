"""Offline-only recovery: validate and persist exactly the next immutable Raw batch."""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from pipeline import restore_history

if __name__ == "__main__":
    os.environ["FOODRADAR_PERSIST_REPLAY_PROGRESS"] = "1"
    os.environ["FOODRADAR_REPLAY_ONE_BATCH"] = "1"
    config = json.loads((ROOT / "config.json").read_text())
    state = restore_history(config)
    state.close()
