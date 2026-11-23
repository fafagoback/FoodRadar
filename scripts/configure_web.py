"""Emit no browser-facing database credentials."""
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
(ROOT/"web"/"config.js").write_text(
    "window.UBER_RADAR_CONFIG = { ENABLE_STATIC: true, ENABLE_TURSO: false, "
    "ENABLE_DUCKDB: false, SINGLE_POINT: true };\n", encoding="utf-8")
print("FOODRADAR_BROWSER_DATA_SOURCE=immutable-static-only", flush=True)
