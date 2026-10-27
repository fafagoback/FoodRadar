"""Prepare GitHub Pages for a verified deployment with a changing version ID."""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

root = Path(__file__).resolve().parents[1]
data = root / "web/data"
if data.exists():
    paths = list(data.iterdir())
    if any(path.is_dir() for path in paths):
        raise ValueError("Unexpected directory in Pages data")
    for path in paths:
        path.unlink()

# A fixed version.json stopped already-open pages from detecting new releases.
# Bind the version to both the verified ingestion batch and source commit so
# changing the UI without a new crawl also triggers the page's update watcher.
batch_path = root / "data/latest.txt"
batch = batch_path.read_text(encoding="utf-8").strip() if batch_path.exists() else ""
commit = os.environ.get("GITHUB_SHA", "").strip()
if not batch or not commit or len(batch) != 14 or not batch.isdigit():
    raise ValueError("Pages release requires verified batch ID and GITHUB_SHA")
now = datetime.now(timezone(timedelta(hours=8)))
manifest = {
    "version": f"{batch}-{commit[:12]}",
    "buildTime": now.strftime("%Y-%m-%d %H:%M:%S +08:00"),
    "batch": batch,
    "commit": commit,
}
(root / "web/version.json").write_text(
    json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
print("FOODRADAR_PAGES_VERSION=" + manifest["version"], flush=True)
