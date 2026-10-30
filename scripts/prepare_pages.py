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
# Website deployment is independent of HF crawling. Always read the
# ALREADY-PUBLISHED batch from Turso rather than stale local data/latest.txt.
# This prevents deploying a website that claims an uncommitted HF snapshot.
from publish_turso_delta import Remote

url = os.environ["TURSO_DATABASE_URL"].replace("libsql://", "https://")
token = os.environ["TURSO_READONLY_TOKEN"]
remote = Remote(url, token)
try:
    rows = remote.query("SELECT value FROM metadata WHERE key='latest_batch'")
    if len(rows) != 1:
        raise ValueError("Turso has no unique verified latest_batch metadata")
    batch = json.loads(rows[0]["value"])
finally:
    remote.close()
commit = os.environ.get("GITHUB_SHA", "").strip()
if (not isinstance(batch,str) or not commit or len(batch) != 14
        or not batch.isdigit()):
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
# Fingerprint all browser assets with the verified Pages release rather than
# a fresh timestamp on every visit. Release changes still invalidate caches.
index = root / "web/index.html"
source = index.read_text(encoding="utf-8")
if source.count("__FOODRADAR_RELEASE__") != 1:
    raise ValueError("Pages HTML release placeholder not found")
index.write_text(source.replace("__FOODRADAR_RELEASE__", manifest["version"]), encoding="utf-8")
print("FOODRADAR_PAGES_VERSION=" + manifest["version"], flush=True)
