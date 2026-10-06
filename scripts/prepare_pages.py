"""Pages contains frontend assets only; business data is queried from Turso."""
from pathlib import Path
root = Path(__file__).resolve().parents[1]
data = root / 'web/data'
if data.exists():
    paths = list(data.iterdir())
    if any(path.is_dir() for path in paths):
        raise ValueError('Unexpected directory in Pages data')
    for path in paths:
        path.unlink()
