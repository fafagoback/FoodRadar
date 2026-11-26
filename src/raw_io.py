"""Bounded immutable Raw I/O: no expanded archive or in-memory nationwide menu."""
from contextlib import contextmanager
import gzip
import hashlib
import io
import json
import os
import pickle
import shutil
import sqlite3
import tarfile
import tempfile
import zlib
from pathlib import Path, PurePosixPath


def disk_report(phase, root, **details):
    usage = shutil.disk_usage(root)
    print("FOODRADAR_DISK=" + json.dumps({
        "phase": phase, "total_bytes": usage.total, "free_bytes": usage.free,
        **details}), flush=True)


def digest_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_name(name):
    p = PurePosixPath(name)
    if not name or p.is_absolute() or ".." in p.parts or "\\" in name or p.as_posix() != name:
        raise ValueError("Unsafe archive member: " + name)
    return name


class RawWriter:
    """Write original bytes directly to one gzip TAR; each member is checked once."""
    def __init__(self, archive):
        self.path = Path(archive)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.tar = tarfile.open(self.path, "x:gz", compresslevel=3)
        self.checks = {}
        self.raw_bytes = 0

    def add(self, name, body, expected):
        safe_name(name)
        if not name.endswith(".body") or name in self.checks:
            raise ValueError("Duplicate or invalid Raw body: " + name)
        actual = hashlib.sha256(body).hexdigest()
        if actual != expected:
            raise ValueError("Raw body SHA256 mismatch: " + name)
        info = tarfile.TarInfo(name)
        info.size = len(body)
        info.mode = 0o644
        info.mtime = 0
        self.tar.addfile(info, io.BytesIO(body))
        self.checks[name] = actual
        self.raw_bytes += len(body)

    def __enter__(self):
        return self

    def __exit__(self, kind, value, tb):
        self.tar.close()
        if kind is not None:
            self.path.unlink(missing_ok=True)


@contextmanager
def seekable_archive(path):
    """Index compressed gzip blocks rather than materializing an expanded TAR."""
    try:
        from indexed_gzip import IndexedGzipFile
    except ImportError:
        # Small local tests can use the standard library; production installs
        # indexed_gzip through requirements.txt.
        if Path(path).stat().st_size > 64 * 1024 * 1024:
            raise RuntimeError("indexed_gzip is required for large Raw archives")
        stream = gzip.open(path, "rb")
    else:
        stream = IndexedGzipFile(filename=str(path), spacing=4 * 1024 * 1024)
    with stream:
        with tarfile.open(fileobj=stream, mode="r:") as archive:
            yield archive


class DocumentSpool:
    """Sequence of exact decoded shop documents, compressed on disk, not in RAM."""
    def __init__(self):
        self.directory = tempfile.TemporaryDirectory(prefix="fr-docs-")
        self.db = sqlite3.connect(Path(self.directory.name) / "docs.sqlite")
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("CREATE TABLE docs (i INTEGER PRIMARY KEY, body BLOB NOT NULL)")
        self.store_ids = []

    @staticmethod
    def encode(doc):
        return zlib.compress(pickle.dumps(doc, protocol=5), level=1)

    def append(self, doc):
        self.db.execute("INSERT INTO docs VALUES (?,?)", (len(self.store_ids), self.encode(doc)))
        self.store_ids.append(doc["store_uuid"])

    def __len__(self):
        return len(self.store_ids)

    def __getitem__(self, i):
        row = self.db.execute("SELECT body FROM docs WHERE i=?", (i,)).fetchone()
        if row is None:
            raise IndexError(i)
        return pickle.loads(zlib.decompress(row[0]))

    def __setitem__(self, i, doc):
        if doc["store_uuid"] != self.store_ids[i]:
            raise ValueError("Document replacement changed canonical identity")
        self.db.execute("UPDATE docs SET body=? WHERE i=?", (self.encode(doc), i))

    def __iter__(self):
        for (body,) in self.db.execute("SELECT body FROM docs ORDER BY i"):
            yield pickle.loads(zlib.decompress(body))

    def close(self):
        self.db.close()
        self.directory.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def local_verified_archive(config, meta, root):
    """Reuse only the current run's locally verified HF publication."""
    marker = Path(root) / "data/raw-stage/verified-local.json"
    if not marker.is_file():
        return None
    row = json.loads(marker.read_text())
    if (row.get("repo_id") != config["hf_repo_id"]
            or row.get("archive_path") != meta.get("archive_path")
            or row.get("sha256") != meta.get("archive_sha256")):
        return None
    path = Path(row["local_path"])
    if not path.is_file() or digest_file(path) != row["sha256"]:
        raise ValueError("Verified local Raw archive changed")
    return path
