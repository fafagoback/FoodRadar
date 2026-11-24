"""Verified disposable SQLite checkpoint. Immutable HF Raw remains authoritative."""
import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "data" / "state-checkpoint"
CONTRACTS = ("src/pipeline.py", "src/serving_state.py", "src/raw_capture.py",
             "src/menu_core.py", "src/catalog_complete.py", "src/json_to_db.py",
             "src/raw_price_enrichment.py", "src/verified_prices.py",
             "src/price_observation.py",
             "src/snapshot_validation.py", "src/promotion_evidence.py",
             "src/catalog_web_fields.py")


def digest_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for part in iter(lambda: f.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def contract_digest(root=ROOT):
    h = hashlib.sha256()
    for file in CONTRACTS:
        h.update(file.encode() + b"\0")
        h.update((Path(root) / file).read_bytes())
    return h.hexdigest()


# Only the raw-to-observation contract invalidates the checkpoint. Weekly
# prices are derived afresh on every restore_history call.
# Legacy caches are accepted only for reviewed, exact historical contracts.
# The changes between these commits and the anchor affect capture retries
# or weekly projections, not persisted native observations.
MIGRATION_ANCHOR = "f05afa66b8483d93781816bacee75f3d8cb944c6"
LEGACY_CONTRACT_REFS = (
    ("a5bf4dd71aa46d92bb60349408b5f86d721296ae", False),
    ("2e91a6a748374bbe1e7df41dfb7622a17e2f57f9", True),
    ("86eac20a8a8c77d7f83d673016d5dfa476e0097e", True),
    ("4c758cf2369e81fdc249adb8b7bada42108b20b6", True),
    (MIGRATION_ANCHOR, True),
)


def _historical_digest(root, ref, files):
    h = hashlib.sha256()
    for file in files:
        data = subprocess.check_output(
            ["git", "show", f"{ref}:{file}"], cwd=root,
            stderr=subprocess.DEVNULL, timeout=15)
        h.update(file.encode() + b"\0")
        h.update(data)
    return h.hexdigest()


def compatible_contract(meta, root):
    current = contract_digest(root)
    if meta.get("format") == "foodradar-checkpoint-v2":
        return meta.get("contract_digest") == current
    if meta.get("format") != "foodradar-checkpoint-v1":
        return False
    try:
        if _historical_digest(root, MIGRATION_ANCHOR, CONTRACTS) != current:
            return False
        pos = CONTRACTS.index("src/price_observation.py")
        for ref, has_oscillation in LEGACY_CONTRACT_REFS:
            projection = ("src/verified_weekly.py",)
            if has_oscillation:
                projection += ("src/price_oscillation.py",)
            files = CONTRACTS[:pos] + projection + CONTRACTS[pos:]
            if meta.get("contract_digest") == _historical_digest(root, ref, files):
                print("FOODRADAR_CHECKPOINT_LEGACY_CONTRACT_MIGRATED=" + ref, flush=True)
                return True
    except (OSError, subprocess.SubprocessError):
        pass
    return False


def manifest_digest(rows):
    h = hashlib.sha256()
    for path, _meta, sha in rows:
        h.update(path.encode() + b"\0" + sha.encode() + b"\n")
    return h.hexdigest()


def restore(target, records, config, cache=CACHE, root=ROOT):
    meta_path, candidate = Path(cache) / "manifest.json", Path(cache) / "serving.db"
    if not meta_path.is_file() or not candidate.is_file():
        print("FOODRADAR_CHECKPOINT_MISS=no_cache", flush=True)
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        count = meta.get("manifest_count")
        if (meta.get("format") not in ("foodradar-checkpoint-v1", "foodradar-checkpoint-v2")
                or meta.get("repo_id") != config["hf_repo_id"]
                or meta.get("folder") != config["hf_folder"].strip("/")
                or type(count) is not int or count < 1 or count > len(records)
                or meta.get("manifest_digest") != manifest_digest(records[:count])
                or not compatible_contract(meta, root)
                or meta.get("sqlite_sha256") != digest_file(candidate)):
            raise ValueError("invalid checkpoint provenance, code contract or SHA-256")
        with sqlite3.connect(candidate) as db:
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("SQLite quick_check failed")
            row = db.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
            db_count = db.execute("SELECT COUNT(*) FROM crawl_batches").fetchone()[0]
        if not row or row[0] != meta.get("batch_id") or db_count != meta.get("verified_batches"):
            raise ValueError("checkpoint and database batch mismatch")
        import shutil
        shutil.copy2(candidate, target)
        print("FOODRADAR_CHECKPOINT_RESTORED=" + json.dumps({
            "batch": row[0], "manifests": count, "verified_batches": db_count
        }), flush=True)
        return row[0], count
    except (OSError, ValueError, sqlite3.DatabaseError) as e:
        Path(target).unlink(missing_ok=True)
        print("FOODRADAR_CHECKPOINT_REJECTED=" + json.dumps({
            "reason": str(e)[:200], "fallback": "complete_HF_Raw"
        }), flush=True)
        return None


def save(source, raw_index_path, cache=CACHE, root=ROOT):
    """This cache is reusable only after verified SQLite and Pages-source checks pass."""
    import shutil
    index = json.loads(Path(raw_index_path).read_text(encoding="utf-8"))
    if index.get("contract_digest") != contract_digest(root):
        raise ValueError("ETL contract changed during release")
    with sqlite3.connect(source) as db:
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("serving database failed integrity check")
        row = db.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
        count = db.execute("SELECT COUNT(*) FROM crawl_batches").fetchone()[0]
    if not row or index.get("batch_id") != row[0] or index.get("verified_batches") != count:
        raise ValueError("Raw manifest state differs from serving DB")
    cache = Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / "serving.db"
    shutil.copy2(source, target)
    (cache / "manifest.json").write_text(json.dumps({
        **index, "format": "foodradar-checkpoint-v2",
        "sqlite_sha256": digest_file(target)
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print("FOODRADAR_CHECKPOINT_PREPARED=" + json.dumps({
        "batch": row[0], "bytes": target.stat().st_size
    }), flush=True)
