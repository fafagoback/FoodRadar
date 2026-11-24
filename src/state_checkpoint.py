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
    if meta.get("format") in ("foodradar-checkpoint-v2", "foodradar-checkpoint-v3"):
        if meta.get("contract_digest") == current:
            return True
        if meta.get("format") != "foodradar-checkpoint-v2":
            return False
        # The exact previous deployed contract differs only in how a verified
        # TAR is read. Require every observation-producing module to be byte-
        # identical to either the old production ref or the two reviewed
        # seekable-reader revisions (pinned Git blob hashes).
        previous = "29ea5410840233c1334f7dfef599881730d7ebc4"
        revised = {
            "src/raw_capture.py": "79ca2526353b720be9d6abdc97362d5aa4e98af0",
            "src/pipeline.py": "3e31841d0d4c8d7a305ccb4938e6e2d6c5ee564a",
        }
        try:
            if meta.get("contract_digest") != _historical_digest(root,previous,CONTRACTS):
                return False
            for file in CONTRACTS:
                data = (Path(root)/file).read_bytes()
                if file in revised:
                    blob = (b"blob " + str(len(data)).encode() + b"\0" + data)
                    if hashlib.sha1(blob).hexdigest() != revised[file]:
                        return False
                elif data != subprocess.check_output(
                        ["git","show",f"{previous}:{file}"],cwd=root,
                        stderr=subprocess.DEVNULL,timeout=15):
                    return False
            print("FOODRADAR_CHECKPOINT_REVIEWED_SEEKABLE_READER_MIGRATION=True",flush=True)
            return True
        except (OSError,subprocess.SubprocessError):
            return False
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
    from compact_checkpoint import expand as expand_compact
    cache=Path(cache)
    meta_path=cache/"manifest.json"
    if not meta_path.is_file():
        print("FOODRADAR_CHECKPOINT_MISS=no_cache",flush=True)
        return None
    try:
        meta=json.loads(meta_path.read_text(encoding="utf-8"))
        fmt=meta.get("format")
        if fmt not in ("foodradar-checkpoint-v1","foodradar-checkpoint-v2",
                       "foodradar-checkpoint-v3"):
            raise ValueError("Unsupported checkpoint version")
        compact=fmt=="foodradar-checkpoint-v3"
        candidate=cache/("serving.compact.db" if compact else "serving.db")
        count=meta.get("manifest_count")
        expected_hash=meta.get("compact_sha256" if compact else "sqlite_sha256")
        if (not candidate.is_file() or not expected_hash
                or meta.get("repo_id") != config["hf_repo_id"]
                or meta.get("folder") != config["hf_folder"].strip("/")
                or type(count) is not int or count<1 or count>len(records)
                or meta.get("manifest_digest") != manifest_digest(records[:count])
                or not compatible_contract(meta,root)
                or expected_hash != digest_file(candidate)):
            raise ValueError("Invalid checkpoint provenance, code contract or SHA-256")
        if compact:
            counts=expand_compact(candidate,target)
            if counts["stores"] != meta.get("stores") or counts["products"] != meta.get("products"):
                raise ValueError("Compact checkpoint identity counts changed")
            if counts["observations"] != meta.get("observations"):
                raise ValueError("Compact checkpoint price evidence counts changed")
        else:
            import shutil
            shutil.copy2(candidate,target)
        with sqlite3.connect(target) as db:
            if db.execute("PRAGMA quick_check").fetchone()[0]!="ok":
                raise ValueError("Checkpoint quick_check failed")
            row=db.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
            db_count=db.execute("SELECT COUNT(*) FROM crawl_batches").fetchone()[0]
        if not row or row[0]!=meta.get("batch_id") or db_count!=meta.get("verified_batches"):
            raise ValueError("Checkpoint/database batch mismatch")
        print("FOODRADAR_CHECKPOINT_RESTORED="+json.dumps({
            "batch":row[0],"manifests":count,"verified_batches":db_count,
            "compact_bytes":candidate.stat().st_size if compact else None,
            "historical_hashes_in_hf_raw":compact,
        },ensure_ascii=False),flush=True)
        return row[0],count
    except (OSError,ValueError,KeyError,IndexError,sqlite3.DatabaseError) as exc:
        Path(target).unlink(missing_ok=True)
        print("FOODRADAR_CHECKPOINT_REJECTED="+json.dumps({
            "reason":str(exc)[:200],"fallback":"complete_HF_Raw"},ensure_ascii=False),flush=True)
        return None
def save(source, raw_index_path, cache=CACHE, root=ROOT, *, compact=True):
    """Save a verified derived checkpoint. The original HF Raw never changes."""
    from compact_checkpoint import create as create_compact
    import shutil
    source=Path(source)
    index=json.loads(Path(raw_index_path).read_text(encoding="utf-8"))
    if index.get("contract_digest") != contract_digest(root):
        raise ValueError("ETL contract changed during release")
    with sqlite3.connect(source) as db:
        if db.execute("PRAGMA quick_check").fetchone()[0]!="ok":
            raise ValueError("Serving database failed quick_check")
        row=db.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
        count=db.execute("SELECT COUNT(*) FROM crawl_batches").fetchone()[0]
    if not row or index.get("batch_id")!=row[0] or index.get("verified_batches")!=count:
        raise ValueError("Raw manifest differs from serving state")
    cache=Path(cache)
    cache.mkdir(parents=True,exist_ok=True)
    if compact:
        target=cache/"serving.compact.db"
        info=create_compact(source,target)
        if info["bytes"]>=20_000_000:
            raise ValueError(f"Compact checkpoint over 20 MB: {info['bytes']}")
        metadata={**index,"format":"foodradar-checkpoint-v3",
                  "compact_sha256":digest_file(target),
                  "stores":info["stores"],"products":info["products"],
                  "observations":info["observations"],
                  "expanded_work_db_bytes":source.stat().st_size}
        (cache/"serving.db").unlink(missing_ok=True)
    else:
        target=cache/"serving.db"
        shutil.copy2(source,target)
        metadata={**index,"format":"foodradar-checkpoint-v2",
                  "sqlite_sha256":digest_file(target)}
    path=cache/"manifest.json"
    temp=cache/"manifest.tmp"
    temp.write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
    temp.replace(path)
    print("FOODRADAR_CHECKPOINT_PREPARED="+json.dumps({
        "batch":row[0],"bytes":target.stat().st_size,
        "format":metadata["format"],"transient_db_bytes":source.stat().st_size,
    },ensure_ascii=False),flush=True)
