"""Verified disposable SQLite checkpoint. Immutable HF Raw remains authoritative."""
import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "data" / "state-checkpoint"
LEGACY_CONTRACTS = ("src/pipeline.py", "src/serving_state.py", "src/raw_capture.py",
             "src/menu_core.py", "src/catalog_complete.py", "src/json_to_db.py",
             "src/raw_price_enrichment.py", "src/verified_prices.py",
             "src/price_observation.py",
             "src/snapshot_validation.py", "src/promotion_evidence.py",
             "src/catalog_web_fields.py")
CONTRACTS = LEGACY_CONTRACTS + ("src/working_quotes.py", "src/compact_checkpoint.py")


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
        if meta.get("format") == "foodradar-checkpoint-v3":
            # Only the exact audited storage-only migration may reuse v3.
            previous = "92f415b05121a93e53bd7ebabd2d1143da1c6479"
            reviewed = {
                "src/pipeline.py":"5d2a821c179600e78a8ba7927a88e5626ac6d364",
                "src/serving_state.py":"5b6b721211ffaf819b638b8550cdd4028b1a7351",
                "src/working_quotes.py":"1f16a712a5ef922653323ca24d0ead70abdefaeb",
                "src/compact_checkpoint.py":"16b78e61fb5d7168bc6742bdf4ccf493e1a160f9",
            }
            try:
                if meta.get("contract_digest") != _historical_digest(root,previous,LEGACY_CONTRACTS):
                    return False
                for file in CONTRACTS:
                    data=(Path(root)/file).read_bytes()
                    if file in reviewed:
                        blob=b"blob "+str(len(data)).encode()+b"\0"+data
                        if hashlib.sha1(blob).hexdigest()!=reviewed[file]:
                            return False
                    elif data!=subprocess.check_output(
                            ["git","show",f"{previous}:{file}"],cwd=root,
                            stderr=subprocess.DEVNULL,timeout=15):
                        return False
                print("FOODRADAR_WORKING_DB_MIGRATION=PASS",flush=True)
                return True
            except (OSError,subprocess.SubprocessError):
                return False
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


def restore(target, records, config, cache=CACHE, root=ROOT, *, lean=False):
    from compact_checkpoint import expand as expand_compact, expand_shards
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
        parts=meta.get("parts") if compact else None
        candidate=cache/("serving.compact.db" if compact else "serving.db")
        count=meta.get("manifest_count")
        expected_hash=meta.get("compact_sha256" if compact else "sqlite_sha256")
        if (meta.get("repo_id") != config["hf_repo_id"]
                or meta.get("folder") != config["hf_folder"].strip("/")
                or type(count) is not int or count<1 or count>len(records)
                or meta.get("manifest_digest") != manifest_digest(records[:count])
                or not compatible_contract(meta,root)):
            raise ValueError("Invalid checkpoint provenance or code contract")
        if not parts and (not candidate.is_file() or not expected_hash or
                          expected_hash != digest_file(candidate)):
            raise ValueError("Invalid checkpoint SHA-256")
        if compact:
            counts=(expand_shards(cache,parts,target,lean=lean) if parts else
                    expand_compact(candidate,target,lean=lean))
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
            "compact_bytes":(sum(p["bytes"] for p in parts) if parts else
                             candidate.stat().st_size if compact else None),
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
    from compact_checkpoint import create as create_compact, create_shards
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
        parts=None
        if info["bytes"]>=20_000_000:
            target.unlink()
            parts=create_shards(source,cache/"shards",shops_per_shard=500)
            if (sum(p["stores"] for p in parts)!=info["stores"]
                    or sum(p["products"] for p in parts)!=info["products"]
                    or sum(p["observations"] for p in parts)!=info["observations"]):
                raise ValueError("Nationwide shards lost records")
        metadata={**index,"format":"foodradar-checkpoint-v3",
                  "compact_sha256":digest_file(target) if parts is None else None,
                  "parts":parts,
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
        "batch":row[0],"bytes":(sum(p["bytes"] for p in parts) if compact and parts else target.stat().st_size),
        "parts":len(parts) if compact and parts else 1,
        "format":metadata["format"],"transient_db_bytes":source.stat().st_size,
    },ensure_ascii=False),flush=True)


# Direct compact checkpoints use the same v3 physical layout; v4 identifies
# the new bounded mutation path. No historical observations are regenerated
# by expanding a full normalized SQL database.
DIRECT_CONTRACTS=CONTRACTS+("src/compact_direct.py",)
PREVIOUS_LAYOUT_CONTRACTS=DIRECT_CONTRACTS+("src/compact_templates.py","src/compact_repack.py")
DIRECT_LAYOUT_CONTRACTS=PREVIOUS_LAYOUT_CONTRACTS+("src/compact_payload.py",)
DIRECT_V3_ANCHOR="3ecd55f11ab6449f1eab661833833d17c97d3dab"
DIRECT_V3_REVIEWED={
    "src/pipeline.py":"a55e5a5a71a2e3d78daf310afb011ceec93901cf",
    "src/compact_direct.py":"27bace902b74505d3e8d214200de7ae8c4b24e65",
}

def direct_contract_digest(root=ROOT):
    h=hashlib.sha256()
    for file in DIRECT_LAYOUT_CONTRACTS:
        h.update(file.encode()+b"\0")
        h.update((Path(root)/file).read_bytes())
    return h.hexdigest()

# Reviewed capture-only migration: the old verified compact DB was produced
# by 48b1356. The three changed contract files affect live transport/retries
# only; archived Raw decoding, price evidence, DB layout and mutation are
# unchanged. Require exact hashes for BOTH endpoints, not a generic bypass.
DIRECT_CAPTURE_MIGRATIONS = (
    ("48b1356a58c07d4aa0825346f3e32ad4edf47ce8",
     "1219b24c4239e5a923b1b141acc9d0e169743890"),
    # Capture request provenance, targeted promo verification, and bounded
    # metadata refresh change future capture only; replay of existing Raw,
    # persisted observations, and compact layout are byte-contract identical.
    ("ec8967d99c608519b8e99892fa331afacf532ab8",
     "f9556966d9469bbe8fb4d5130bde62f46dcb708b"),
    # Physical recompression preserves every decoded MessagePack byte and
    # the schema, so either verified previous checkpoint is directly reusable.
    ("ec8967d99c608519b8e99892fa331afacf532ab8",
     "19731748a3a1672a977b644f09f288786e3192c0"),
    ("a7c6bfb0917d957c1119540fa31f32390fbf60bc",
     "19731748a3a1672a977b644f09f288786e3192c0"),
    # Native partition task scheduling and page capture provenance only:
    # the persisted observation decoder and database schema remain identical.
    ("ec8967d99c608519b8e99892fa331afacf532ab8",
     "d7ce089c208e0b0d2337779b8de6d1c742eab6f6"),
    ("a7c6bfb0917d957c1119540fa31f32390fbf60bc",
     "d7ce089c208e0b0d2337779b8de6d1c742eab6f6"),
    ("19731748a3a1672a977b644f09f288786e3192c0",
     "d7ce089c208e0b0d2337779b8de6d1c742eab6f6"),
    # Bounded native metadata retries do not alter the observation contract.
    ("ec8967d99c608519b8e99892fa331afacf532ab8",
     "91f9c257bfc100bb8c06258046ca6102ad53507f"),
    ("a7c6bfb0917d957c1119540fa31f32390fbf60bc",
     "91f9c257bfc100bb8c06258046ca6102ad53507f"),
    ("19731748a3a1672a977b644f09f288786e3192c0",
     "91f9c257bfc100bb8c06258046ca6102ad53507f"),
)

# Explicit forward-only migration to the lossless physical representation.
# Every previous dictionary token, shop row, quote and native evidence is
# decoded exactly; the old v4 layout remains readable.
DIRECT_LAYOUT_MIGRATIONS = (
    ("ec8967d99c608519b8e99892fa331afacf532ab8","97fe6baca43bb8bcbd087b7fa03c6d372f2b646f"),
    ("a7c6bfb0917d957c1119540fa31f32390fbf60bc","97fe6baca43bb8bcbd087b7fa03c6d372f2b646f"),
    ("668b031becd4514357feee5e1fe930a030b71178","97fe6baca43bb8bcbd087b7fa03c6d372f2b646f"),
    ("19731748a3a1672a977b644f09f288786e3192c0","97fe6baca43bb8bcbd087b7fa03c6d372f2b646f"),
)

# Exact reviewed codec upgrade. Earlier verified checkpoints retain their
# original schema and remain readable; no arbitrary digest bypass.
CODEC_UPGRADE="37fc80992b90312a8a777435d0351aa4dabdf0ab"
CODEC_OLD_LAYOUTS=("97fe6baca43bb8bcbd087b7fa03c6d372f2b646f","7fb36f3df7f43399278eba3582a4f0899b6a7f9d")
CODEC_OLD_CONTRACTS=tuple(dict.fromkeys(old for old,new in DIRECT_LAYOUT_MIGRATIONS))

def compatible_direct_contract(meta,root=ROOT):
    fmt=meta.get("format")
    current=direct_contract_digest(root) if fmt=="foodradar-checkpoint-v4" else None
    if fmt=="foodradar-checkpoint-v4" and meta.get("contract_digest")==current:
        return True
    if fmt=="foodradar-checkpoint-v4":
        try:
            if current==_historical_digest(root,CODEC_UPGRADE,DIRECT_LAYOUT_CONTRACTS):
                for old,files in [(r,PREVIOUS_LAYOUT_CONTRACTS) for r in CODEC_OLD_LAYOUTS]+[(r,DIRECT_CONTRACTS) for r in CODEC_OLD_CONTRACTS]:
                    if meta.get("contract_digest")==_historical_digest(root,old,files):
                        print("FOODRADAR_CHECKPOINT_CODEC_MIGRATION="+old,flush=True)
                        return True
        except (OSError,subprocess.SubprocessError):
            pass
        for old,new in DIRECT_LAYOUT_MIGRATIONS:
            try:
                if (current==_historical_digest(root,new,DIRECT_LAYOUT_CONTRACTS)
                    and meta.get("contract_digest")==_historical_digest(root,old,DIRECT_CONTRACTS)):
                    print("FOODRADAR_CHECKPOINT_LAYOUT_MIGRATION="+old+".."+new,flush=True)
                    return True
            except (OSError,subprocess.SubprocessError):
                continue
        for old,new in DIRECT_CAPTURE_MIGRATIONS:
            try:
                if (current==_historical_digest(root,new,DIRECT_CONTRACTS)
                    and meta.get("contract_digest")==_historical_digest(root,old,DIRECT_CONTRACTS)):
                    print("FOODRADAR_CHECKPOINT_CAPTURE_ONLY_MIGRATION="+old+".."+new,flush=True)
                    return True
            except (OSError,subprocess.SubprocessError):
                continue
    # Explicit reviewed image-only migration from already verified compact state.
    # Accept only the exact old observation contract and exact new code blobs.
    if fmt=="foodradar-checkpoint-v4":
        try:
            historical=("f674d805e55db21ef1d21cfba8f4a15d73ba50e7","ab8fcab771f67a037b0eed331c2ca6a0c90ffbe2")
            old_ok=any(meta.get("contract_digest")==_historical_digest(root,ref,DIRECT_CONTRACTS)
                       for ref in historical)
            reviewed_image={
                "src/compact_direct.py":"2012dc9f908d28ad84b380d07017aa3eab3a100b",
                "src/serving_state.py":"8dc5e0c3f9a09b6134769c7b6bb585de9290efce",
            }
            if old_ok:
                unchanged=True
                for file in DIRECT_CONTRACTS:
                    data=(Path(root)/file).read_bytes()
                    if file in reviewed_image:
                        blob=b"blob "+str(len(data)).encode()+b"\0"+data
                        unchanged &= hashlib.sha1(blob).hexdigest()==reviewed_image[file]
                    else:
                        unchanged &= data==subprocess.check_output(["git","show",f"ab8fcab771f67a037b0eed331c2ca6a0c90ffbe2:{file}"],
                            cwd=root,stderr=subprocess.DEVNULL,timeout=15)
                if unchanged:return True
        except (OSError,subprocess.SubprocessError):
            pass
    # Exact storage-only cleanup: the old full-replay alternative and
    # reporting constants were removed. The direct observation path is unchanged.
    anchor="f674d805e55db21ef1d21cfba8f4a15d73ba50e7"
    reviewed={
        "src/pipeline.py":"69b5fcb595db1bcef6e92df0f670595c46cf5955",
        "src/compact_direct.py":"a9034a15304d3957330ea9b7be55787f4c8b947d",
    }
    if fmt=="foodradar-checkpoint-v4":
        previous=anchor
        previous_files=DIRECT_CONTRACTS
    elif fmt=="foodradar-checkpoint-v3":
        previous=DIRECT_V3_ANCHOR
        previous_files=CONTRACTS
    else:
        return False
    try:
        if meta.get("contract_digest")!=_historical_digest(root,previous,previous_files):
            return False
        for file in DIRECT_CONTRACTS:
            data=(Path(root)/file).read_bytes()
            if file in reviewed:
                blob=b"blob "+str(len(data)).encode()+b"\0"+data
                if hashlib.sha1(blob).hexdigest()!=reviewed[file]:return False
            elif data!=subprocess.check_output(["git","show",f"{anchor}:{file}"],
                    cwd=root,stderr=subprocess.DEVNULL,timeout=15):return False
        return True
    except (OSError,subprocess.SubprocessError):
        return False

def restore_direct(records,config,cache=CACHE,root=ROOT):
    from compact_direct import open_collection
    cache=Path(cache);state=None
    if not (cache/"manifest.json").is_file():return None
    try:
        meta=json.loads((cache/"manifest.json").read_text(encoding="utf-8"))
        count=meta["manifest_count"]
        if (meta.get("repo_id")!=config["hf_repo_id"]
            or meta.get("folder")!=config["hf_folder"].strip("/")
            or type(count) is not int or not 0<count<=len(records)
            or meta.get("manifest_digest")!=manifest_digest(records[:count])
            or not compatible_direct_contract(meta,root)):
            raise ValueError("Invalid direct checkpoint provenance or code contract")
        parts=meta.get("parts")
        if parts:
            paths=[]
            for p in parts:
                rel=Path(p["path"])
                if len(rel.parts)!=2 or rel.parts[0]!="shards" or rel.is_absolute() or ".." in rel.parts:
                    raise ValueError("Unsafe checkpoint partition path")
                path=cache/rel
                if digest_file(path)!=p["sha256"]:raise ValueError("Corrupt partition SHA")
                paths.append(path)
        else:
            path=cache/"serving.compact.db"
            if digest_file(path)!=meta["compact_sha256"]:raise ValueError("Corrupt compact SHA")
            paths=[path]
        state=open_collection(paths)
        if any(p.conn.execute("PRAGMA quick_check").fetchone()[0]!="ok" for p in state.parts):
            raise ValueError("Invalid compact partition")
        if (state.stats()!={k:meta[k] for k in ("stores","products","observations")}
            or state.meta.get("latest_batch")!=meta["batch_id"]
            or len(state.batch_rows())!=meta["verified_batches"]):
            raise ValueError("Checkpoint counts or batch mismatch")
        for part in state.parts:part.remove_image_urls()
        print("FOODRADAR_CHECKPOINT_RESTORED="+json.dumps({
            "batch":meta["batch_id"],"manifests":count,"verified_batches":meta["verified_batches"],
            "compact_bytes":state.metrics()["compact_bytes"],"direct":True},ensure_ascii=False),flush=True)
        return state,meta["batch_id"],count
    except (OSError,ValueError,KeyError,IndexError,sqlite3.DatabaseError) as e:
        if state is not None:state.close()
        print("FOODRADAR_CHECKPOINT_REJECTED="+json.dumps({
            "reason":str(e)[:200],"fallback":"direct_HF_Raw_replay"}),flush=True)
        return None

def new_direct(cache=CACHE):
    import shutil
    from compact_direct import CompactStore,CompactCollection
    cache=Path(cache)
    # This is only disposable runner-local acceleration, never HF Raw.
    shutil.rmtree(cache,ignore_errors=True)
    cache.mkdir(parents=True)
    return CompactCollection([CompactStore(cache/"serving.compact.db",empty=True)])

def save_direct(state,raw_index_path,cache=CACHE,root=ROOT):
    from compact_direct import MAX_BYTES
    cache=Path(cache)
    index=json.loads(Path(raw_index_path).read_text(encoding="utf-8"))
    if (index.get("contract_digest")!=direct_contract_digest(root)
        or state.meta.get("latest_batch")!=index.get("batch_id")
        or len(state.batch_rows())!=index.get("verified_batches")):
        raise ValueError("Direct cache publication provenance differs")
    state.flush()
    from compact_repack import optimize_part
    for part in state.parts:
        optimize_part(part)
    if any(p.path.stat().st_size>=MAX_BYTES for p in state.parts):
        raise ValueError("Compact partition exceeds strict byte budget")
    counts=state.stats()
    parts=[]
    for p in state.parts:
        if p.conn.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
            raise ValueError("Compact checkpoint integrity check failed")
        parts.append({"path":str(p.path.relative_to(cache)),"sha256":digest_file(p.path),
                      "bytes":p.path.stat().st_size})
    single=len(parts)==1 and parts[0]["path"]=="serving.compact.db"
    metadata={**index,"format":"foodradar-checkpoint-v4",**counts,
              "compact_sha256":parts[0]["sha256"] if single else None,
              "parts":None if single else parts}
    temp=cache/"manifest.tmp"
    temp.write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
    temp.replace(cache/"manifest.json")
    (cache/"serving.db").unlink(missing_ok=True)
    print("FOODRADAR_CHECKPOINT_PREPARED="+json.dumps({
        "batch":index["batch_id"],"bytes":sum(p["bytes"] for p in parts),
        "parts":len(parts),"format":metadata["format"],
        **state.metrics()},ensure_ascii=False),flush=True)
