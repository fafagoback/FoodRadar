"""Shared FR/FRT HF lifecycle; only the selected dedicated dataset is writable."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from price_retention import RETENTION_DAYS
_ALLOWED = {"fafagoback/FoodRadar":"hub-google/FoodRadar", "fafagoback/FoodRadar-TW":"hub-google/Foodradar-TW"}
REPO = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))["hf_repo_id"]
FOLDER = "Raw"
STATE = "State/current/"
LEDGER = "State/retired-raw-index.json"
TW = timezone(timedelta(hours=8))
BATCH = re.compile(r"Raw/(\d{14})/(.+)")
HEX = re.compile(r"[0-9a-f]{64}")
CACHE = ROOT / "data/state-checkpoint"


def config():
    value = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    github_repo = os.getenv("GITHUB_REPOSITORY")
    if (REPO not in _ALLOWED.values() or value.get("hf_repo_id") != REPO
            or value.get("hf_folder") != FOLDER
            or (github_repo and _ALLOWED.get(github_repo) != REPO)):
        raise ValueError("Refusing cleanup outside dedicated project Raw")
    if value.get("hf_raw_retention_days") != RETENTION_DAYS:
        raise ValueError("Unexpected retention configuration")
    return value


def pack(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def snapshot(api):
    rev = api.repo_info(REPO, repo_type="dataset", revision="main").sha
    files = set(api.list_repo_files(REPO, repo_type="dataset", revision=rev))
    return rev, files


def remote_path(path, rev):
    from huggingface_hub import hf_hub_download
    return Path(hf_hub_download(REPO, path, repo_type="dataset",
                               revision=rev, token=os.environ["HF_TOKEN"]))


def read(path, rev):
    return remote_path(path, rev).read_bytes()


def batch_of(path):
    match = BATCH.fullmatch(path)
    if not match:
        return None
    try:
        datetime.strptime(match[1], "%Y%m%d%H%M%S")
    except ValueError:
        raise ValueError("Invalid Raw batch timestamp: " + path)
    return match[1]


def expired_batches(files, now):
    cutoff = (now.astimezone(TW) - timedelta(days=RETENTION_DAYS)).strftime("%Y%m%d%H%M%S")
    return {batch for path in files if (batch := batch_of(path)) and batch < cutoff}


def retired_rows(files, rev):
    if LEDGER not in files:
        return []
    ledger = json.loads(read(LEDGER, rev))
    if ledger.get("format") != "frt-retired-raw-v1" or ledger.get("repo") != REPO:
        raise ValueError("Invalid retired Raw index")
    result = []
    for entry in ledger["entries"]:
        path, sha = entry["path"], entry["manifest_sha256"]
        batch = batch_of(path)
        if (not batch or path != f"Raw/{batch}/manifest.json"
                or not HEX.fullmatch(sha) or type(entry["replay"]) is not bool
                or (entry["replay"] and not HEX.fullmatch(entry["archive_sha256"]))):
            raise ValueError("Invalid retired manifest receipt")
        result.append((path, {"batch_id": batch, "_frt_retired": True,
                             "replay": entry["replay"],
                             "archive_sha256": entry["archive_sha256"]}, sha))
    if [row[0] for row in result] != sorted(set(row[0] for row in result)):
        raise ValueError("Unsorted or duplicate retired manifest receipts")
    return result


def combine_records(records, retired):
    combined = sorted([*records, *retired], key=lambda row: row[0])
    paths = [row[0] for row in combined]
    if len(paths) != len(set(paths)):
        raise ValueError("Retired and active Raw overlap")
    if combined[:len(retired)] != retired:
        raise ValueError("Retired Raw is not a contiguous history prefix")
    return combined


def history_records(api, live_records):
    config()
    rev, files = snapshot(api)
    return combine_records(live_records, retired_rows(files, rev))


def all_records(files, rev):
    live = []
    for path in sorted(files):
        batch = batch_of(path)
        if batch and path == f"Raw/{batch}/manifest.json":
            data = read(path, rev)
            live.append((path, json.loads(data), hashlib.sha256(data).hexdigest()))
    return combine_records(live, retired_rows(files, rev))


def check_prefix(meta, records):
    from state_checkpoint import manifest_digest
    from pipeline import raw_manifest_should_replay
    n = meta["manifest_count"]
    if (meta.get("format") != "foodradar-checkpoint-v4"
            or meta.get("repo_id") != REPO or meta.get("folder") != FOLDER
            or type(n) is not int or not 0 < n <= len(records)
            or meta["manifest_digest"] != manifest_digest(records[:n])):
        raise ValueError("Durable checkpoint does not cover the verified Raw prefix")
    replayed = [row for row in records[:n] if raw_manifest_should_replay(row[1], row[0], FOLDER)]
    if (len(replayed) != meta["verified_batches"] or not replayed
            or replayed[-1][1]["batch_id"] != meta["batch_id"]):
        raise ValueError("Durable checkpoint batch coverage mismatch")
    return n


def checkpoint_parts(meta):
    if meta.get("parts"):
        parts = {p["path"]: p["sha256"] for p in meta["parts"]}
    else:
        parts = {"serving.compact.db": meta["compact_sha256"]}
    for path, sha in parts.items():
        if (path != "serving.compact.db" and not re.fullmatch(r"shards/[^/]+\.db", path)) or not HEX.fullmatch(sha):
            raise ValueError("Unsafe compact checkpoint partition")
    return parts


def verify_remote_checkpoint(api, meta, files, rev):
    parts = checkpoint_parts(meta)
    expected = {STATE + path for path in parts} | {STATE + "manifest.json"}
    if expected != {p for p in files if p.startswith(STATE)}:
        raise ValueError("Durable checkpoint remote inventory mismatch")
    entries = api.get_paths_info(REPO, [STATE + p for p in parts],
                                repo_type="dataset", revision=rev, expand=True)
    if len(entries) != len(parts):
        raise ValueError("Missing compact checkpoint remote partitions")
    for entry in entries:
        relative = entry.path[len(STATE):]
        expected_sha = parts[relative]
        lfs = getattr(entry, "lfs", None)
        sha = lfs.get("sha256") if isinstance(lfs, dict) else getattr(lfs, "sha256", None)
        if not sha:
            sha = digest(remote_path(entry.path, rev))
        if sha != expected_sha:
            raise ValueError("Durable checkpoint remote SHA mismatch: " + relative)


def persist(api):
    from huggingface_hub import CommitOperationAdd, CommitOperationDelete
    config()
    meta_path = CACHE / "manifest.json"
    meta = json.loads(meta_path.read_bytes())
    if meta.get("retention_days") != RETENTION_DAYS or not meta.get("retention_cutoff"):
        raise ValueError("Missing 60-day price retention proof")
    rev, files = snapshot(api)
    records = all_records(files, rev)
    new_count = check_prefix(meta, records)
    if STATE + "manifest.json" in files:
        previous = json.loads(read(STATE + "manifest.json", rev))
        old_count = check_prefix(previous, records)
        if old_count > new_count:
            raise ValueError("Refusing to replace newer durable checkpoint")
    parts = checkpoint_parts(meta)
    for path, sha in parts.items():
        if digest(CACHE / path) != sha:
            raise ValueError("Invalid local compact checkpoint partition")
    expected = {STATE + path for path in parts} | {STATE + "manifest.json"}
    ops = [CommitOperationDelete(path_in_repo=p) for p in sorted(files)
           if p.startswith(STATE) and p not in expected]
    ops.extend(CommitOperationAdd(path_in_repo=STATE + path, path_or_fileobj=str(CACHE / path))
               for path in parts)
    ops.append(CommitOperationAdd(path_in_repo=STATE + "manifest.json", path_or_fileobj=str(meta_path)))
    api.create_commit(REPO, repo_type="dataset", revision="main", parent_commit=rev,
                      operations=ops, commit_message="Replace verified project compact state")
    rev, files = snapshot(api)
    if read(STATE + "manifest.json", rev) != meta_path.read_bytes():
        raise ValueError("Durable checkpoint manifest changed")
    verify_remote_checkpoint(api, meta, files, rev)
    print("FOODRADAR_DURABLE_CHECKPOINT_SAVED=" + meta["batch_id"], flush=True)


def restore(api, rebuild=False):
    config()
    rev, files = snapshot(api)
    retired = retired_rows(files, rev)
    if rebuild and retired:
        raise ValueError("Full Raw rebuild is impossible after 60-day retention; use verified compact state")
    if rebuild:
        return
    if STATE + "manifest.json" not in files:
        if retired:
            raise ValueError("Retired Raw requires durable compact checkpoint")
        return
    raw = read(STATE + "manifest.json", rev)
    meta = json.loads(raw)
    parts = checkpoint_parts(meta)
    local = CACHE / "manifest.json"
    if local.is_file() and local.read_bytes() == raw and all(
            (CACHE / p).is_file() and digest(CACHE / p) == sha for p, sha in parts.items()):
        print("FOODRADAR_DURABLE_CHECKPOINT_LOCAL_MATCH", flush=True)
        return
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=CACHE.parent) as temp:
        staged = Path(temp) / "checkpoint"
        staged.mkdir()
        for path, sha in parts.items():
            target = staged / path
            target.parent.mkdir(parents=True, exist_ok=True)
            source = remote_path(STATE + path, rev)
            if digest(source) != sha:
                raise ValueError("Durable compact checkpoint download SHA mismatch")
            shutil.copyfile(source, target)
        (staged / "manifest.json").write_bytes(raw)
        shutil.rmtree(CACHE, ignore_errors=True)
        staged.rename(CACHE)
    print("FOODRADAR_DURABLE_CHECKPOINT_RESTORED=" + meta["batch_id"], flush=True)


def report(row):
    print("FOODRADAR_HF_RETENTION=" + json.dumps(row, ensure_ascii=False), flush=True)
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write("\n### HF 原始資料與歷史版本清理\n")
            for key, val in row.items():
                out.write(f"- {key}: {val}\n")


def cleanup(api, dry=False, now=None):
    from huggingface_hub import CommitOperationAdd, CommitOperationDelete
    from pipeline import raw_manifest_should_replay
    config()
    rev, files = snapshot(api)
    now = now or datetime.now(TW)
    expired = expired_batches(files, now)
    targets = {p for p in files if batch_of(p) in expired}
    commits_before = len(api.list_repo_commits(REPO, repo_type="dataset", revision="main"))
    result = {"保留天數": RETENTION_DAYS, "到期批次": len(expired), "到期檔案": len(targets),
              "清理前歷史提交": commits_before, "模式": "僅盤點" if dry else "正式"}
    refs = api.list_repo_refs(REPO, repo_type="dataset", include_pull_requests=True)
    extra = [r.ref for r in refs.branches if r.name != "main"] + [r.ref for r in refs.tags]
    extra += [r.ref for r in (getattr(refs, "pull_requests", None) or [])]
    # Other explicit refs are not silently destroyed. They can retain old LFS data.
    if extra:
        result["阻擋清理的其他引用"] = ", ".join(extra)
        report(result)
        raise ValueError("Unexpected HF branches/tags/PR refs retain history")
    if dry:
        report(result)
        return
    if os.getenv("GITHUB_REPOSITORY"):
        sys.path.insert(0, str(ROOT / "scripts"))
        from cleanup_artifacts import active_crawl, active_deploy
        if active_crawl() or active_deploy():
            result["狀態"] = "有執行中的 01/02，保留資料，交由下次 03 清理"
            report(result)
            return
    if targets:
        if STATE + "manifest.json" not in files:
            raise ValueError("No verified durable checkpoint; refusing destructive Raw expiry")
        records = all_records(files, rev)
        meta = json.loads(read(STATE + "manifest.json", rev))
        if meta.get("retention_days") != RETENTION_DAYS or not meta.get("retention_cutoff"):
            raise ValueError("No verified 60-day checkpoint; refusing Raw expiry")
        count = check_prefix(meta, records)
        verify_remote_checkpoint(api, meta, files, rev)
        entries = []
        for index, (path, row, sha) in enumerate(records):
            if row.get("_frt_retired") or batch_of(path) in expired:
                if index >= count:
                    raise ValueError("Expired Raw has not been included in durable checkpoint")
                replay = raw_manifest_should_replay(row, path, FOLDER)
                entries.append({"path": path, "manifest_sha256": sha, "replay": replay,
                                "archive_sha256": row.get("archive_sha256", "")})
        # A malformed/missing manifest is not silently destroyed.
        if expired - {batch_of(row["path"]) for row in entries}:
            raise ValueError("Expired Raw batch lacks a verified manifest receipt")
        # Keep existing pointers only if their referenced archive still exists.
        for pointer in ("Raw/latest.json", "Raw/latest-partial.json"):
            if pointer in files:
                value = json.loads(read(pointer, rev))
                if value.get("batch_id") in expired:
                    targets.add(pointer)
        ledger = {"format": "frt-retired-raw-v1", "repo": REPO, "entries": entries}
        ops = [CommitOperationDelete(path_in_repo=p) for p in sorted(targets)]
        ops.append(CommitOperationAdd(path_in_repo=LEDGER, path_or_fileobj=pack(ledger)))
        api.create_commit(REPO, repo_type="dataset", revision="main", parent_commit=rev,
                          operations=ops, commit_message="Expire project Raw older than 60 days")
        rev, after = snapshot(api)
        if targets & after or expired_batches(after, now):
            raise ValueError("Expired Raw remains after delete commit")
        if read(LEDGER, rev) != pack(ledger):
            raise ValueError("Retired Raw receipt verification failed")
        files = after
    if len(api.list_repo_commits(REPO, repo_type="dataset", revision="main")) > 1:
        if api.repo_info(REPO, repo_type="dataset", revision="main").sha != rev:
            raise ValueError("HF main changed concurrently; refuse history rewrite")
        api.super_squash_history(REPO, branch="main", repo_type="dataset",
                                 commit_message="FRT rolling 60-day Raw and current compact state")
    final_rev, final_files = snapshot(api)
    after_commits = len(api.list_repo_commits(REPO, repo_type="dataset", revision="main"))
    if files != final_files or after_commits != 1:
        raise ValueError("HF squash did not preserve current files or reduce history to one commit")
    result.update({"已刪除檔案": len(targets), "清理後歷史提交": after_commits,
                   "剩餘 Raw 批次": len({batch_of(p) for p in final_files if batch_of(p)}),
                   "HF 儲存空間": "後台回收非即時，需等待 HF 更新"})
    report(result)


if __name__ == "__main__":
    import argparse
    from huggingface_hub import HfApi
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["restore", "persist", "cleanup"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()
    api = HfApi(token=os.environ["HF_TOKEN"])
    if args.mode == "restore":
        restore(api, rebuild=args.rebuild)
    elif args.mode == "persist":
        persist(api)
    else:
        cleanup(api, dry=args.dry_run)
