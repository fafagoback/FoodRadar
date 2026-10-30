"""One-time lossless migration to an independent Hugging Face dataset.

Source: hub-google/UberEat / FoodRadar/Raw/
Target: hub-google/FoodRadar / Raw/

Original capture.tar.gz bytes are copied without decoding or repacking.
The capture manifest only changes its archive_path to the new repository-relative
path. Neither source snapshots nor any historical/transformed files are deleted.
Reruns verify existing targets instead of overwriting them.
"""
from __future__ import annotations

import hashlib
import json
import os
import tarfile
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download

SOURCE_REPO = "hub-google/UberEat"
DESTINATION_REPO = "hub-google/FoodRadar"
SOURCE_PREFIX = "FoodRadar/Raw/"
DESTINATION_PREFIX = "Raw/"


def checked_download(repo_id, path, token):
    return Path(hf_hub_download(
        repo_id=repo_id, filename=path, repo_type="dataset", token=token,
    ))


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_capture(archive_path, manifest):
    if sha256(archive_path) != manifest["archive_sha256"]:
        raise ValueError(f"Raw archive SHA256 mismatch: {archive_path}")
    with tarfile.open(archive_path, "r:gz") as tar:
        files = {item.name: item for item in tar.getmembers() if item.isfile()}
        expected = manifest["responses"]
        if files.keys() != expected.keys():
            raise ValueError("Original response file list differs from the manifest")
        for name, digest in expected.items():
            if hashlib.sha256(tar.extractfile(files[name]).read()).hexdigest() != digest:
                raise ValueError(f"Raw response body checksum mismatch: {name}")


def migrate(token):
    api = HfApi(token=token)
    source_files = set(api.list_repo_files(
        repo_id=SOURCE_REPO, repo_type="dataset"))
    sources = sorted(
        p for p in source_files if p.startswith(SOURCE_PREFIX)
        and p.endswith("/manifest.json"))
    if not sources:
        raise RuntimeError("No source raw manifests in hub-google/UberEat/FoodRadar/Raw")
    # Independent dataset, rather than a folder beneath the legacy UberEat repo.
    api.create_repo(
        repo_id=DESTINATION_REPO, repo_type="dataset",
        private=False, exist_ok=True,
    )
    dest_files = set(api.list_repo_files(
        repo_id=DESTINATION_REPO, repo_type="dataset"))
    completed = []
    for source_manifest_path in sources:
        source_bytes = checked_download(SOURCE_REPO, source_manifest_path, token).read_bytes()
        manifest = json.loads(source_bytes)
        batch = manifest["batch_id"]
        if (manifest.get("format") != "foodradar-immutable-http-body-v1"
                or not isinstance(batch,str) or len(batch)!=14 or not batch.isdigit()
                or manifest.get("archive_path") != f"{SOURCE_PREFIX}{batch}/capture.tar.gz"):
            raise ValueError("Unexpected legacy Raw manifest: " + source_manifest_path)
        source_archive = checked_download(SOURCE_REPO, manifest["archive_path"], token)
        verify_capture(source_archive, manifest)
        migrated = dict(manifest)
        migrated["archive_path"] = f"{DESTINATION_PREFIX}{batch}/capture.tar.gz"
        target_manifest_path = f"{DESTINATION_PREFIX}{batch}/manifest.json"
        target_archive_path = migrated["archive_path"]
        target_manifest_bytes = json.dumps(
            migrated, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        existing_manifest = target_manifest_path in dest_files
        existing_archive = target_archive_path in dest_files
        if existing_manifest != existing_archive:
            raise RuntimeError(f"Partially migrated batch {batch}: refuse overwrite")
        if existing_manifest:
            present = json.loads(checked_download(
                DESTINATION_REPO,target_manifest_path,token).read_bytes())
            if present != migrated:
                raise RuntimeError(f"Destination manifest differs: {batch}")
            verify_capture(checked_download(
                DESTINATION_REPO,target_archive_path,token), migrated)
        else:
            api.create_commit(
                repo_id=DESTINATION_REPO,repo_type="dataset",
                operations=[
                    CommitOperationAdd(
                        path_in_repo=target_archive_path,
                        path_or_fileobj=str(source_archive)),
                    CommitOperationAdd(
                        path_in_repo=target_manifest_path,
                        path_or_fileobj=target_manifest_bytes),
                ],
                commit_message=f"Import immutable FoodRadar Raw HTTP capture {batch}",
            )
            verify_capture(checked_download(
                DESTINATION_REPO,target_archive_path,token), migrated)
            dest_files.update((target_archive_path,target_manifest_path))
        if migrated.get("complete"):
            completed.append(migrated)
        print("RAW_MIGRATION_BATCH="+json.dumps({
            "batch":batch, "responses":len(manifest["responses"]),
            "archive_sha256":manifest["archive_sha256"],
            "destination":target_archive_path,
        },ensure_ascii=False),flush=True)
    if not completed:
        raise RuntimeError("No complete Raw batches to activate")
    latest = max(completed,key=lambda x:x["batch_id"])
    # Mark latest only after every archive was independently verified.
    latest_payload = json.dumps(
        latest,ensure_ascii=False,separators=(",", ":")).encode("utf-8")
    if "Raw/latest.json" in dest_files:
        existing = json.loads(checked_download(
            DESTINATION_REPO,"Raw/latest.json",token).read_bytes())
        if existing["batch_id"] > latest["batch_id"]:
            raise RuntimeError("Destination has a newer capture; refuse pointer regression")
    if "README.md" not in dest_files:
        description = """---
language:
- zh
license: other
pretty_name: FoodRadar 原始 API 回應封存
---

# FoodRadar — 原始爬蟲資料

本 Dataset 與舊的 `hub-google/UberEat` Repository **相互獨立**。
`Raw/<YYYYMMDDHHMMSS>/capture.tar.gz` 以無損壓縮保存
Uber Eats API 實際收到的原始 HTTP 回應內容；不更改價格、
商品、促銷或任何 JSON 欄位。

`manifest.json` 是獨立的採集索引及 SHA-256 校驗紀錄。
所有正規化與價差計算皆由 GitHub FoodRadar 轉入 Turso 時進行。
"""
        api.create_commit(
            repo_id=DESTINATION_REPO,repo_type="dataset",
            operations=[CommitOperationAdd(
                path_in_repo="README.md",path_or_fileobj=description.encode("utf-8"))],
            commit_message="Document standalone FoodRadar raw dataset",
        )
    if ("Raw/latest.json" not in dest_files or json.loads(
            checked_download(DESTINATION_REPO,"Raw/latest.json",token).read_bytes())!=latest):
        api.create_commit(
            repo_id=DESTINATION_REPO,repo_type="dataset",
            operations=[CommitOperationAdd(
                path_in_repo="Raw/latest.json",path_or_fileobj=latest_payload)],
            commit_message=f"Point FoodRadar Raw to verified capture {latest['batch_id']}",
        )
    print("FOODRADAR_STANDALONE_DATASET_READY="+json.dumps({
        "repo_id":DESTINATION_REPO,
        "batches":len(sources),
        "complete_batches":len(completed),
        "latest_batch":latest["batch_id"],
        "source_legacy_read_only":SOURCE_REPO,
        "root":"Raw/",
    },ensure_ascii=False),flush=True)


if __name__ == "__main__":
    if not os.environ.get("HF_TOKEN"):
        raise SystemExit("HF_TOKEN GitHub Action secret is required")
    migrate(os.environ["HF_TOKEN"])
