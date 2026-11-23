"""Publish verified, content-addressed FoodRadar static blobs to HF Dataset.

Only move current.json after all referenced blobs have been uploaded. Rollback
is possible by republishing a previous verified manifest; no Turso calls.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

from huggingface_hub import HfApi, CommitOperationAdd
from build_static_catalog import verify
from prepare_pages import _paged_manifest, referenced_blobs

PREFIX = "StaticCatalog"
MAX_PER_COMMIT = 100


def publish(staging, repo_id, token):
    staging = Path(staging)
    if not token:
        raise ValueError("HF_TOKEN is required")
    pointer=staging/"current.json"
    manifest=json.loads(pointer.read_text(encoding="utf-8"))
    if manifest.get("schema") == "foodradar-static-v1":
        verify(staging)
        manifest=_paged_manifest(manifest,staging)
        pointer.write_text(json.dumps(manifest,ensure_ascii=False,separators=(",",":")),encoding="utf-8")
    elif manifest.get("schema") != "foodradar-static-v2":
        raise ValueError("Unsupported FoodRadar static source")
    # Hugging Face rejects directories with more than 10,000 files.
    # A content-hash fanout keeps current and future nationwide releases under that limit.
    manifest["hf_blob_layout"]="sha256-prefix-2"
    pointer.write_text(json.dumps(manifest,ensure_ascii=False,separators=(",",":")),encoding="utf-8")
    refs=referenced_blobs(manifest)
    if not refs:
        raise ValueError("Refusing to publish empty static catalog")
    total=0
    for h in refs:
        path=staging/"blobs"/(h+".zst")
        contents=path.read_bytes()
        if hashlib.sha256(contents).hexdigest()!=h:
            raise ValueError("Refusing corrupt V2 blob: "+h)
        total+=len(contents)
    if total!=manifest["bytes"]:
        raise ValueError("V2 static manifest byte mismatch")
    api = HfApi(token=token)
    existing = set(api.list_repo_files(repo_id=repo_id, repo_type="dataset"))
    files = [staging/"blobs"/(h+".zst") for h in sorted(refs)]
    def path_in_repo(filename):
        digest=filename.split(".",1)[0]
        return f"{PREFIX}/v2/blobs/{digest[:2]}/{filename}"
    pending = [p for p in files if path_in_repo(p.name) not in existing]
    uploaded = 0
    for start in range(0, len(pending), MAX_PER_COMMIT):
        batch = pending[start:start + MAX_PER_COMMIT]
        api.create_commit(
            repo_id=repo_id, repo_type="dataset",
            commit_message=f"FoodRadar immutable static assets {manifest['batch']} ({start//MAX_PER_COMMIT+1})",
            operations=[CommitOperationAdd(
                path_in_repo=path_in_repo(p.name),
                path_or_fileobj=str(p)) for p in batch],
        )
        uploaded += len(batch)
    # HEAD verify the repo references exist before switching the public pointer.
    remote = set(api.list_repo_files(repo_id=repo_id, repo_type="dataset"))
    expected = {path_in_repo(p.name) for p in files}
    if not expected.issubset(remote):
        raise ValueError("Incomplete HF blob upload: refusing to switch current.json")
    api.upload_file(
        path_or_fileobj=str(staging / "current.json"),
        path_in_repo=f"{PREFIX}/current.json",
        repo_id=repo_id, repo_type="dataset",
        commit_message=f"FoodRadar static current pointer {manifest['batch']}",
    )
    result = {"batch": manifest["batch"], "source_count": len(files),
              "new_blobs": uploaded, "reused_blobs": len(files) - uploaded,
              "data_revision": manifest["data_revision"]}
    print("FOODRADAR_HF_STATIC_PUBLISHED=" + json.dumps(result), flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("staging")
    parser.add_argument("--repo", default="hub-google/FoodRadar")
    args = parser.parse_args()
    publish(args.staging, args.repo, os.environ.get("HF_TOKEN"))
