"""Immutable original Uber Eats HTTP JSON response archive and HF -> DB adapter.

The Raw archive holds response.body bytes unchanged. All decoding, merging,
promotion interpretation, and Schema.org conversion happens only after reading
the archived bytes back from Hugging Face.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from menu_core import attach_delivery_location, normalize_store_uuid

TW = timezone(timedelta(hours=8))


def capture_response(directory: Path, filename: str, response) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    if path.exists():
        raise FileExistsError(f"Refusing to replace captured response: {path}")
    path.write_bytes(response.content)
    return path.as_posix()


def fetch_store_raw(store: dict, root: Path, max_retries: int = 3) -> dict:
    """Capture native getStoreV1 response bytes; no menu/schema conversion."""
    uuid = normalize_store_uuid(store["store_uuid"], store["store_url"])
    if not re.fullmatch(r"[0-9a-f-]{36}", uuid):
        return {"store_uuid": uuid, "error": "Invalid store UUID"}
    session = requests.Session()
    if not attach_delivery_location(session, store):
        session.close()
        return {"store_uuid": uuid, "error": "Missing feed delivery coordinate"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
        "x-csrf-token": "x",
        "Content-Type": "application/json",
    }
    result = {"store_uuid": uuid, "store_url": store["store_url"], "success": False}
    try:
        for attempt in range(1, max_retries + 1):
            try:
                response = session.post(
                    "https://www.ubereats.com/api/getStoreV1",
                    json={"storeUuid": uuid, "sfNuggetCount": 24},
                    headers=headers, timeout=12,
                )
                relative = f"stores/{uuid}-attempt-{attempt:02d}-status-{response.status_code}.body"
                capture_response(root, relative, response)
                # This parsing only controls retry. Saved response bytes are unchanged.
                if response.status_code == 200:
                    payload = response.json()
                    data = payload.get("data") if isinstance(payload, dict) else None
                    if isinstance(data, dict) and (data.get("title") or data.get("catalogSectionsMap")):
                        result.update(success=True, response_path=relative,
                                      captured_at=datetime.now(TW).isoformat())
                        return result
                result["error"] = f"HTTP {response.status_code} or incomplete response"
            except (requests.RequestException, ValueError, TypeError) as exc:
                result["error"] = str(exc)
            if attempt < max_retries:
                time.sleep(attempt)
    finally:
        session.close()
    return result


def archive_and_upload(root: Path, config: dict, batch: str,
                       stores: list[dict], pages: int, complete: bool) -> dict:
    """Commit unmodified response bodies. Manifest is separate capture metadata."""
    from huggingface_hub import CommitOperationAdd, HfApi

    folder = config["hf_folder"].strip("/")
    if folder != "FoodRadar":
        raise ValueError("Raw captures must be stored below FoodRadar/Raw")
    prefix = f"{folder}/Raw/{batch}"
    raw_files = sorted(p for p in root.rglob("*.body") if p.is_file())
    if not raw_files:
        raise RuntimeError("No actual response bodies to archive")
    names = [p.relative_to(root).as_posix() for p in raw_files]
    record = {
        "format": "foodradar-immutable-http-body-v1",
        "batch_id": batch,
        "complete": bool(complete),
        "feed_pages": pages,
        "stores": stores,
        "responses": {
            name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in names
        },
        "captured_count": len(names),
    }
    archive_path = root.parent / "raw-capture.tar.gz"
    # Only lossless compression; original body bytes stay identical inside TAR.
    with tarfile.open(archive_path, "w:gz") as archive:
        for path in raw_files:
            archive.add(path, arcname=path.relative_to(root).as_posix())
    record["archive_sha256"] = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    record["archive_path"] = f"{prefix}/capture.tar.gz"
    payload = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()
    operations = [
        CommitOperationAdd(path_in_repo=record["archive_path"],
                           path_or_fileobj=str(archive_path)),
        CommitOperationAdd(path_in_repo=f"{prefix}/manifest.json",
                           path_or_fileobj=payload),
    ]
    if complete:
        operations.append(CommitOperationAdd(
            path_in_repo=f"{folder}/Raw/latest.json", path_or_fileobj=payload))
    api = HfApi(token=os.environ["HF_TOKEN"])
    api.create_commit(
        repo_id=config["hf_repo_id"], repo_type="dataset", operations=operations,
        commit_message=f"FoodRadar immutable Raw API capture {batch}",
    )
    # Immediately download from the HF folder and verify both archive and members.
    from huggingface_hub import hf_hub_download
    downloaded = Path(hf_hub_download(
        config["hf_repo_id"], record["archive_path"], repo_type="dataset",
        token=os.environ["HF_TOKEN"],
    ))
    if hashlib.sha256(downloaded.read_bytes()).hexdigest() != record["archive_sha256"]:
        raise ValueError("Uploaded HF Raw archive differs from local capture")
    with tarfile.open(downloaded, "r:gz") as archive:
        members = {m.name: m for m in archive.getmembers() if m.isfile()}
        if set(members) != set(record["responses"]):
            raise ValueError("Uploaded HF Raw body member set mismatch")
        for name, expected in record["responses"].items():
            if hashlib.sha256(archive.extractfile(members[name]).read()).hexdigest() != expected:
                raise ValueError(f"Uploaded HF response body changed: {name}")
    print("HF_IMMUTABLE_RAW_UPLOADED=" + json.dumps({
        "batch": batch, "complete": bool(complete), "responses": len(names),
        "archive": record["archive_path"],
    }, ensure_ascii=False), flush=True)
    return record


def documents_from_hf_raw(archive, manifest: dict) -> list[dict]:
    """ETL adapter: only for DB construction; never writes to HF Raw."""
    from menu_core import convert_api_data_to_schema
    from snapshot_validation import validate_document

    members = {m.name: m for m in archive.getmembers() if m.isfile()}
    expected = manifest["responses"]
    if set(members) != set(expected):
        raise ValueError("HF Raw member set changed")
    for name, digest in expected.items():
        if hashlib.sha256(archive.extractfile(members[name]).read()).hexdigest() != digest:
            raise ValueError("HF Raw member checksum mismatch")
    if not manifest.get("complete"):
        raise ValueError("Incomplete original capture cannot be released to DB")
    batch = manifest["batch_id"]
    documents = []
    for store in manifest["stores"]:
        if not store.get("success"):
            raise ValueError("Cannot publish an incomplete store capture")
        raw = store["response_path"]
        envelope = json.loads(archive.extractfile(members[raw]).read())
        native = envelope.get("data") if isinstance(envelope, dict) else None
        if not isinstance(native, dict):
            raise ValueError(f"Missing native menu: {raw}")
        doc = convert_api_data_to_schema(native, store["store_url"])
        doc.update(
            store_uuid=store["store_uuid"],
            store_id=hashlib.md5(store["store_url"].encode()).hexdigest(),
            captured_at=store["captured_at"],
            batch_id=batch,
        )
        validate_document(doc)
        documents.append(doc)
    if len(documents) != len(manifest["stores"]):
        raise ValueError("HF Raw decoded store count mismatch")
    return documents
