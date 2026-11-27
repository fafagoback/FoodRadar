"""Immutable original Uber Eats HTTP JSON response archive and HF -> DB adapter.

The Raw archive holds response.body bytes unchanged. All decoding, merging,
promotion interpretation, and Schema.org conversion happens only after reading
the archived bytes back from Hugging Face.
"""
from __future__ import annotations

import hashlib
import gzip
import shutil
import tempfile
from contextlib import contextmanager
import io
import json
import os
import re
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from menu_core import attach_delivery_location, normalize_store_uuid, global_limiter, response_retry_after
from promo_api_recheck import QUARANTINE_STATUSES
from catalog_complete import (
    capture_catalog, merge_catalog_pages,
    EmptyCatalogPagesError,
)

TW = timezone(timedelta(hours=8))


class CheckpointStorageError(RuntimeError):
    """Durable capture failed; recover the task instead of accepting a failed store."""



from raw_io import seekable_archive, digest_file, disk_report


@contextmanager
def open_seekable_raw_archive(path):
    """Seek within the compressed archive without expanding it to disk."""
    with seekable_archive(path) as archive:
        yield archive


def capture_response(directory: Path, filename: str, response) -> str:
    path = directory / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"Refusing to replace captured response: {path}")
    path.write_bytes(response.content)
    return path.as_posix()


def fetch_store_raw(store: dict, root: Path, max_retries: int = 5, session_factory=None,
                    catalog_runner=None, catalog_options=None) -> dict:
    """Capture immutable HTTP bodies; retry store metadata only before paging.

    capture_catalog owns the per-page retry budget, including 429 cooldown.
    Once catalog capture starts, never restart the entire store on an
    exhausted page or structural partition error. Keep the failed capture
    for audit and let the existing release gate decide whether to publish.
    """
    max_retries = max(max_retries, min(12, int(os.getenv("FOODRADAR_STORE_RETRIES", str(max_retries)))))
    uuid = normalize_store_uuid(store["store_uuid"], store["store_url"])
    if not re.fullmatch(r"[0-9a-f-]{36}", uuid):
        return {"store_uuid": uuid, "error": "Invalid store UUID"}
    session = (session_factory or requests.Session)()
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
            catalog_started = False
            try:
                global_limiter.wait()
                response = session.post(
                    "https://www.ubereats.com/api/getStoreV1",
                    json={"storeUuid": uuid, "sfNuggetCount": 24},
                    headers=headers, timeout=12,
                )
                relative = f"stores/{uuid}-attempt-{attempt:02d}-status-{response.status_code}.body"
                capture_response(root, relative, response)
                if response.status_code == 429:
                    global_limiter.trigger_backoff(
                        min(120, 15 * (2 ** (attempt - 1))), "Store HTTP 429",
                        retry_after=response_retry_after(response))
                    result["error"] = "Store HTTP 429 (too many requests)"
                    continue
                # Terminal inactive responses are complete captures too.
                # Only the DB adapter turns them into an inactive-store record.
                if response.status_code in (404, 410):
                    result.update(success=True, inactive=True, response_path=relative,
                                  captured_at=datetime.now(TW).isoformat())
                    return result
                # This parsing only controls retry. Saved response bytes are unchanged.
                if response.status_code == 200:
                    payload = response.json()
                    data = payload.get("data") if isinstance(payload, dict) else None
                    code = str(data.get("code") or "") if isinstance(data, dict) else ""
                    message = str(data.get("message") or "").lower() if isinstance(data, dict) else ""
                    if payload.get("status") == "failure" and code == "429":
                        global_limiter.trigger_backoff(
                            min(120, 15 * (2 ** (attempt - 1))),
                            "Store application 429", retry_after=response_retry_after(response))
                        result["error"] = "Store application code 429 (too many requests)"
                        continue
                    if (payload.get("status") == "failure" and
                            (code in ("404", "410") or "inactive_account" in message)):
                        result.update(success=True, inactive=True, response_path=relative,
                                      captured_at=datetime.now(TW).isoformat())
                        return result
                    if isinstance(data, dict) and (data.get("title") or data.get("catalogSectionsMap")):
                        global_limiter.record_success()
                        # getStoreV1 is only a preview for large grocery stores.
                        # Save every native catalog page before accepting a complete capture.
                        catalog_started = True
                        refresh_number = 0
                        def refresh_native_metadata():
                            nonlocal refresh_number
                            refresh_number += 1
                            global_limiter.wait()
                            renewed = session.post(
                                "https://www.ubereats.com/api/getStoreV1",
                                json={"storeUuid": uuid, "sfNuggetCount": 24},
                                headers=headers, timeout=12,
                            )
                            extra = f"stores/{uuid}-attempt-{attempt:02d}-metadata-refresh-{refresh_number:02d}-status-{renewed.status_code}.body"
                            capture_response(root, extra, renewed)
                            if renewed.status_code == 429:
                                global_limiter.trigger_backoff(30, "Store metadata refresh HTTP 429",
                                    retry_after=response_retry_after(renewed))
                                raise RuntimeError("Native metadata refresh HTTP 429")
                            if renewed.status_code != 200:
                                raise RuntimeError(f"Native metadata refresh HTTP {renewed.status_code}")
                            document = renewed.json()
                            current = document.get("data") if isinstance(document, dict) else None
                            if (isinstance(document, dict) and document.get("status") == "failure"
                                    and isinstance(current, dict) and str(current.get("code")) == "429"):
                                global_limiter.trigger_backoff(30, "Store metadata refresh application 429")
                                raise RuntimeError("Native metadata refresh application 429")
                            if (not isinstance(current, dict) or document.get("status") == "failure"
                                    or not (current.get("title") or current.get("catalogSectionsMap"))):
                                raise RuntimeError("Invalid native metadata refresh")
                            if current.get("storeUuid") not in (None, uuid):
                                raise RuntimeError("Native metadata refresh store identity mismatch")
                            global_limiter.record_success()
                            return current

                        options = {"refresh_native_metadata": refresh_native_metadata}
                        if os.getenv("FOODRADAR_PARALLEL_SECTIONS") == "1":
                            def section_session():
                                independent = requests.Session()
                                independent.cookies.update(session.cookies)
                                return independent
                            # Store slots learn independently. Keep nested sections bounded.
                            section_threads = int(os.getenv("FOODRADAR_SECTION_THREADS", "1"))
                            if section_threads < 1:
                                raise ValueError("FOODRADAR_SECTION_THREADS must be positive")
                            if section_threads > 1:
                                options.update(session_factory=section_session, section_workers=section_threads)
                        all_catalog = None
                        if catalog_runner is not None:
                            all_catalog = catalog_runner(store, root, relative, data, headers, attempt)
                        if all_catalog is None:
                            options.update(catalog_options or {})
                            all_catalog = capture_catalog(
                                session, uuid, data, root, headers, capture_response,
                                store_attempt=attempt, **options)
                        result.update(success=True, response_path=relative,
                                      captured_at=datetime.now(TW).isoformat(), **all_catalog)
                        print("CATALOG_CAPTURE=" + json.dumps({
                            "store_uuid": uuid,
                            "pages": len(all_catalog["catalog_responses"]),
                            "sections": all_catalog.get("catalog_section_count", 0),
                            "occurrences": all_catalog["catalog_item_occurrences"]}), flush=True)
                        return result
                result["error"] = f"HTTP {response.status_code} or incomplete response"
            except CheckpointStorageError:
                raise
            except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
                # Preserve the deepest native HTTP/parsing failure in the report,
                # including exceptions wrapped by catalog pagination.
                chain, cause = [], exc
                while cause is not None and len(chain) < 4:
                    chain.append(f"{type(cause).__name__}: {cause}")
                    cause = cause.__cause__
                result["error"] = " <- ".join(chain)
                if catalog_started:
                    # A page already exhausted its own retry budget, or the
                    # partition is structurally incomplete. Do not re-download
                    # earlier successful pages or bless the partial menu.
                    print("FOODRADAR_CATALOG_RETRY_STOP=" + json.dumps({
                        "store_uuid": uuid, "store_attempt": attempt,
                        "error": result["error"],
                    }, ensure_ascii=False), flush=True)
                    return result
            if attempt < max_retries:
                time.sleep(attempt)
    finally:
        session.close()
    return result


def archive_and_upload(root: Path, config: dict, batch: str,
                       stores: list[dict], pages: int, complete: bool,
                       partial_replayable: bool = False, *,
                       prepared_archive=None, verified_responses=None) -> dict:
    """Commit unmodified response bodies. Manifest is separate capture metadata."""
    from huggingface_hub import CommitOperationAdd, HfApi

    successful = [s for s in stores if s.get("success") is True]
    failed = [s for s in stores if s.get("success") is not True]
    if partial_replayable:
        if complete or not successful or not failed or not pages:
            raise ValueError("Invalid partially verified Raw snapshot")
        if len({s.get("store_uuid") for s in stores}) != len(stores):
            raise ValueError("Duplicate Raw store UUID in partial capture")
        from release_policy import require_failure_budget
        require_failure_budget(len(failed), len(stores))
    folder = config["hf_folder"].strip("/")
    if config["hf_repo_id"] != "hub-google/FoodRadar" or folder != "Raw":
        raise ValueError("Raw must go to independent hub-google/FoodRadar dataset /Raw")
    prefix = f"{folder}/{batch}"
    if (prepared_archive is None) != (verified_responses is None):
        raise ValueError("Prepared Raw must have verified responses")
    if prepared_archive is None:
        raw_files = sorted(p for p in root.rglob("*.body") if p.is_file())
        names = [p.relative_to(root).as_posix() for p in raw_files]
        responses = {name: digest_file(root / name) for name in names}
    else:
        responses = verified_responses
        names = list(responses)
    if not names:
        raise RuntimeError("No actual response bodies to archive")
    record = {
        "format": "foodradar-immutable-http-body-v1",
        "batch_id": batch,
        "complete": bool(complete),
        "partial_replayable": bool(partial_replayable),
        "successful_store_count": len(successful),
        "failed_store_count": len(failed),
        "failed_store_uuids": [s["store_uuid"] for s in failed],
        "feed_pages": pages,
        "stores": stores,
        "responses": responses,
        "captured_count": len(names),
    }
    if prepared_archive is None:
        archive_path = root.parent / "raw-capture.tar.gz"
        # Only lossless compression; original body bytes stay identical inside TAR.
        with tarfile.open(archive_path, "w:gz") as archive:
            for path in raw_files:
                archive.add(path, arcname=path.relative_to(root).as_posix())
    else:
        archive_path = Path(prepared_archive)
    record["archive_sha256"] = digest_file(archive_path)
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
            path_in_repo=f"{folder}/latest.json", path_or_fileobj=payload))
    elif partial_replayable:
        # Keep the last *complete* pointer intact. A separate partial pointer
        # advertises exactly which stores were not freshly verified.
        operations.append(CommitOperationAdd(
            path_in_repo=f"{folder}/latest-partial.json", path_or_fileobj=payload))
    api = HfApi(token=os.environ["HF_TOKEN"])
    if prepared_archive is not None and api.file_exists(
            repo_id=config["hf_repo_id"], filename=f"{prefix}/manifest.json", repo_type="dataset"):
        raise FileExistsError("Refusing to overwrite immutable Raw batch " + batch)
    api.create_commit(
        repo_id=config["hf_repo_id"], repo_type="dataset", operations=operations,
        commit_message=f"FoodRadar immutable Raw API capture {batch}",
    )
    # The streaming writer already checked every original member. The remote
    # content SHA256 proves byte-identical publication without a second download.
    remote_sha = None
    if prepared_archive is not None:
        entries = api.get_paths_info(config["hf_repo_id"], [record["archive_path"]],
                                     repo_type="dataset", expand=True)
        if len(entries) == 1:
            lfs = getattr(entries[0], "lfs", None)
            remote_sha = lfs.get("sha256") if isinstance(lfs, dict) else getattr(lfs, "sha256", None)
            if remote_sha and getattr(entries[0], "size", None) != archive_path.stat().st_size:
                raise ValueError("Uploaded HF Raw archive size mismatch")
    if remote_sha:
        if remote_sha != record["archive_sha256"]:
            raise ValueError("Uploaded HF Raw archive SHA256 mismatch")
        downloaded = archive_path
    else:
        from huggingface_hub import hf_hub_download
        if prepared_archive is not None:
            # Fallback when HF has no LFS SHA256 metadata: do not retain two
            # complete compressed copies while downloading the remote object.
            archive_path.unlink()
        downloaded = Path(hf_hub_download(
            config["hf_repo_id"], record["archive_path"], repo_type="dataset",
            token=os.environ["HF_TOKEN"],
        ))
        if digest_file(downloaded) != record["archive_sha256"]:
            raise ValueError("Uploaded HF Raw archive differs from local capture")
    if prepared_archive is None:
        with open_seekable_raw_archive(downloaded) as archive:
            members = {m.name: m for m in archive.getmembers() if m.isfile()}
            if set(members) != set(record["responses"]):
                raise ValueError("Uploaded HF Raw body member set mismatch")
            for name, expected in record["responses"].items():
                if hashlib.sha256(archive.extractfile(members[name]).read()).hexdigest() != expected:
                    raise ValueError(f"Uploaded HF response body changed: {name}")
    else:
        stage = Path(prepared_archive).parent
        (stage / "verified-local.json").write_text(json.dumps({
            "repo_id": config["hf_repo_id"], "archive_path": record["archive_path"],
            "sha256": record["archive_sha256"], "local_path": str(downloaded.resolve())}),
            encoding="utf-8")
        disk_report("hf_verified", stage, archive_bytes=downloaded.stat().st_size,
                    remote_sha256_metadata=bool(remote_sha))
    print("HF_IMMUTABLE_RAW_UPLOADED=" + json.dumps({
        "batch": batch, "complete": bool(complete),
        "partial_replayable": bool(partial_replayable),
        "verified_stores": len(successful), "failed_stores": len(failed),
        "responses": len(names),
        "archive": record["archive_path"],
    }, ensure_ascii=False), flush=True)
    return record


def _decode_verified_store(store, canonical_id, batch, body, pages):
    """CPU-only native decode. All input bytes were checked by the parent."""
    from menu_core import convert_api_data_to_schema, create_inactive_store_schema
    from snapshot_validation import validate_document
    if store.get("inactive"):
        doc=create_inactive_store_schema(
            {"name":"","city":""},store["store_url"],store["store_uuid"],
            hashlib.md5(store["store_url"].encode()).hexdigest(),batch+"_")
    else:
        envelope=json.loads(body)
        native=envelope.get("data") if isinstance(envelope,dict) else None
        if not isinstance(native,dict):
            raise ValueError(f"Missing native menu: {store['response_path']}")
        if pages:
            try:native=merge_catalog_pages(native,[json.loads(page) for page in pages])
            except EmptyCatalogPagesError:return None
        doc=convert_api_data_to_schema(native,store["store_url"])
    doc.update(store_uuid=canonical_id,store_id=hashlib.md5(store["store_url"].encode()).hexdigest(),
               captured_at=store["captured_at"],batch_id=batch)
    validate_document(doc)
    return doc


def documents_from_hf_raw(archive, manifest: dict, *,
                          quarantined_store_uuids: list[str] | None = None,
                          document_spool=None):
    """Convert verified HF Raw to DB documents without mutating the archive.

    Explicitly report empty-menu stores to the caller for safe exclusion.
    """
    from menu_core import normalize_store_uuid, canonical_store_uuid
    from concurrent.futures import ProcessPoolExecutor
    from collections import deque
    from contextlib import nullcontext
    import multiprocessing

    members = {m.name: m for m in archive.getmembers() if m.isfile()}
    expected = manifest["responses"]
    if set(members) != set(expected):
        raise ValueError("HF Raw member set changed")
    from time import perf_counter
    verified=set();hash_seconds=0.0
    def read_verified(name):
        nonlocal hash_seconds
        t=perf_counter()
        body=archive.extractfile(members[name]).read()
        if name not in verified:
            if hashlib.sha256(body).hexdigest()!=expected[name]:
                raise ValueError("HF Raw member checksum mismatch")
            verified.add(name)
        hash_seconds+=perf_counter()-t
        return body
    verified_partial = manifest.get("complete") is False and (
        manifest.get("partial_replayable") is True)
    if manifest.get("complete") is not True and not verified_partial:
        raise ValueError("Unverified incomplete Raw cannot be released to DB")
    all_stores = manifest["stores"]
    if verified_partial:
        from release_policy import validate_partial_manifest
        successful, failed = validate_partial_manifest(manifest)
    else:
        successful = all_stores
    batch = manifest["batch_id"]
    documents = [] if document_spool is None else document_spool
    canonical_ids = {}
    repaired_url_id_mismatches = 0
    duplicate_canonical_stores = 0
    empty_catalog_ids = set()
    unconfirmed_promotion_ids = set()
    worker_count=max(1,min(4,int(os.environ.get("FR_RAW_DECODE_WORKERS","1"))))
    pending=deque()
    def accept(canonical_id,doc):
        nonlocal duplicate_canonical_stores
        if doc is None:
            empty_catalog_ids.add(canonical_id)
            return
        if canonical_id in canonical_ids:
            # Same storefront may be surfaced by multiple feed cards. Keep
            # exactly one complete verified capture, preferring the latest.
            duplicate_canonical_stores += 1
            index = canonical_ids[canonical_id]
            if str(doc["captured_at"]) >= str(documents[index]["captured_at"]):
                documents[index] = doc
        else:
            canonical_ids[canonical_id] = len(documents)
            documents.append(doc)

    def drain():
        sid,future=pending.popleft()
        accept(sid,future.result())
    # Spawn independent CPU workers; only the parent reads/seeks the verified TAR.
    # Limit in-flight stores so decoded Raw bodies do not scale with nationwide data.
    context=(ProcessPoolExecutor(max_workers=worker_count,mp_context=multiprocessing.get_context("spawn"))
             if worker_count>1 else nullcontext(None))
    decode_started=perf_counter()
    with context as pool:
        for store in successful:
            if not store.get("success"):
                raise ValueError("Cannot publish an incomplete store capture")
            reported_id = normalize_store_uuid(store["store_uuid"], store["store_url"])
            canonical_id = canonical_store_uuid(store["store_uuid"], store["store_url"])
            if not canonical_id:
                raise ValueError("Raw store has no stable UUID or URL identity")
            if reported_id != canonical_id:
                repaired_url_id_mismatches += 1
            raw = store["response_path"]
            if store.get("catalog_responses") and store.get("catalog_complete") is not True:
                raise ValueError("Refusing to replay incomplete catalog capture")
            recheck = store.get("promo_recheck") or {}
            if recheck:
                if recheck.get("status") not in ("MATCHED", *QUARANTINE_STATUSES):
                    raise ValueError("Invalid historical promotion API recheck status")
                if recheck.get("status") in ("MATCHED", "CONFLICT"):
                    secondary = recheck.get("second_response_path")
                    if not isinstance(secondary, str) or secondary not in members:
                        raise ValueError("Missing immutable second API capture")
                    second_pages = recheck.get("second_catalog_response_paths")
                    if second_pages is not None:
                        if (not isinstance(second_pages, list)
                                or len(second_pages) != recheck.get("second_catalog_pages")
                                or len(second_pages) != len(set(second_pages))
                                or any(not isinstance(name, str)
                                       or name not in members
                                       or not name.startswith(recheck.get("raw_prefix", "#missing#"))
                                       for name in second_pages)):
                            raise ValueError("Missing or inconsistent independent API catalog evidence")
                if recheck.get("status") in QUARANTINE_STATUSES:
                    # Inconclusive is NOT a claim that the first API response was
                    # wrong. Keep both HTTP captures; retain prior DB prices until
                    # a verified subsequent observation arrives.
                    unconfirmed_promotion_ids.add(canonical_id)
                    continue
            if store.get("inactive"):
                body,pages=None,[]
            else:
                body=read_verified(raw)
                pages=[read_verified(row["path"]) for row in store.get("catalog_responses") or []]
            args=(store,canonical_id,batch,body,pages)
            if pool is None:
                accept(canonical_id,_decode_verified_store(*args))
            else:
                pending.append((canonical_id,pool.submit(_decode_verified_store,*args)))
                if len(pending)>=worker_count:drain()
        while pending:drain()
    print("FOODRADAR_RAW_DECODE_DETAIL="+json.dumps({"workers":worker_count,
          "decode_wall_seconds":round(perf_counter()-decode_started,3)}),flush=True)
    # Unused evidence (feed, failed requests, historical rechecks) is still verified.
    for name in members:
        if name not in verified:read_verified(name)
    print("FOODRADAR_RAW_MEMBER_TIMING="+json.dumps({"members":len(verified),"read_and_hash_seconds":round(hash_seconds,3)}),flush=True)
    if len(documents) != len(canonical_ids):
        raise ValueError("Raw canonical storefront deduplication failed")
    # Multiple feed cards may point to the same canonical storefront: a valid
    # capture supersedes an empty duplicate. Never exclude a verified store.
    empty_quarantined = sorted(empty_catalog_ids - set(canonical_ids))
    promotion_quarantined = sorted(unconfirmed_promotion_ids - set(canonical_ids))
    quarantined = sorted(set(empty_quarantined) | set(promotion_quarantined))
    if quarantined:
        if quarantined_store_uuids is None:
            raise ValueError("Unverified Raw store requires explicit per-store quarantine")
        quarantined_store_uuids.extend(quarantined)
    if empty_quarantined:
        print("FOODRADAR_RAW_EMPTY_CATALOG_QUARANTINED=" + json.dumps({
            "batch": batch, "count": len(empty_quarantined),
            "store_uuids": empty_quarantined,
            "reason": "verified_native_catalog_pages_have_no_products",
        }, ensure_ascii=False), flush=True)
    if promotion_quarantined:
        print("FOODRADAR_RAW_PROMO_API_RECHECK_UNVERIFIED=" + json.dumps({
            "batch": batch, "count": len(promotion_quarantined),
            "store_uuids": promotion_quarantined,
            "reason": "independent_api_disagreed_or_retry_unavailable_not_proven_wrong",
        }, ensure_ascii=False), flush=True)
    if duplicate_canonical_stores:
        print("FOODRADAR_RAW_DUPLICATE_STORES_MERGED=" + json.dumps({
            "batch": batch, "deduplicated": duplicate_canonical_stores,
        }), flush=True)
    if repaired_url_id_mismatches:
        print("FOODRADAR_RAW_CANONICAL_UUID_REPAIRS=" + json.dumps({
            "batch": batch, "count": repaired_url_id_mismatches,
        }), flush=True)
    return documents
