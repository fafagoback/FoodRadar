"""Attach immutable HF getStoreV1 captures to their converted Json documents.

2026-10-29 historical archives have ApiRaw/<batch>_<md5(store_url)>_<name>.json
but Json/<batch>_<sha256(canonical_store_url)>.json. Therefore comparing
member basenames is demonstrably wrong. New archives align both member paths.
Match older captures using the canonical URL's exact MD5 store ID instead of
names, section labels, file sorting, or product price guesses.
"""
from __future__ import annotations

import hashlib
import json
from urllib.parse import urlsplit, urlunsplit

from raw_price_enrichment import enrich_converted_with_native


def _store_id_from_document(doc):
    value = str(doc.get("store_id") or "").strip().lower()
    if len(value) != 32 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("Cannot safely join archived raw data without a 32-digit store URL MD5")
    return value


def archived_documents_with_native(tar, members, batch, metadata):
    menu_names = sorted(name for name in members if name.startswith("Json/"))
    raw_names = [name for name in members if name.startswith("ApiRaw/")]
    expected_raw = int(metadata.get("api_raw_response_count") or 0)
    if expected_raw != len(raw_names):
        raise ValueError(
            f"Raw capture manifest mismatch for {batch}: "
            f"manifest {expected_raw} versus archive {len(raw_names)}")
    # Historical source filename starts with batch + URL MD5 + an underscore.
    # All matches must be unique: ambiguous joins must abort the replay.
    raw_by_md5 = {}
    prefix = f"ApiRaw/{batch}_"
    for name in raw_names:
        if not name.startswith(prefix):
            # New archives use the same hashed name as Json; handled below.
            continue
        suffix = name[len(prefix):]
        md5 = suffix[:32].lower()
        if len(md5) == 32 and all(c in "0123456789abcdef" for c in md5) and suffix[32:33] == "_":
            if md5 in raw_by_md5:
                raise ValueError(f"Duplicate raw capture store key in {batch}")
            raw_by_md5[md5] = name
    docs=[]
    matched=set()
    for name in menu_names:
        doc=json.load(tar.extractfile(members[name]))
        raw_name="ApiRaw/" + name[len("Json/"):]
        if raw_name not in members and raw_names:
            md5=_store_id_from_document(doc)
            raw_name=raw_by_md5.get(md5)
        if raw_name and raw_name in members:
            if raw_name in matched:
                raise ValueError(f"One native raw response mapped twice in batch {batch}")
            wrapper=json.load(tar.extractfile(members[raw_name]))
            raw_data=wrapper.get("data") or {}
            if not isinstance(raw_data,dict):
                raise ValueError(f"Invalid native response for batch {batch}")
            native_uuid = raw_data.get("uuid")
            doc_uuid = doc.get("store_uuid")
            if native_uuid and doc_uuid and str(native_uuid).lower() != str(doc_uuid).lower():
                raise ValueError(f"Raw/canonical store UUID disagreement in {batch}")
            doc=enrich_converted_with_native(doc,wrapper)
            matched.add(raw_name)
        docs.append(doc)
    if len(matched) != expected_raw:
        raise ValueError(
            f"Raw pairing failed in {batch}: joined {len(matched)}/{expected_raw}, "
            "refusing unverified price rebuild")
    return docs, {"batch":batch,"json_files":len(menu_names),
                  "raw_files":len(raw_names),"native_matched":len(matched)}
