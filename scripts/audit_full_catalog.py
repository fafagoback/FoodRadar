"""Whole-scope FoodRadar daily integrity audit over the latest complete HF Raw capture.

This is not an independent inventory of everything on Uber Eats. It verifies
every SKU that Uber returned to our captured getStoreV1 + getCatalogPresentationV2
responses survived untouched HF storage and normalized menu conversion, with a
read-only check of the published Turso batch.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tarfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from huggingface_hub import hf_hub_download

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from raw_capture import documents_from_hf_raw
from publish_turso_delta import Remote

TW = timezone(timedelta(hours=8))
OUT = ROOT / "audit-results" / "full-catalog-latest.json"


def native_products(envelope):
    data = envelope.get("data") or {}
    for sections in (data.get("catalogSectionsMap") or {}).values():
        if not isinstance(sections, list):
            continue
        for section in sections:
            if not isinstance(section, dict):
                continue
            for item in (((section.get("payload") or {}).get("standardItemsPayload") or {})
                         .get("catalogItems") or []):
                if isinstance(item, dict):
                    yield item


def sku_identifier(item):
    uid = item.get("uuid") or item.get("itemUuid") or item.get("id")
    if isinstance(uid, str) and uid.strip():
        return "id:" + uid.strip()
    title = item.get("title") or item.get("name")
    return "name:" + str(title).strip().casefold() if title else None


def audit_merged_native(native, doc):
    expected = {}
    conflicts = []
    for item in native_products({"data": native}):
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        key = sku_identifier(item)
        if not key:
            continue
        price = item.get("price")
        if key in expected and (expected[key]["title"] != title or
                                expected[key]["price"] != price):
            conflicts.append({"sku":key,"existing":expected[key]["title"],"new":title})
        expected.setdefault(key, {"title":title,"price":price})
    actual = {}
    for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
        for item in section.get("hasMenuItem") or []:
            key = sku_identifier({"uuid":item.get("identifier"),"name":item.get("name")})
            if key and item.get("name"):
                actual.setdefault(key,item)
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    price_mismatches = []
    for key in set(expected) & set(actual):
        raw_price = expected[key]["price"]
        if type(raw_price) not in (int,float) or raw_price <= 0:
            continue
        try:
            parsed = float(actual[key]["offers"]["price"])
        except (KeyError,TypeError,ValueError):
            price_mismatches.append(key)
            continue
        if abs(parsed - round(float(raw_price)/100,2)) > 0.011:
            price_mismatches.append(key)
    return {
        "raw_unique_skus":len(expected),"etl_unique_skus":len(actual),
        "raw_missing_in_etl":len(missing),
        "raw_missing_examples":missing[:12],
        "etl_extra_not_in_raw":len(extra),
        "etl_extra_examples":extra[:12],
        "price_mismatches":len(price_mismatches),
        "price_mismatch_examples":price_mismatches[:12],
        "same_sku_conflicting_native_records":len(conflicts),
        "conflicting_examples":conflicts[:6],
    }


def audit_native_raw(manifest, archive, docs):
    if manifest.get("complete") is not True:
        raise AssertionError("Refusing incomplete HF Raw batch")
    if len(docs) != len(manifest.get("stores", [])):
        raise AssertionError("Store-level replay count mismatch")
    members = {m.name:m for m in archive.getmembers() if m.isfile()}
    totals=Counter()
    reports=[]
    for store,doc in zip(manifest["stores"],docs):
        sid=store["store_uuid"]
        if doc.get("store_uuid") != sid:
            raise AssertionError("Raw/ETL store UUID order differs")
        entry={"uuid":sid,"inactive":bool(store.get("inactive")),
               "catalog_pages":len(store.get("catalog_responses") or []),
               "catalog_complete":store.get("catalog_complete")}
        if store.get("inactive"):
            totals["inactive_stores"] += 1
            reports.append(entry)
            continue
        original=json.load(archive.extractfile(members[store["response_path"]]))
        native=original["data"]
        from catalog_complete import merge_catalog_pages
        pages=[]
        previous_by_section={}
        terminal_by_section={}
        section_order=[]
        for record in store.get("catalog_responses") or []:
            page=json.load(archive.extractfile(members[record["path"]]))
            payload=page.get("data") or {}
            info=payload.get("pagingInfo") or {}
            current=record.get("offset")
            section_uuid=record.get("section_uuid") or "__legacy_all_sections__"
            previous=previous_by_section.get(section_uuid,-1)
            if not isinstance(current,int) or current <= previous:
                raise AssertionError(
                    f"Non-monotonic native page offsets: {sid}/{section_uuid}")
            if type(info.get("hasMore")) is not bool:
                raise AssertionError(f"Native page lacks hasMore boolean: {sid}")
            if terminal_by_section.get(section_uuid):
                raise AssertionError(
                    f"Pages continue after terminal page: {sid}/{section_uuid}")
            if section_uuid not in section_order:
                section_order.append(section_uuid)
            previous_by_section[section_uuid]=current
            terminal_by_section[section_uuid]=info["hasMore"] is False
            pages.append(page)
        if pages and not all(terminal_by_section.values()):
            raise AssertionError(f"Catalog sections lack terminal pages: {sid}")
        advertised=store.get("catalog_section_count")
        has_per_category_metadata=all(
            "section_uuid" in rec for rec in store.get("catalog_responses") or [])
        if advertised is not None and pages and has_per_category_metadata and (
                len(section_order)!=advertised):
            raise AssertionError(
                f"Catalog category coverage mismatch: {sid} "
                f"{len(section_order)} vs {advertised}")
        if store.get("catalog_responses") and not store.get("catalog_complete"):
            raise AssertionError(f"Store claims successful but incomplete catalog: {sid}")
        native=merge_catalog_pages(native,pages)
        entry.update(audit_merged_native(native,doc))
        entry["store_is_open"]=bool(doc.get("isOpen"))
        totals.update({key:entry[key] for key in
            ("raw_unique_skus","etl_unique_skus","raw_missing_in_etl",
             "etl_extra_not_in_raw","price_mismatches",
             "same_sku_conflicting_native_records")})
        totals["catalog_pages"] += entry["catalog_pages"]
        totals["active_stores"] += 1
        if entry["raw_missing_in_etl"] or entry["price_mismatches"]:
            totals["failed_stores"] += 1
        reports.append(entry)
    return dict(totals),reports


def audit_turso(latest_batch):
    url=os.environ.get("TURSO_DATABASE_URL")
    token=os.environ.get("TURSO_READONLY_TOKEN")
    if not url or not token:
        return {"status":"NOT_CONFIGURED"}
    db=Remote(url,token)
    try:
        rows=db.query("SELECT key,value FROM metadata WHERE key IN ('latest_batch','active_products','active_stores')")
        m={}
        for row in rows:
            try:m[row["key"]]=json.loads(row["value"])
            except (ValueError,TypeError):m[row["key"]]=row["value"]
        count=db.query("SELECT COUNT(*) AS n FROM web_products")[0]["n"]
        version_match=(m.get("latest_batch")==latest_batch)
        return {"status":"CURRENT" if version_match else "STALE",
                "hf_latest_batch":latest_batch,"turso_latest_batch":m.get("latest_batch"),
                "web_product_cards":count,
                "turso_active_products":m.get("active_products"),
                "turso_active_stores":m.get("active_stores"),
                "same_batch":version_match}
    finally:db.close()


def main():
    token=os.environ["HF_TOKEN"]
    manifest_path=Path(hf_hub_download("hub-google/FoodRadar","Raw/latest.json",
                          repo_type="dataset",token=token))
    manifest=json.loads(manifest_path.read_text(encoding="utf-8"))
    archive_path=Path(hf_hub_download("hub-google/FoodRadar",manifest["archive_path"],
                          repo_type="dataset",token=token))
    if hashlib.sha256(archive_path.read_bytes()).hexdigest()!=manifest["archive_sha256"]:
        raise AssertionError("HF archive SHA-256 differs from manifest")
    with tarfile.open(archive_path,"r:gz") as archive:
        docs=documents_from_hf_raw(archive,manifest)
        totals,records=audit_native_raw(manifest,archive,docs)
    issues=[r for r in records if r.get("raw_missing_in_etl") or r.get("price_mismatches")]
    remote=audit_turso(manifest["batch_id"])
    status=("PASS_WITHIN_OBSERVED_RAW_SCOPE" if not issues and remote.get("same_batch") is True
            else "FAIL_OR_UNVERIFIED")
    report={"checked_at":datetime.now(TW).isoformat(),
        "batch_id":manifest["batch_id"],
        "status":status,
        "scope":"All stores in latest successful HF Raw batch; NOT all undisclosed Uber Eats inventory",
        "hf_body_count":len(manifest["responses"]),"hf_sha256_verified":True,
        "captured_store_count":len(manifest["stores"]),
        "totals":totals,"failing_store_count":len(issues),
        "failing_store_examples":issues[:25],
        "stores":records,"turso":remote}
    OUT.parent.mkdir(exist_ok=True,parents=True)
    OUT.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print("FULL_SCOPE_CATALOG_AUDIT="+json.dumps({
        "batch":report["batch_id"],"status":status,
        "stores":report["captured_store_count"],"raw_bodies":report["hf_body_count"],
        "raw_skus":totals.get("raw_unique_skus"),"etl_skus":totals.get("etl_unique_skus"),
        "sku_missing":totals.get("raw_missing_in_etl"),
        "price_mismatches":totals.get("price_mismatches"),"turso":remote,
        "sample_issues":issues[:4]},ensure_ascii=False),flush=True)
    if status!="PASS_WITHIN_OBSERVED_RAW_SCOPE":
        raise SystemExit("HF Raw -> ETL -> Turso full-scope audit not verified")


if __name__=="__main__":main()
