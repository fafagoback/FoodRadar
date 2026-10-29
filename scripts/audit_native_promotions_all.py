"""Read-only, exhaustive Uber getStoreV1 promotion field inventory for EVERY item.

Uses only HF ApiRaw/ snapshots and does not alter HF, Turso, or web.
Print bounded aggregate evidence and representative inconsistencies.
"""
import collections
import hashlib
import json
import os
import re
import tarfile
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download

REPO = "hub-google/UberEat"
PREFIX = "FoodRadar"
MONEY = re.compile(r"(?:NT\$|TWD|\$)\s*([0-9][0-9,]*(?:\.[0-9]+)?)", re.I)
PROMO_TEXT = re.compile(r"\b(?:[0-9]+(?:\.[0-9]+)?%\s*off|[0-9]+\s*for\s*[0-9]+|[0-9]+\s*off)\b|買\s*[一二三四五六七八九0-9]+\s*送|折|特價|優惠", re.I)


def strings_in(node):
    if isinstance(node, dict):
        if "text" in node and isinstance(node["text"], str):
            yield node["text"]
        for value in node.values():
            yield from strings_in(value)
    elif isinstance(node, list):
        for value in node:
            yield from strings_in(value)


def thumbnail_prices(item):
    found = []
    for element in (item.get("itemThumbnailElements") or []):
        for text in strings_in(element):
            for number in MONEY.findall(text):
                found.append(round(float(number.replace(",", "")), 2))
    return list(dict.fromkeys(found))


def overlay_labels(item):
    out = []
    for element in item.get("imageOverlayElements") or []:
        for text in strings_in(element):
            if text and PROMO_TEXT.search(text):
                out.append(text)
    return list(dict.fromkeys(out))


def promotion_type(item):
    return str((item.get("catalogItemAnalyticsData") or {}).get("promoType") or "MISSING")


def native_offer(item):
    promo = item.get("itemPromotion")
    if isinstance(promo, dict) and promo:
        return str(promo.get("type") or "UNNAMED")
    return "NONE"


def emit(label, value):
    print(label + "=" + json.dumps(value, ensure_ascii=False, default=str), flush=True)


def main():
    api = HfApi(token=os.environ.get("HF_TOKEN"))
    metadata = json.loads(Path(hf_hub_download(REPO, f"{PREFIX}/latest.json", repo_type="dataset", token=os.environ.get("HF_TOKEN"))).read_text())
    batch = metadata["batch_id"]
    filename = metadata["archive_path"]
    archive = hf_hub_download(REPO, filename, repo_type="dataset", token=os.environ.get("HF_TOKEN"))
    counters = collections.Counter()
    promo_types = collections.Counter()
    native_types = collections.Counter()
    promotion_type_pairs = collections.Counter()
    promo_fields = collections.Counter()
    badge_texts = collections.Counter()
    thumbnail_examples = []
    samples = collections.defaultdict(list)
    stores = set()
    unique_products = {}
    comparisons = {}
    with tarfile.open(archive, "r:gz") as tf:
        members = tf.getmembers()
        raw_members = [m for m in members if m.isfile() and m.name.startswith("ApiRaw/") and m.name.endswith(".json")]
        menu_members = [m for m in members if m.isfile() and m.name.startswith("Json/") and m.name.endswith(".json")]
        emit("AUDIT_MANIFEST", {
            "batch": batch, "raw_api_files": len(raw_members),
            "converted_menu_files": len(menu_members), "stores_expected": metadata.get("store_count"),
            "raw_expected": metadata.get("api_raw_response_count"), "archive": filename})
        for member in raw_members:
            wrapper = json.load(tf.extractfile(member))
            data = wrapper.get("data") or {}
            store_id = str(data.get("uuid") or data.get("title") or member.name)
            stores.add(store_id)
            for group, sections in (data.get("catalogSectionsMap") or {}).items():
                if not isinstance(sections, list):
                    continue
                for section in sections:
                    payload = (section.get("payload") or {}).get("standardItemsPayload") or {}
                    title = payload.get("title") or {}
                    category = title.get("text") if isinstance(title, dict) else str(title)
                    for item in payload.get("catalogItems") or []:
                        if not isinstance(item, dict):
                            continue
                        key = (store_id, str(item.get("uuid") or item.get("itemUuid") or item.get("id") or item.get("title")))
                        if not item.get("title"):
                            continue
                        counters["occurrences"] += 1
                        if key in unique_products:
                            counters["duplicate_occurrences"] += 1
                        typ = promotion_type(item)
                        native = native_offer(item)
                        badge = overlay_labels(item)
                        prices = thumbnail_prices(item)
                        price_cents = item.get("price")
                        try:
                            price = round(float(price_cents) / 100, 2)
                        except (ValueError, TypeError):
                            price = None
                        struct = item.get("itemPromotion") or {}
                        buyget = struct.get("buyXGetYItemPromotion") if isinstance(struct, dict) else None
                        buy = buyget.get("buyQuantity") if isinstance(buyget, dict) else None
                        get = buyget.get("getQuantity") if isinstance(buyget, dict) else None
                        record = dict(store=data.get("title"), sku=item.get("uuid"), title=item.get("title"),
                            category=category, native_type=native, promo_type=typ, badge=badge,
                            promo_info=item.get("promoInfo"), price=price,
                            thumbnail_prices=prices, buy=buy, get=get, native_value=struct,
                            item_level_promotion=item.get("itemLevelPromotion"),
                            purchase_info=item.get("purchaseInfo"))
                        if key not in unique_products:
                            unique_products[key] = []
                        unique_products[key].append(record)
                        promo_types[typ] += 1
                        native_types[native] += 1
                        promotion_type_pairs[typ + " / " + native] += 1
                        for b in badge:
                            badge_texts[b] += 1
                        for field in item:
                            if re.search("promo|offer|discount|benefit|deal|reward", field, re.I):
                                promo_fields[field] += 1
                        if native != "NONE" and len(samples["native:" + native]) < 4:
                            samples["native:" + native].append(record)
                        if typ != "MISSING" and len(samples["promo:" + typ]) < 5:
                            samples["promo:" + typ].append(record)
        # Summary counted on unique SKU to avoid treating repeated menu sections as more goods
        unique_type = collections.Counter()
        unique_class = collections.Counter()
        unique_badges = collections.Counter()
        conflicts = []
        opaque_promos = []
        sale_examples = []
        error_examples = []
        for key, records in unique_products.items():
            counts = collections.Counter(r["promo_type"] for r in records)
            native_counts = collections.Counter(r["native_type"] for r in records)
            if len(counts) > 1 or len(native_counts) > 1:
                unique_class["conflicting_promos_across_sections"] += 1
                if len(conflicts) < 12:
                    conflicts.append(dict(key=key, rows=records[:4]))
            has_struct_bogo = any(r["native_type"]=="buyXGetYItemPromotion" and
                isinstance(r["buy"], int) and isinstance(r["get"], int) and r["buy"]>0 and r["get"]>0 for r in records)
            has_other_struct = any(r["native_type"] not in ("NONE", "buyXGetYItemPromotion") for r in records)
            has_bogo = any(r["promo_type"]=="BOGO" for r in records)
            has_disc = any(r["promo_type"]=="DISCOUNTED_ITEM" for r in records)
            has_unknown = any(r["promo_type"] not in ("MISSING", "BOGO", "DISCOUNTED_ITEM") for r in records)
            has_prices = any(len(r["thumbnail_prices"])>=2 and r["price"] and
                 max(r["thumbnail_prices"])>r["price"] for r in records)
            has_badge = any(r["badge"] for r in records)
            if has_struct_bogo:
                classification = "STRUCT_BOGO_WITH_BUY_GET"
            elif has_bogo:
                classification = "BOGO_TYPE_WITHOUT_VERIFIED_BUY_GET"
            elif has_other_struct:
                classification = "OTHER_STRUCTURED_PROMOTION"
            elif has_disc and has_prices:
                classification = "DISCOUNTED_ITEM_WITH_PRIOR_AND_CURRENT"
            elif has_disc:
                classification = "DISCOUNTED_ITEM_WITHOUT_TWO_PRICES"
            elif has_unknown:
                classification = "UNRECOGNIZED_PROMO_TYPE"
            elif has_badge:
                classification = "PROMO_BADGE_ONLY"
            else:
                classification = "NO_DETECTED_ITEM_PROMOTION"
            unique_class[classification]+=1
            unique_type[",".join(sorted(counts))]+=1
            if has_disc and len(sale_examples)<12:
                for r in records:
                    if r["promo_type"]=="DISCOUNTED_ITEM":
                        old=max(r["thumbnail_prices"]) if r["thumbnail_prices"] else None
                        discount=round((1-r["price"]/old)*100,2) if old and r["price"] else None
                        sale_examples.append(dict(store=r["store"],title=r["title"],sku=r["sku"],price=r["price"],
                            original=old,percent_off=discount,discount_badge=r["badge"],category=r["category"]))
                        break
            if classification in ("DISCOUNTED_ITEM_WITHOUT_TWO_PRICES","BOGO_TYPE_WITHOUT_VERIFIED_BUY_GET","OTHER_STRUCTURED_PROMOTION","UNRECOGNIZED_PROMO_TYPE","PROMO_BADGE_ONLY") and len(opaque_promos)<35:
                opaque_promos.append(dict(classification=classification,rows=records[:2]))
            if has_bogo and has_disc and len(error_examples)<20:
                error_examples.append(dict(key=key,rows=records[:3]))
        emit("AUDIT_TOTALS", dict(unique_stores=len(stores),unique_products=len(unique_products),
            all_item_occurrences=counters["occurrences"],duplicate_occurrences=counters["duplicate_occurrences"]))
        emit("AUDIT_ALL_PROMO_TYPES", promo_types.most_common())
        emit("AUDIT_ALL_NATIVE_ITEM_PROMOTION_TYPES", native_types.most_common())
        emit("AUDIT_PROMO_TYPE_NATIVE_PAIRS", promotion_type_pairs.most_common(35))
        emit("AUDIT_CLASSIFIED_UNIQUE_PRODUCTS", unique_class.most_common())
        emit("AUDIT_FIELDS", promo_fields.most_common())
        emit("AUDIT_BADGE_TYPES", badge_texts.most_common(40))
        emit("AUDIT_DISCOUNT_EXAMPLES", sale_examples)
        emit("AUDIT_UNKNOWN_EXAMPLES", opaque_promos[:16])
        emit("AUDIT_CATEGORY_CONFLICTS", conflicts[:6])
        emit("AUDIT_BOGO_AND_DISCOUNT_CONFLICTS", error_examples[:6])
        emit("AUDIT_STRUCTURED_EXAMPLES", {k:v[:3] for k,v in samples.items() if k.startswith("native:")})
        # An additional field-level completeness check, avoiding any text/category
        # inference except as a *counterexample* to the current parser.
        import sys
        sys.path.insert(0, "src")
        from json_to_db import extract_promo_info
        details = collections.defaultdict(collections.Counter)
        type_examples = collections.defaultdict(list)
        mismatches = []
        summary_badges = collections.Counter()
        for key, records in unique_products.items():
            seen = set()
            for record in records:
                seen.add(record["promo_type"])
            ptype = sorted(seen - {"MISSING"})[0] if seen - {"MISSING"} else "MISSING"
            info = next((r for r in records if r.get("promo_info")), records[0])
            badge = set()
            for row in records:
                badge.update(row["badge"])
                raw_info = row.get("promo_info") or {}
                promo_badge = raw_info.get("promoBadge") or {}
                if isinstance(promo_badge, dict):
                    tag = promo_badge.get("accessibilityText")
                    if tag:badge.add(str(tag))
                    for x in strings_in(promo_badge):
                        if PROMO_TEXT.search(x):badge.add(x)
            has_old = any(len(r["thumbnail_prices"]) >= 2 and
                max(r["thumbnail_prices"]) > (r["price"] or 0) for r in records)
            has_badge = bool(badge)
            has_percent = any(re.search(r"\\d+(?:\\.\\d+)?%\\s*off", b, re.I) for b in badge)
            details[ptype]["count"] += 1
            details[ptype]["old_price_in_thumbnail"] += int(has_old)
            details[ptype]["promo_badge"] += int(has_badge)
            details[ptype]["explicit_percent_badge"] += int(has_percent)
            details[ptype]["with_item_promotion"] += int(any(r["native_type"]!="NONE" for r in records))
            details[ptype]["with_item_level_promotion"] += int(any(bool(r.get("item_level_promotion")) for r in records))
            details[ptype]["with_promo_uuid"] += int(any(bool((r.get("promo_info") or {}).get("promotionUUID")) for r in records))
            if ptype!="MISSING":
                for badge_text in sorted(badge):
                    summary_badges[(ptype,badge_text)] += 1
                if len(type_examples[ptype]) < 3:
                    type_examples[ptype].append(dict(
                        store=info["store"], title=info["title"], price=info["price"],
                        prices=info["thumbnail_prices"], promo_uuid=info.get("promo_info",{}).get("promotionUUID"),
                        badge=sorted(badge), item_promotion=info["native_value"],
                        item_level=info.get("item_level_promotion"),
                        purchase_info=info.get("purchase_info")))
            by_category = [extract_promo_info(row.get("category") or "",row["title"],"")[0] for row in records]
            legacy_bogo = any(re.match(r"買\\d+送\\d+", p) for p in by_category)
            native_bogo = any(row["promo_type"] in ("BOGO", "BOGA", "MULTI_SKU_BXFY") and
                row["native_type"]=="buyXGetYItemPromotion" for row in records)
            if legacy_bogo and not native_bogo:
                details["LEGACY_CATEGORY_PARSING"]["FALSE_BOGO_SKUS"] += 1
                if len(mismatches)<12:
                    mismatches.append(dict(store=info["store"],title=info["title"],sku=info["sku"],
                        promo_type=ptype,legacy_categories=sorted(set(r["category"] for r in records)),
                        badge=sorted(badge),price=info["price"],prices=info["thumbnail_prices"]))
            if native_bogo and not legacy_bogo:
                details["LEGACY_CATEGORY_PARSING"]["MISSED_BOGO_SKUS"] += 1
        emit("AUDIT_UNIQUE_PROMO_TYPE_COVERAGE", {k:dict(v) for k,v in details.items()})
        emit("AUDIT_REAL_PROMO_EXAMPLES_BY_TYPE", dict(type_examples))
        emit("AUDIT_PROMO_BADGES_WITH_SOURCE", [
            {"type":kind,"badge":label,"count":count} for (kind,label),count
            in summary_badges.most_common(45)])
        emit("AUDIT_LEGACY_BOGO_FALSE_POSITIVES", mismatches)
        emit("AUDIT_COMPLETE", True)

if __name__=="__main__":
    main()
