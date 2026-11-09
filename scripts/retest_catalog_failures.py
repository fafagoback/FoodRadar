"""Live regression of the four stores that failed HF Raw batch 20261109105106.

No HF uploads or Turso writes. Uses the same production getStoreV1 +
getCatalogPresentationV2 capture path and retains only temporary HTTP bytes.
"""
from __future__ import annotations
import json
import os
import sys
import tempfile
from pathlib import Path
from huggingface_hub import hf_hub_download

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from raw_capture import fetch_store_raw

SOURCE_BATCH = "20261109105106"


def main():
    manifest = Path(hf_hub_download(
        "hub-google/FoodRadar", f"Raw/{SOURCE_BATCH}/manifest.json",
        repo_type="dataset", token=os.environ["HF_TOKEN"]))
    source = json.loads(manifest.read_text(encoding="utf-8"))
    failed = [x for x in source.get("stores", []) if not x.get("success")]
    if len(failed) != 4:
        raise AssertionError(f"Expected exactly four failed stores; got {len(failed)}")
    results = []
    with tempfile.TemporaryDirectory(prefix="foodradar-retest-") as tmp:
        root = Path(tmp)
        for store in failed:
            entry = {
                "store_uuid": store["store_uuid"],
                "store_url": store["store_url"],
                "discovered_points": [{
                    "lat": 25.123246847935267, "lon": 121.52951826298867,
                    "county": "台北市"
                }],
            }
            result = fetch_store_raw(entry, root)
            page_paths = [x["path"] for x in result.get("catalog_responses", [])]
            # No swallowed exceptions: all four must yield a complete native
            # catalog while keeping page bytes in their original form.
            record = {
                "uuid": entry["store_uuid"],
                "success": result.get("success", False),
                "catalog_complete": result.get("catalog_complete"),
                "pages": len(page_paths),
                "item_occurrences": result.get("catalog_item_occurrences", 0),
                "error": result.get("error"),
            }
            results.append(record)
            print("LIVE_FAILED_STORE_RETEST=" + json.dumps(record, ensure_ascii=False), flush=True)
        if not all(x["success"] and x["catalog_complete"] for x in results):
            raise AssertionError("One or more previously failed stores still could not be captured")
        body_count = len(list(root.rglob("*.body")))
        print("LIVE_FAILED_STORE_RETEST_SUMMARY=" + json.dumps({
            "stores_checked": len(results), "recovered_stores": len(results),
            "native_body_files_preserved": body_count,
            "catalog_page_count": sum(x["pages"] for x in results)
        }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
