"""Read-only audit of ACTUAL published FoodRadar Turso, not mock data.

No token, address, or customer data is printed. Queries only summary
metadata/table counts; the independent HF RAW script audits SKU-level prices.
"""
import json
import os
from pathlib import Path

from publish_turso_delta import Remote


def main():
    remote = Remote(os.environ["TURSO_DATABASE_URL"], os.environ["TURSO_READONLY_TOKEN"])
    try:
        selected = (
            "latest_batch", "latest_processed_at", "definition_version",
            "active_stores", "active_products", "active_comparable_products",
            "seven_day_reference_products", "seven_day_verified_dates",
            "seven_day_history_incomplete", "active_30pct_drops",
            "archive_reference_products", "archive_30pct_candidates",
            "active_platform_promotions", "source_counts",
        )
        rows = remote.query("SELECT key,value FROM metadata")
        values = {}
        for row in rows:
            if row["key"] in selected:
                try:values[row["key"]] = json.loads(row["value"])
                except (ValueError, TypeError):values[row["key"]] = row["value"]
        # A successful query against a stale database is not a successful
        # release. Cross-check the exact batch prepared from HF Raw.
        expected_batch = Path("data/latest.txt").read_text(encoding="utf-8").strip()
        if not expected_batch or str(values.get("latest_batch")) != expected_batch:
            raise ValueError(
                f"Turso published batch does not match HF release: "
                f"expected {expected_batch}, observed {values.get('latest_batch')}")
        observed = {}
        for table in ("store_directory","store_bundles","search_buckets","metadata"):
            observed[table] = remote.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]
        # Validate actual web-facing fields, not only aggregate table counts.
        observed["web_products"] = remote.query("SELECT COUNT(*) AS n FROM web_products")[0]["n"]
        projection = remote.query("""SELECT
            SUM(CASE WHEN json_extract(card,'$.image_url') IS NOT NULL THEN 1 ELSE 0 END) AS images,
            SUM(CASE WHEN json_extract(card,'$.display_original_price') IS NOT NULL THEN 1 ELSE 0 END) AS list_prices,
            SUM(CASE WHEN json_extract(card,'$.cart_promo_label') IS NOT NULL THEN 1 ELSE 0 END) AS cart_promotions,
            SUM(CASE WHEN json_extract(card,'$.is_sold_out') = 1 THEN 1 ELSE 0 END) AS sold_out,
            SUM(CASE WHEN json_extract(card,'$.display_original_price') IS NOT NULL
                      AND json_extract(card,'$.display_original_price') <= json_extract(card,'$.price')
                     THEN 1 ELSE 0 END) AS impossible_list_price,
            SUM(CASE WHEN platform_promo=1 AND (
                          json_extract(card,'$.is_sold_out') = 1
                          OR json_extract(card,'$.is_available') = 0)
                     THEN 1 ELSE 0 END) AS unavailable_promotions
            FROM web_products""")[0]
        if projection["impossible_list_price"] or projection["unavailable_promotions"]:
            raise ValueError("Published catalog contains invalid display price or unavailable promotion")
        beitou = remote.query("""SELECT COUNT(*) AS total,
            SUM(CASE WHEN instr(lower(search_text),'哈根達斯')>0
                      OR instr(lower(search_text),'häagen')>0
                     THEN 1 ELSE 0 END) AS haagen
            FROM web_products
            WHERE store_uuid='d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42'""")[0]
        print("FOODRADAR_LIVE_TURSO_CATALOG_AUDIT="+json.dumps(
            {"projection":projection,"beitou":beitou},ensure_ascii=False),flush=True)
        print("FOODRADAR_LIVE_TURSO_AUDIT="+json.dumps(
            {"metadata":values,"physical_table_counts":observed},
            ensure_ascii=False,sort_keys=True),flush=True)
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a", encoding="utf-8") as out:
                out.write(
                    "\\n### 正式 Turso 唯讀驗證（已核對批次）\\n"
                    "| 項目 | 正式資料庫結果 |\\n|---|---:|\\n"
                    f"| 已發布批次 | {expected_batch} |\\n"
                    f"| 店家索引筆數 | {observed['store_directory']} |\\n"
                    f"| 商品筆數 | {observed['web_products']} |\\n"
                    f"| 最新商品來源數 | {values.get('active_products', 'N/A')} |\\n")
    finally:
        remote.close()

if __name__ == "__main__":
    main()
