"""Read-only audit of ACTUAL published FoodRadar Turso, not mock data.

No token, address, or customer data is printed. Queries only summary
metadata/table counts; the independent HF RAW script audits SKU-level prices.
"""
import json
import os

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
        observed = {}
        for table in ("store_directory","store_bundles","search_buckets","metadata"):
            observed[table] = remote.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]
        print("FOODRADAR_LIVE_TURSO_AUDIT="+json.dumps(
            {"metadata":values,"physical_table_counts":observed},
            ensure_ascii=False,sort_keys=True),flush=True)
    finally:
        remote.close()

if __name__ == "__main__":
    main()
