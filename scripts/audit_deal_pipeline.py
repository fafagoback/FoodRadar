"""Read-only release audit of verified seven-day projected discounts.

Shares the canonical streamed observation window, but recalculates the
published numbers independently. No nationwide in-memory SKU dictionary.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from verified_weekly import TW, _two_decimals, iter_weekly_price_evidence


def report(path):
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        batch = conn.execute(
            "SELECT value FROM metadata WHERE key='latest_batch'").fetchone()[0]
        now_iso = datetime.strptime(
            batch, "%Y%m%d%H%M%S").replace(tzinfo=TW).isoformat()

        # Merge-join two ordered cursors: one SKU's evidence in memory at once.
        evidence = iter(iter_weekly_price_evidence(conn, batch))
        candidate = next(evidence, None)
        inspected = compared = suppressed = matched_deals = 0
        errors = []
        products = conn.execute("""SELECT p.*, s.is_open AS store_is_open,
                s.status AS store_status, s.last_seen AS store_last_seen
            FROM products p JOIN stores s ON s.store_uuid=p.store_uuid
            ORDER BY p.store_uuid,p.product_uuid""")

        for row in products:
            key = (row["store_uuid"], row["product_uuid"])
            while candidate is not None and candidate[:2] < key:
                candidate = next(evidence, None)

            comparable = candidate is not None and candidate[:2] == key
            if comparable:
                compared += 1
            eligible = (comparable and row["status"] == "active"
                and row["is_open"] == 1 and row["store_is_open"] == 1
                and row["store_status"] == "active"
                and row["store_last_seen"] == row["last_seen"]
                and row["delivery_available"] == 1
                and row["price_evidence_status"] == "VERIFIED_MENU"
                and row["last_seen"] == now_iso)

            ref = None
            percent = savings = 0.0
            deal = 0
            if eligible:
                _, _, current, previous, blocked = candidate
                if blocked:
                    suppressed += 1
                else:
                    highest = max(previous)
                    saved = max(Fraction(0), highest - current)
                    ref = _two_decimals(highest)
                    percent = _two_decimals(saved * 100 / highest)
                    savings = _two_decimals(saved)
                    deal = int(saved > 0 and percent >= 30)
            expected = (ref, percent, savings, deal)
            actual = (row["seven_day_reference_price"],
                row["seven_day_discount_pct"],
                row["seven_day_discount_amount"],
                row["seven_day_is_price_deal"])
            inspected += 1
            matched_deals += deal
            if not all(
                (a is None and b is None) or
                (a is not None and b is not None and abs(float(a) - float(b)) <= 0.0101)
                for a, b in zip(actual, expected)
            ):
                errors.append({"store_uuid": key[0], "product_uuid": key[1],
                    "expected": expected, "actual": actual})
                if len(errors) >= 12:
                    break

        result = {
            "batch": batch,
            "audited_products": inspected,
            "eligible_comparison_count": compared,
            "oscillation_suppressed": suppressed,
            "certified_7day_30pct_deals": matched_deals,
            "errors": errors,
        }
        print("V2_DEAL_PIPELINE_AUDIT=" + json.dumps(
            result, ensure_ascii=False), flush=True)
        if errors:
            raise ValueError(
                "Verified V2 prices and published seven-day discounts disagree")
        return result


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: audit_deal_pipeline.py path/to/serving.db")
    report(sys.argv[1])
