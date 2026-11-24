"""Verified seven-day price projection, with one canonical history-window reader.

A completed A-B-A reversal within 14 days suspends seven-day discount claims;
older reversals also block while their disputed high remains in the baseline. We do not
change raw Uber quotes, platform promotions, or ordinary product search.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction
from itertools import groupby

from price_oscillation import REVERSAL_DAYS, has_disputed_high_in_week

TW = timezone(timedelta(hours=8))
UPDATE_BATCH_SIZE = 2048

PRICE_UPDATE_SQL = """UPDATE products SET
    seven_day_reference_price=?,seven_day_discount_pct=?,
    seven_day_discount_amount=?,seven_day_is_price_deal=?
    WHERE store_uuid=? AND product_uuid=? AND last_seen=?
      AND price_evidence_status='VERIFIED_MENU'
      AND delivery_available=1 AND is_open=1 AND status='active'
      AND EXISTS (SELECT 1 FROM stores s WHERE s.store_uuid=products.store_uuid
                  AND s.status='active' AND s.is_open=1
                  AND s.last_seen=products.last_seen)"""


def _two_decimals(value):
    if isinstance(value, Fraction):
        value = Decimal(value.numerator) / Decimal(value.denominator)
    return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def iter_weekly_price_evidence(conn, latest_batch_id, days=7):
    """Yield only current comparable SKUs, one at a time.

    Format: (store_uuid, product_uuid, current_unit, prior_units,
             oscillation_blocks_discount).

    Both the publisher and independent auditor consume this verified evidence.
    The auditor independently recomputes persisted monetary values.
    The extra lookback retains a completed reversal after its first A expires,
    without extending the actual seven-day reference-price window.
    """
    now = datetime.strptime(latest_batch_id, "%Y%m%d%H%M%S").replace(tzinfo=TW)
    cutoff = now - timedelta(days=days)
    lookback = now - timedelta(days=days + REVERSAL_DAYS)
    now_iso = now.isoformat()

    cursor = conn.execute("""SELECT store_uuid,product_uuid,batch_id,observed_at,
        payable_total_cents,received_quantity
        FROM product_price_observations
        WHERE observed_at>=? AND observed_at<=?
        ORDER BY store_uuid,product_uuid,observed_at,batch_id""",
        (lookback.isoformat(), now_iso))
    for (sid, pid), records in groupby(cursor, key=lambda r: (r[0], r[1])):
        observations = []
        previous = []
        current = None
        for _, _, batch, observed_at, cents, count in records:
            if (type(cents) is not int or type(count) is not int
                    or cents <= 0 or count <= 0):
                continue
            unit = Fraction(cents, 100 * count)
            stamp = datetime.fromisoformat(observed_at)
            observations.append((stamp, unit))
            if observed_at == now_iso and batch == latest_batch_id:
                current = unit
            elif cutoff <= stamp < now:
                previous.append(unit)
        if current is not None and previous:
            yield sid, pid, current, previous, has_disputed_high_in_week(
                observations, now, days=days)


def update_verified_weekly(conn, latest_batch_id, minimum_discount_pct=30):
    """Publish price flags in bounded write batches (safe at nationwide scale)."""
    now_iso = datetime.strptime(
        latest_batch_id, "%Y%m%d%H%M%S").replace(tzinfo=TW).isoformat()
    conn.execute("""UPDATE products SET seven_day_reference_price=NULL,
        seven_day_discount_pct=0,seven_day_discount_amount=0,
        seven_day_is_price_deal=0,archive_reference_price=NULL,
        archive_discount_pct=0,archive_discount_amount=0,
        archive_evidence_count=0,archive_is_price_deal=0""")

    updates = []
    comparisons = suppressed = 0

    for sid, pid, current, previous, blocked in iter_weekly_price_evidence(
            conn, latest_batch_id):
        comparisons += 1
        if blocked:
            suppressed += 1
            updates.append((None, 0.0, 0.0, 0, sid, pid, now_iso))
        else:
            highest = max(previous)
            difference = max(Fraction(0), highest - current)
            percent = _two_decimals(100 * difference / highest)
            updates.append((_two_decimals(highest), percent,
                _two_decimals(difference),
                int(difference > 0 and percent >= minimum_discount_pct),
                sid, pid, now_iso))

        if len(updates) >= UPDATE_BATCH_SIZE:
            conn.executemany(PRICE_UPDATE_SQL, updates)
            updates.clear()
    if updates:
        conn.executemany(PRICE_UPDATE_SQL, updates)

    conn.commit()
    return {"qualified_comparisons": comparisons,
            "oscillation_suppressed": suppressed,
            "deals": conn.execute("""SELECT COUNT(*) FROM products
                WHERE seven_day_is_price_deal=1""").fetchone()[0]}
