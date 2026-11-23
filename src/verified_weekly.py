"""Seven-day discount projection from verified V2 numeric observations.

Price reversals A-B-A block the price-drop listing while any of the
reversal's high-priced observations remain in the comparison window.
Neither Uber Raw observations nor normal catalog prices are modified.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from decimal import Decimal, ROUND_HALF_UP
from itertools import groupby

from price_oscillation import has_disputed_high_in_week

TW = timezone(timedelta(hours=8))


def _two_decimals(value):
    if isinstance(value, Fraction):
        value = Decimal(value.numerator) / Decimal(value.denominator)
    return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def update_verified_weekly(conn, latest_batch_id, minimum_discount_pct=30):
    now = datetime.strptime(latest_batch_id, "%Y%m%d%H%M%S").replace(tzinfo=TW)
    cutoff = (now - timedelta(days=7)).isoformat()
    lookback = (now - timedelta(days=14)).isoformat()
    now_iso = now.isoformat()
    conn.execute("""UPDATE products SET seven_day_reference_price=NULL,
        seven_day_discount_pct=0,seven_day_discount_amount=0,
        seven_day_is_price_deal=0,archive_reference_price=NULL,
        archive_discount_pct=0,archive_discount_amount=0,
        archive_evidence_count=0,archive_is_price_deal=0""")

    # Stream by SKU rather than materializing the entire nationwide history.
    # An extra seven days is required to recognize A-B-A after the first A
    # moves outside the current seven-day baseline window.
    rows = conn.execute("""SELECT store_uuid,product_uuid,batch_id,observed_at,
            payable_total_cents,received_quantity
        FROM product_price_observations
        WHERE observed_at >= ? AND observed_at <= ?
        ORDER BY store_uuid,product_uuid,observed_at,batch_id""",
        (lookback, now_iso))
    updates = []
    suppressed = 0
    for (sid, pid), sku_rows in groupby(rows, key=lambda row: (row[0], row[1])):
        observations = []
        previous = []
        current = None
        for _, _, batch, at, cents, count in sku_rows:
            if not isinstance(cents, int) or not isinstance(count, int) or cents <= 0 or count <= 0:
                continue
            price = Fraction(cents, 100 * count)
            observations.append((datetime.fromisoformat(at), price))
            if at == now_iso and batch == latest_batch_id:
                current = price
            elif cutoff <= at < now_iso:
                previous.append(price)
        if current is None or not previous:
            continue

        if has_disputed_high_in_week(observations, now):
            # The full SKU remains searchable. Only its seven-day discount
            # reference and deal flags are withheld until the dispute expires.
            suppressed += 1
            updates.append((None, 0.0, 0.0, 0, sid, pid, now_iso))
            continue

        high = max(previous)
        drop = max(Fraction(0), high - current)
        pct = _two_decimals(100 * drop / high)
        updates.append((_two_decimals(high), pct, _two_decimals(drop),
                        int(drop > 0 and pct >= minimum_discount_pct),
                        sid, pid, now_iso))

    conn.executemany("""UPDATE products SET
        seven_day_reference_price=?,seven_day_discount_pct=?,
        seven_day_discount_amount=?,seven_day_is_price_deal=?
        WHERE store_uuid=? AND product_uuid=? AND last_seen=?
          AND price_evidence_status='VERIFIED_MENU'
          AND delivery_available=1 AND is_open=1 AND status='active'
          AND EXISTS (SELECT 1 FROM stores s WHERE s.store_uuid=products.store_uuid
                      AND s.status='active' AND s.is_open=1
                      AND s.last_seen=products.last_seen)""", updates)
    conn.commit()
    return {"qualified_comparisons": len(updates),
            "oscillation_suppressed": suppressed,
            "deals": conn.execute("""SELECT COUNT(*) FROM products
                WHERE seven_day_is_price_deal=1""").fetchone()[0]}
