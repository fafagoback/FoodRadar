"""Seven-day discount projection from V2 numeric raw-API observations ONLY.

No historical Json promotion guesses, category badges or unit-price rounding may
enter the official deal baseline. Compare exact Fraction prices per received unit.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from decimal import Decimal, ROUND_HALF_UP

TW = timezone(timedelta(hours=8))


def _two_decimals(value):
    if isinstance(value, Fraction):
        value = Decimal(value.numerator) / Decimal(value.denominator)
    return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def update_verified_weekly(conn, latest_batch_id, minimum_discount_pct=30):
    now = datetime.strptime(latest_batch_id, "%Y%m%d%H%M%S").replace(tzinfo=TW)
    cutoff = (now - timedelta(days=7)).isoformat()
    now_iso = now.isoformat()
    # Explicitly clear all old inferred price flags even for off-hours products.
    conn.execute("""UPDATE products SET seven_day_reference_price=NULL,
        seven_day_discount_pct=0,seven_day_discount_amount=0,
        seven_day_is_price_deal=0,archive_reference_price=NULL,
        archive_discount_pct=0,archive_discount_amount=0,
        archive_evidence_count=0,archive_is_price_deal=0""")
    candidates = {}
    rows = conn.execute("""SELECT o.store_uuid,o.product_uuid,o.batch_id,
        o.observed_at,o.payable_total_cents,o.received_quantity
        FROM product_price_observations o
        WHERE o.observed_at >= ? AND o.observed_at <= ?
        ORDER BY o.store_uuid,o.product_uuid,o.observed_at""", (cutoff, now_iso))
    for sid,pid,batch,at,cents,count in rows:
        if not isinstance(cents,int) or not isinstance(count,int) or count <= 0:
            continue
        record = candidates.setdefault((sid,pid), [None, []])
        unit = Fraction(cents, 100 * count)
        if at == now_iso and batch == latest_batch_id:
            record[0] = unit
        elif at < now_iso:
            record[1].append(unit)
    updates = []
    for (sid,pid),(current,prior) in candidates.items():
        if current is None or not prior:
            continue
        high = max(prior)
        drop = max(Fraction(0), high - current)
        percent = (drop / high) * 100
        pct = _two_decimals(percent)
        updates.append((_two_decimals(high), pct, _two_decimals(drop),
                        int(drop > 0 and pct >= minimum_discount_pct),
                        sid,pid,now_iso))
    conn.executemany("""UPDATE products SET
        seven_day_reference_price=?,seven_day_discount_pct=?,
        seven_day_discount_amount=?,seven_day_is_price_deal=?
        WHERE store_uuid=? AND product_uuid=? AND last_seen=?
          AND price_evidence_status='VERIFIED_MENU'
          AND is_open=1 AND status='active'""", updates)
    conn.commit()
    return {"qualified_comparisons": len(updates),
            "deals": conn.execute("""SELECT COUNT(*) FROM products
                WHERE seven_day_is_price_deal=1""").fetchone()[0]}
