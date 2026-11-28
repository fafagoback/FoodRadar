"""Prune identities that have no observations inside the verified 60-day window.

This runs after canonical price-history bitsets have been physically expired.
Never remove an active shop, an active product, a recent listing, or any SKU
still referenced by a retained native price observation. The original Raw
evidence is independently retained under the project's 60-day Raw policy.
"""
from datetime import datetime
from compact_checkpoint import binary_uuid
import json


def _expired(value, cutoff):
    if not isinstance(value, str) or not value:
        return False
    try:
        at=datetime.fromisoformat(value)
    except ValueError:
        return False
    if at.tzinfo is None or at.utcoffset() is None:
        return False
    return at < cutoff


def prune_expired_entities(state):
    cutoff=datetime.fromisoformat(state.meta["price_history_cutoff"])
    if cutoff.tzinfo is None:
        raise ValueError("Price retention cutoff must be timezone-aware")
    removed_stores=0
    removed_products=0
    protected_quotes=0
    for sid in list(state.ids()):
        part=state.lookup[sid]
        store,products,quotes=part.read_shop(sid)
        evidence={q[2] for q in quotes}
        protected_quotes+=len(quotes)
        if (store.get("status")!="active"
            and _expired(store.get("last_seen"),cutoff)
            and not evidence):
            # Both materialized records must be deleted atomically.
            with part.conn:
                part.conn.execute("DELETE FROM price_history WHERE store=?",
                                  (binary_uuid(sid),))
                part.conn.execute("DELETE FROM shop_state WHERE store=?",
                                  (binary_uuid(sid),))
            part.ids.remove(sid)
            del state.lookup[sid]
            removed_stores+=1
            removed_products+=len(products)
            continue
        keep=[item for item in products if
              item.get("status")=="active" or
              not _expired(item.get("last_seen"),cutoff) or
              item.get("product_uuid") in evidence]
        if len(keep)==len(products):
            continue
        removed_products+=len(products)-len(keep)
        # Per-store historical masks use local SKU ordinals. put_shop rebuilds
        # those ordinals from the remaining immutable UUIDs, retaining exact
        # original native observations for every kept product.
        part.put_shop(store,keep,quotes)
    if removed_stores or removed_products:
        state.flush()
        for part in state.parts:
            if part.conn.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
                raise ValueError("Expired-identity pruning corrupted checkpoint")
            part.conn.execute("VACUUM")
    result={"retention_days":60,"removed_inactive_stores":removed_stores,
            "removed_inactive_products":removed_products,
            "preserved_observations":protected_quotes,
            "remaining_stores":len(state.lookup)}
    print("FOODRADAR_EXPIRED_IDENTITY_PRUNE="+json.dumps(result),flush=True)
    return result
