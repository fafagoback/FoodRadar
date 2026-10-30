"""Deprecated V1 archive repair is not permitted to create V2 price claims.

Old Json-only archives discarded getStoreV1 native promotion metadata.
They cannot substantiate paid/free quantities or direct strikethrough prices.
V2 reconstruction is exclusively handled by raw_price_enrichment + verified_weekly.
"""


def canonical_menu_quotes(doc, sid):
    """No legacy Json-only input produces price-proof observations."""
    return {}


def apply_archival_quote_references(conn, latest_batch, historical_documents):
    """Explicitly invalidate legacy inferred archive deal fields."""
    from serving_state import ensure_schema_and_migrations
    ensure_schema_and_migrations(conn)
    conn.execute("""UPDATE products SET archive_reference_price=NULL,
      archive_discount_pct=0,archive_discount_amount=0,
      archive_evidence_count=0,archive_is_price_deal=0""")
    conn.commit()
    return {"archive_quote_candidates":0,"archive_evidence_count":0}


def restore_archive_price_references(db_path, config, current_batch):
    raise ValueError(
        "V1 Json archive prices are uncertified; rebuild V2 from HF ApiRaw")


if __name__=="__main__":
    raise SystemExit(
        "V1 archive repair disabled: use V2 HF ApiRaw reconstruction")
