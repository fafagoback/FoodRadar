"""Write public discount/promotions/history from V2 certified SQLite only.

Never publish cached Json/Parquet heuristic prices as seven-day price drops.
"""
import json
import sqlite3
from pathlib import Path


def export_verified_public(source_db, site_directory, batch):
    site=Path(site_directory)
    site.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(source_db) as conn:
        conn.row_factory=sqlite3.Row
        now=conn.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()[0]
        if now != batch:
            raise ValueError("V2 public export batch mismatch")
        rows=conn.execute("""SELECT p.*,s.name AS store_name,s.address,s.city,s.locality,
               s.rating,s.review_count,s.order_url AS store_order_url
            FROM products p JOIN stores s ON s.store_uuid=p.store_uuid
            WHERE p.last_seen=s.last_seen
              AND s.last_seen=(SELECT MAX(processed_at) FROM crawl_batches)
              AND p.status='active' AND s.status='active'
              AND p.is_open=1 AND s.is_open=1 AND p.delivery_available=1
              AND p.price_evidence_status='VERIFIED_MENU'
              AND p.menu_quote_effective_price>0
        """).fetchall()
        discounts, promotions = [], []
        for p in rows:
            base={
                "store_uuid":p["store_uuid"],"product_uuid":p["product_uuid"],
                "store_id":p["store_uuid"],"product_id":p["product_uuid"],
                "store_name":p["store_name"],"product_name":p["product_name"],
                "category_name":p["category"],"description":p["description"],
                "price":p["price"],"quantity":p["quantity"],
                "promo_type":p["promo_type"],
                "effective_price":p["menu_quote_effective_price"],
                "eff_price":p["menu_quote_effective_price"],
                "price_evidence_status":"VERIFIED_MENU",
                "verified_discount_pct":p["verified_discount_pct"],
                "source_hash":p["price_evidence_source_hash"],
                "order_action_url":p["order_url"] or p["store_order_url"],
                "rating_value":p["rating"],"review_count":p["review_count"],
                "locality":p["locality"],"street_address":p["address"],
                "city":p["city"],"crawled_time":batch,"is_open":1,
            }
            if p["promo_type"]=="直接折價" or (
                str(p["promo_type"] or "").startswith("買")
                and "送" in str(p["promo_type"])):
                if float(p["verified_discount_pct"] or 0)>0:
                    promo=dict(base)
                    promo.update({
                        "discount_rate":round(float(p["verified_discount_pct"]),2),
                        "discount_pct":round(float(p["verified_discount_pct"]),2)
                    })
                    promotions.append(promo)
            if p["seven_day_is_price_deal"]==1 and float(p["seven_day_discount_pct"] or 0)>=30:
                item=dict(base)
                item.update({
                    "original_price":p["seven_day_reference_price"],
                    "current_price":p["menu_quote_effective_price"],
                    "discount_pct":p["seven_day_discount_pct"],
                    "savings_amount":p["seven_day_discount_amount"],
                    "is_price_drop":True,"seven_day_is_price_deal":True,
                })
                discounts.append(item)
        discounts.sort(key=lambda p:(p["discount_pct"],p["savings_amount"]),reverse=True)
        promotions.sort(key=lambda p:p["discount_rate"],reverse=True)
        for name,items in (("discounts.json",discounts),("promotions.json",promotions)):
            (site/name).write_text(json.dumps({
                "status":"success","schema_version":2,
                "total":len(items),"items":items,
            },ensure_ascii=False,separators=(",",":")),encoding="utf-8")
        # The old Parquet-derived price history is unverified and must not escape
        # into web fallbacks. The live history chart loads V2 Turso quotes.
        (site/"history.json").write_text(json.dumps({
            "status":"success","schema_version":2,"history":{},
        },ensure_ascii=False,separators=(",",":")),encoding="utf-8")
        stats=site/"stats.json"
        info=json.loads(stats.read_text(encoding="utf-8")) if stats.exists() else {}
        info.update({"schema_version":2,"big_discounts_count":len(discounts),
                     "promotions_count":len(promotions),
                     "max_savings_twd":round(max((p["savings_amount"] for p in discounts),default=0))})
        stats.write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding="utf-8")
        return {"discounts":len(discounts),"promotions":len(promotions),
                "verified_live_products":len(rows)}
