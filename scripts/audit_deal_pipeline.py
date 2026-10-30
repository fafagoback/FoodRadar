"""Independent read-only audit of V2 native numeric seven-day comparisons."""
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from fractions import Fraction
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from verified_weekly import _two_decimals


def report(path):
    with sqlite3.connect(path) as conn:
        conn.row_factory=sqlite3.Row
        batch=conn.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()[0]
        now=datetime.strptime(batch,"%Y%m%d%H%M%S").replace(tzinfo=timezone(timedelta(hours=8)))
        now_iso=now.isoformat()
        cutoff=(now-timedelta(days=7)).isoformat()
        previous=defaultdict(list)
        current={}
        for row in conn.execute("""SELECT store_uuid,product_uuid,batch_id,observed_at,
                payable_total_cents,received_quantity,promotion_type
            FROM product_price_observations
            WHERE observed_at>=? AND observed_at<=?""",(cutoff,now_iso)):
            assert row["promotion_type"] in ("NONE","SAME_SKU_BXGY","DIRECT_DISCOUNT")
            cost=Fraction(row["payable_total_cents"],100*row["received_quantity"])
            key=(row["store_uuid"],row["product_uuid"])
            if row["observed_at"]==now_iso and row["batch_id"]==batch:
                current[key]=cost
            elif row["observed_at"]<now_iso:
                previous[key].append(cost)
        inspected=0
        matched_deals=0
        errors=[]
        for row in conn.execute("""SELECT p.*,s.is_open AS store_is_open
            FROM products p JOIN stores s ON s.store_uuid=p.store_uuid"""):
            key=(row["store_uuid"],row["product_uuid"])
            eligible=(row["status"]=="active" and row["is_open"]==1
                 and row["store_is_open"]==1 and row["delivery_available"]==1
                 and row["price_evidence_status"]=="VERIFIED_MENU"
                 and row["last_seen"]==now_iso and key in current)
            prior=previous.get(key,[])
            expected_ref=None
            expected_pct=expected_savings=0
            expected_deal=0
            if eligible and prior:
                maximum=max(prior)
                current_cost=current[key]
                difference=max(Fraction(0),maximum-current_cost)
                expected_ref=_two_decimals(maximum)
                expected_pct=_two_decimals(100*difference/maximum)
                expected_savings=_two_decimals(difference)
                expected_deal=int(difference>0 and expected_pct>=30)
            expected=(expected_ref,expected_pct,expected_savings,expected_deal)
            actual=(row["seven_day_reference_price"],row["seven_day_discount_pct"],
                    row["seven_day_discount_amount"],row["seven_day_is_price_deal"])
            inspected+=1
            if expected_deal:
                matched_deals+=1
            if not all(
                (a is None and b is None) or
                (a is not None and b is not None and abs(float(a)-float(b))<=.0101)
                for a,b in zip(actual,expected)
            ):
                errors.append({
                    "store_uuid":row["store_uuid"],
                    "product_uuid":row["product_uuid"],
                    "expected":expected,"actual":actual,
                })
                if len(errors)>=12:
                    break
        result={
            "batch":batch,"audited_products":inspected,
            "eligible_comparison_count":sum(1 for k in current if previous.get(k)),
            "certified_7day_30pct_deals":matched_deals,
            "errors":errors,
        }
        print("V2_DEAL_PIPELINE_AUDIT="+json.dumps(result,ensure_ascii=False),flush=True)
        if errors:
            raise ValueError("V2 published seven-day discounts do not match native numeric HF quotes")
        return result


if __name__=="__main__":
    if len(sys.argv)!=2:
        raise SystemExit("Usage: audit_deal_pipeline.py path/to/serving.db")
    report(sys.argv[1])
