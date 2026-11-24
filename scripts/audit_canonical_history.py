"""Compare all existing visible chart points against canonical native evidence.

Read-only acceptance test against the real source checkpoint, not a deployment.
"""
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime,timedelta
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from canonical_history import quote_points
from serving_state import canonical_hash

def main(path):
    source=Path(path).resolve()
    with sqlite3.connect(source.as_uri()+"?mode=ro",uri=True) as db:
        db.row_factory=sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        stats=defaultdict(int)
        points=defaultdict(dict)
        for sid,pid,point in quote_points(db):
            at,unit,menu,promo,count=point
            points[(sid,pid)][at]=[unit,menu,promo,count]
            stats["canonical_points"]+=1
        for row in db.execute("SELECT * FROM products"):
            key=(row["store_uuid"],row["product_uuid"])
            history=json.loads(row["menu_quote_history"] or "[]")
            prices=points.get(key,{})
            for point in history:
                stats["legacy_quote_points"]+=1
                if len(point)>=5 and point[0] in prices and point[1:5]==prices[point[0]]:
                    stats["legacy_quote_points_match"]+=1
                else:
                    stats["legacy_quote_points_differ"]+=1
            # Inactive or absent products may retain last-seen, not global-window history.
            cutoff=(datetime.fromisoformat(row["last_seen"])-timedelta(days=7)).isoformat()
            expected=[[at,*p] for at,p in sorted(prices.items()) if cutoff<=at<=row["last_seen"]]
            stats["products"]+=1
            if history==expected:
                stats["derived_history_exact"]+=1
            else:
                stats["derived_history_exception"]+=1
            state={k:row[k] for k in ("product_name","category","description","price","quantity","promo_type","effective_price","order_url","is_open")}
            # Python float/int encoding matters to the historical state hash.
            if canonical_hash(state)==row["state_hash"]:
                stats["derived_product_hash_exact"]+=1
            else:
                stats["derived_product_hash_exception"]+=1
        for row in db.execute("""SELECT store_uuid,product_uuid,event_time,event_type,new_state,old_state
                FROM events WHERE product_uuid IS NOT NULL AND event_type IN
                ('NEW','PRICE_OBSERVED','PRICE_CHANGED','PROMOTION_CHANGED')"""):
            try:
                state=json.loads(row["new_state"] or row["old_state"] or "{}")
            except ValueError:
                stats["invalid_event_json"]+=1
                continue
            if state.get("price_evidence_status")!="VERIFIED_MENU":
                continue
            if state.get("menu_quote_valid") is not True and not (state.get("price_valid") is True and state.get("is_open") in (True,1)):
                continue
            if not isinstance(state.get("price"),(float,int)) or state["price"]<=0:
                continue
            if not isinstance(state.get("effective_price"),(float,int)) or state["effective_price"]<=0:
                continue
            stats["browser_eligible_event_points"]+=1
            expected=points.get((row["store_uuid"],row["product_uuid"]),{}).get(row["event_time"])
            if expected is None:
                stats["browser_event_missing_canonical"]+=1
            elif abs(expected[0]-float(state["effective_price"]))<=.011 and expected[1]==float(state["price"]):
                stats["browser_event_matches_canonical"]+=1
            else:
                stats["browser_event_differs_canonical"]+=1
        result=dict(stats)
        print("CANONICAL_HISTORY_AUDIT="+json.dumps(result,ensure_ascii=False),flush=True)
        Path("canonical-history-audit.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
        return result

if __name__=="__main__":
    main(sys.argv[1])
