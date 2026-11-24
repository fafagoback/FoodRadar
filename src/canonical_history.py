"""One authoritative price observation stream for browser histories.

Raw native quote amounts are integral cents. Calculate the effective unit
price from the payable total and received quantity, never from event snapshots.
"""
from fractions import Fraction

PROMOTIONS = {"NONE": "無", "DIRECT_DISCOUNT": "直接折價", "SAME_SKU_BXGY": None}

def quote_points(conn):
    """Stream all verified observations ordered by stable SKU and time."""
    yield from points_from_rows(conn.execute("""SELECT store_uuid,product_uuid,observed_at,
            menu_price_cents,promotion_type,buy_quantity,free_quantity,
            received_quantity,payable_total_cents
            FROM product_price_observations
            ORDER BY store_uuid,product_uuid,observed_at,batch_id"""))

def points_from_rows(rows):
    for row in rows:
        sid,pid,at,menu,kind,buy,free,received,payable=row
        if type(menu) is not int or menu <= 0 or type(received) is not int or received <= 0:
            raise ValueError("Invalid verified native quote")
        if type(payable) is not int or payable < 0 or kind not in PROMOTIONS:
            raise ValueError("Invalid verified payable price")
        if payable == 0:
            # The existing browser chart only accepts strictly positive values.
            continue
        label = PROMOTIONS[kind]
        if kind=="SAME_SKU_BXGY":
            label=f"買{buy}送{free}"
        yield sid,pid,[at,round(float(Fraction(payable,100*received)),2),menu/100.0,label,received]
