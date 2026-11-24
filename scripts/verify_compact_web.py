"""End-to-end offline production-sized static build from a compact DB checkpoint.

Never uploads raw data, never dispatches deployment, never publishes a website.
"""
from __future__ import annotations
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"scripts"))
from compact_checkpoint import create, expand
from build_web_db import build_web_db
from build_static_catalog import build,verify
from prepare_pages import prepare

source=Path(sys.argv[1]).resolve()
root=ROOT/"data"/"compact-offline-acceptance"
root.mkdir(parents=True,exist_ok=True)
compact=root/"state.compact.db"
expanded=root/"ephemeral-working.db"
packed=root/"packed-working.db"
static=root/"static-public"
started=time.perf_counter()
size=create(source,compact)
if size["bytes"]>=20_000_000:
    raise ValueError("Compact checkpoint exceeded 20MB")
decoded=expand(compact,expanded)
if any(decoded[k]!=size[k] for k in ("stores","products","observations")):
    raise ValueError("Expanded checkpoint changed store/product/price counts")
# Compare website-relevant flags *before* any new snapshot is applied.
fields=("store_uuid","product_uuid","price_evidence_status","verified_discount_pct",
        "seven_day_reference_price","seven_day_discount_pct","seven_day_is_price_deal",
        "status","is_open","listing_started_at","first_seen","last_seen","delivery_available")
select="SELECT "+",".join(fields)+" FROM products ORDER BY store_uuid,product_uuid"
with sqlite3.connect(source) as before,sqlite3.connect(expanded) as after:
    a=before.execute(select)
    b=after.execute(select)
    verified=0
    while True:
        x,y=a.fetchmany(10000),b.fetchmany(10000)
        if x!=y:
            raise ValueError(f"Published product certification changed after {verified} items")
        verified+=len(x)
        if not x: break
build_web_db(expanded,packed)
manifest=build(packed,expanded,static)
checked=verify(static)
if checked["counts"]!=manifest["counts"]:
    raise ValueError("Invalid static data counts")
if manifest["counts"]["stores"]!=size["stores"]:
    raise ValueError("Static store count mismatch")
if manifest["counts"]["products"]<=0:
    raise ValueError("No searchable product cards")
result=prepare(ROOT/"web",static,commit="compact-acceptance")
assert result["batch"]==manifest["batch"]
site_manifest=json.loads((ROOT/"web"/"data"/"manifest.json").read_text())
assert site_manifest["counts"]==manifest["counts"]
assert site_manifest["geo"]["cell_degrees"]==0.025
report={"checkpoint_bytes":size["bytes"],"stores":size["stores"],"products":size["products"],
        "verified_price_observations":size["observations"],
        "audited_product_flags":verified,
        "site_cards":manifest["counts"]["products"],"site_bytes":manifest["bytes"],
        "geo_cells":len(site_manifest["geo"]["products"]),
        "seconds":round(time.perf_counter()-started,2)}
print("COMPACT_WEB_OFFLINE_ACCEPTANCE="+json.dumps(report,ensure_ascii=False),flush=True)
with open("compact-acceptance.json","w",encoding="utf-8") as file:
    json.dump(report,file,ensure_ascii=False,indent=2)
