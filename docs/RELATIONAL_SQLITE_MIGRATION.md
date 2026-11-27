# FoodRadar shared relational SQLite experiment (FR + FRT)

Status: **opt-in feature branch, not deployed; both crawlers remain disabled.**

## Goal

One public HF SQLite dataset for serving shop/item/price/search queries; a separate
60-day HF Raw archive. No third persistent website product dataset.

## Real data layout

- stores: one row per Uber store UUID; coordinates and lifecycle stay per store.
- products: one row for identical immutable product text (name/category/description).
  The key is SHA-256 (128-bit truncated). This is physical text sharing,
  **not** a claim that two branches have identical source SKUs.
- store_products: one row per (store, Uber SKU); stores each keep their
  own current price pointer, status, first/last seen and all listing-specific state.
- price_terms: one row per identical exact monetary/offer tuple, so branches
  with identical prices can reference the same integer ID.
- price_observations: one row per observed store/SKU/batch (60 days);
  this NEVER merges branches or loses independent price evidence.
- SQLite FTS5 virtual search index and R-Tree geospatial index. These are
  indexes inside the one SQLite database, **not a second product dataset**.

Names alone are NOT used to merge SKU identity, prices or histories.
An individual store's Uber item ID remains authoritative for its lifecycle.

## Offline build (NO UE requests, NO upload)

First restore the prior verified HF checkpoint with the existing HF restore
script on a trusted runner with HF_TOKEN. Then:

    python scripts/build_relational_db.py --checkpoint data/state-checkpoint \
      --output data/relational-candidate.db \
      --report data/relational-report.json

This validates the verified v4 checkpoint manifest, partition hashes,
shop and SKU counts, verified observation conservation, SQLite integrity
and foreign keys before producing the immutable candidate. It cannot overwrite
an existing candidate, old compact checkpoint or production website data.

    python -m unittest discover -s tests -p test_relational_catalog.py -v

## Browser access gate

Do NOT point the production website at HF merely because SQLite WASM can
execute queries. sql.js-httpvfs requires a readable remote byte-range source.

    python scripts/probe_hf_sqlite.py \
      https://huggingface.co/datasets/ORG/REPO/resolve/main/DB/.../catalog.db \
      --origin https://fafagoback.github.io

The probe requires HTTP 206, Content-Range, correct CORS and exposed headers.
A real browser test must additionally pass page-byte bounds, query parity,
cold and warm search latency, latency P95 and concurrent-user HF throttling.

Hugging Face Xet/CDN redirects may block browser Range/CORS. If that fails,
do not remove existing Pages data or expose any privileged HF tokens in JS.
A separate proxy or another read protocol would need a new review.

## Rollout safety

1. FR offline candidate, compare actual byte size against current v4 compact
   checkpoint and existing Pages blobs. This must include indexes, price history
   and all retained auxiliary fields; synthetic ratios are NOT production proof.
2. FR browser end-to-end test (keyword, city, geo, nearby, all products,
   discounts, store lookup, history, ABA suppression) and cold/warm latency.
3. Only after passing, review main/test deployment and then FRT pin.
4. Do not resume either FR or FRT crawlers without a separate explicit request.
5. Retire old HF compact checkpoint and Pages data only after remote DB is
   proven complete and the rollback window is finished.

A guarded two-phase HF publisher is available as scripts/publish_relational_db.py:
It uploads only with --upload, verifies the remote byte length, and switches
DB/current.json only after staging an immutable version. Do not pass --upload
until browser and production volume tests are accepted. A browser-side proof module is available at web/hf-sqlite-client.js with
metadata, product, nearby-store and history SQL queries, and a mocked Node
regression test. It is NOT wired into the main website and the WASM/worker
assets are not bundled. Real HF/CORS and production latency tests remain open.
This is NOT a replacement for FR/FRT production yet.
