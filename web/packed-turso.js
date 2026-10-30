let runtimePromise;
const bucketCache = new Map();
const bundleCache = new Map();
// Deduplicate concurrent bundle requests; decoding a store twice corrupts chunk offsets.
const pendingBundleLoads = new Map();
let cacheEpoch = 0;
const encoder = new TextEncoder();
let locationStoresCache = { key: '', storeIds: new Set(), stores: [] };
let releaseMeta = {};

function cfg() { return window.UBER_RADAR_CONFIG || {}; }
function menuCount(storeId) {
  const chunks = bundleCache.get(Number(storeId));
  if (!chunks) return null;
  let cols, count = 0;
  for (const chunk of chunks) {
    cols = chunk.product_columns || cols;
    for (const [,values] of chunk.products || []) {
      const product = Object.fromEntries(cols.map((key,i) => [key,values[i]]));
      if (product.status === 'active') count++;
    }
  }
  return count;
}
function normalize(value) { return String(value || '').normalize('NFKC').toLocaleLowerCase().replace(/\s+/g, ''); }

function queryTokens(value) {
  return String(value || '').trim().split(/\s+/).filter(Boolean).flatMap(part => {
    const s = normalize(part);
    if (s.length >= 3) return Array.from({length: s.length - 2}, (_, i) => `t:${s.slice(i, i + 3)}`);
    if (s.length === 2) return [`b:${s}`];
    return s ? [`u:${s}`] : [];
  });
}

function matchesWords(haystack, query) {
  const text = normalize(haystack);
  return String(query || '').trim().split(/\s+/).filter(Boolean).every(word => text.includes(normalize(word)));
}

async function runtime() {
  if (!runtimePromise) runtimePromise = Promise.all([
    import('https://cdn.jsdelivr.net/npm/fzstd@0.1.1/+esm'),
    import('https://cdn.jsdelivr.net/npm/@msgpack/msgpack@3.1.2/+esm'),
    import('https://cdn.jsdelivr.net/npm/@noble/hashes@1.7.1/blake2s.js/+esm')
  ]).then(([zstd, msgpack, hashes]) => ({
    decompress: zstd.decompress,
    decode: msgpack.decode,
    blake2s: hashes.blake2s
  }));
  return runtimePromise;
}

function blobBytes(cell) {
  let b64 = (cell.base64 || '').replace(/-/g, '+').replace(/_/g, '/');
  while (b64.length % 4) b64 += '=';
  const raw = atob(b64);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
  return out;
}

async function sql(statement) {
  const c = cfg();
  const url = String(c.TURSO_DATABASE_URL || '').replace(/\/+$/, '');
  const response = await fetch(`${url}/v2/pipeline`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${c.TURSO_READONLY_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      requests: [
        { type: 'execute', stmt: { sql: statement } },
        { type: 'close' }
      ]
    })
  });
  if (!response.ok) throw new Error(`Turso HTTP ${response.status}`);
  const body = await response.json();
  const first = body.results?.[0];
  if (first?.type === 'error') throw new Error(first.error?.message || 'Turso query failed');
  const result = first?.response?.result;
  if (!result) return [];
  const names = result.cols.map(c => c.name);
  return result.rows.map(row => Object.fromEntries(row.map((cell, i) => [names[i], cell.type === 'blob' ? blobBytes(cell) : cell.value])));
}

async function unpack(payload, checksum) {
  const r = await runtime();
  const raw = r.decompress(payload);
  if (checksum) {
    const digest = await crypto.subtle.digest('SHA-256', raw);
    const hex = Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, '0')).join('');
    if (hex !== checksum) throw new Error('Packed payload checksum mismatch');
  }
  return r.decode(raw);
}

async function bucketId(term, count) {
  const { blake2s } = await runtime();
  const d = blake2s(encoder.encode(term), { dkLen: 4 });
  return (((d[0] * 0x1000000) + (d[1] << 16) + (d[2] << 8) + d[3]) >>> 0) % count;
}

let latestMetadataReadAt = 0;
let pendingMetadata = null;
export async function metadata() {
  if (latestMetadataReadAt && Date.now() - latestMetadataReadAt < 15000) return releaseMeta;
  if (pendingMetadata) return pendingMetadata;
  pendingMetadata = (async () => {
  const rows = await sql('select key,value from metadata');
  const meta = Object.fromEntries(rows.map(r => {
    try { return [r.key, JSON.parse(r.value)]; }
    catch { return [r.key, r.value]; }
  }));
  // Replayed HF RAW may change prices without changing the latest batch identifier.
  const previousRevision = releaseMeta.data_revision || releaseMeta.source_revision || releaseMeta.latest_batch;
  const nextRevision = meta.data_revision || meta.source_revision || meta.latest_batch;
  if (previousRevision && previousRevision !== nextRevision) {
    cacheEpoch++;
    bucketCache.clear(); bundleCache.clear(); pendingBundleLoads.clear();
    webIndexState=null;webIndexLastChecked=0;pageResultsCache.clear();totalResultsCache.clear();
    locationStoresCache = { key: '', storeIds: new Set(), stores: [] };
  }
  releaseMeta = meta;
  latestMetadataReadAt = Date.now();
  return meta;
  })();
  try { return await pendingMetadata; }
  finally { pendingMetadata = null; }
}

async function fetchBuckets(bucketIds) {
  const missing = [...new Set(bucketIds)].filter(id => !bucketCache.has(id));
  if (missing.length === 0) return;
  const epoch = cacheEpoch;
  for (let i = 0; i < missing.length; i += 60) {
    const ids = missing.slice(i, i + 60);
    const rows = await sql(`select bucket_id, payload, checksum from search_buckets where bucket_id in (${ids.join(',')})`);
    for (const r of rows) {
      const bucket = r.payload ? await unpack(r.payload, r.checksum) : {};
      if (epoch === cacheEpoch) bucketCache.set(Number(r.bucket_id), bucket);
    }
  }
}

async function posting(term, count) {
  const id = await bucketId(term, count);
  if (!bucketCache.has(id)) {
    await fetchBuckets([id]);
  }
  return (bucketCache.get(id) || {})[term] || [];
}

function intersect(groups) {
  if (!groups.length) return [];
  groups.sort((a, b) => a.length - b.length);
  let result = new Set(groups[0]);
  for (const g of groups.slice(1)) {
    const s = new Set(g);
    result = new Set([...result].filter(x => s.has(x)));
  }
  return [...result];
}

export function distanceKm(a, b, c, d) {
  const R = 6371.0088, p = Math.PI / 180;
  const x = (c - a) * p, y = (d - b) * p;
  const q = Math.sin(x / 2) ** 2 + Math.cos(a * p) * Math.cos(c * p) * Math.sin(y / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(q));
}
function locationBounds(lat, radiusKm) {
  // SQL rectangles only pre-filter candidates; the precise distance check is authoritative.
  const dy = (radiusKm * 1.05) / 110.5;
  const maxLat = Math.min(89, Math.abs(lat) + dy);
  const dx = (radiusKm * 1.05) / Math.max(1, 111.32 * Math.cos(maxLat * Math.PI / 180));
  return { dy, dx };
}

function inLocation(store, location) {
  if (!location?.enabled) return true;
  const lat = Number(store.latitude), lon = Number(store.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  store.distance_km = distanceKm(Number(location.latitude), Number(location.longitude), lat, lon);
  return store.distance_km <= Number(location.radiusKm);
}

function dedupeStores(stores) {
  const seen = new Set();
  const out = [];
  for (const s of stores) {
    const k = String(s.store_uuid || s.store_id);
    if (!seen.has(k)) {
      seen.add(k);
      out.push(s);
    }
  }
  return out;
}

function dedupeProducts(products) {
  const seen = new Set();
  const out = [];
  for (const p of products) {
    const k = `${p.store_uuid || p.store_id}\x1f${p.product_uuid || p.product_id}`;
    if (!seen.has(k)) {
      seen.add(k);
      out.push(p);
    }
  }
  return out;
}

export function isCurrentMenuObservation(product, store, latestProcessedAt = releaseMeta.latest_processed_at) {
  const latest = String(latestProcessedAt || '');
  return Boolean(latest && String(store?.last_seen || '') === latest &&
    String(product?.last_seen || '') === latest &&
    Number(store?.is_open) === 1 && Number(product?.is_open) === 1 &&
    product?.price_evidence_status === 'VERIFIED_MENU' &&
    Number(product?.menu_quote_effective_price) > 0);
}

export function isCurrentPriceObservation(product, store, latestProcessedAt = releaseMeta.latest_processed_at) {
  // Verified price/deliverability is stricter than a current menu quote.
  const latest = String(latestProcessedAt || '');
  return Boolean(latest &&
    String(store?.last_seen || '') === latest &&
    String(product?.last_seen || '') === latest &&
    Number(store?.is_open) === 1 && Number(product?.is_open) === 1 &&
    Number(product?.price) > 0 &&
    product?.price_evidence_status === 'VERIFIED_MENU' &&
    Number(product?.delivery_available ?? 1) === 1);
}

function productView(product, store) {
  const menuCurrent = isCurrentMenuObservation(product, store);
  const quoted = Number(product.menu_quote_price || 0);
  const price = quoted > 0 ? quoted : Number(product.price || 0);
  const promo = quoted > 0 ? (product.menu_quote_promo_type || product.promo_type) : product.promo_type;
  const quantity = quoted > 0 ? Number(product.menu_quote_quantity || 1) : Number(product.quantity || 1);
  // V2: price comes from native numeric evidence. Never parse promo labels
  // in the browser or halve a BOGA price by guessing it is BOGO.
  const verified = product.price_evidence_status === 'VERIFIED_MENU';
  const effective = verified && Number(product.menu_quote_effective_price) > 0
    ? Number(product.menu_quote_effective_price) : null;
  // Derived change data comes only from Python PriceChange, persisted in Turso.
  // This read adapter must not recompare reference_price with effective.
  const refPrice = Number(product.reference_price || 0) > 0 ? Number(product.reference_price) : null;
  const direction = product.price_direction || 'unknown';
  // Only a validated product in the latest batch can be a current deal.
  // Price change decisions still belong exclusively to Python PriceChange.
  const isCurrent = isCurrentPriceObservation(product, store);
  const weekReference = Number(product.seven_day_reference_price || 0) > 0
    ? Number(product.seven_day_reference_price) : null;
  const weekPct = menuCurrent ? Number(product.seven_day_discount_pct || 0) : 0;
  const currentDrop = menuCurrent && verified && effective !== null
    && weekPct > 0 && weekReference > effective;
  const originalPrice = currentDrop ? weekReference : effective;
  const historicalSaving = Number(product.seven_day_discount_amount || 0);
  return {
    product_id: product.product_uuid,
    product_uuid: product.product_uuid,
    store_id: product.store_uuid,
    store_uuid: product.store_uuid,
    store_name: store.name || store.store_name,
    product_name: product.product_name,
    category_name: product.category,
    description: product.description,
    price,
    quantity,
    promo_type: promo,
    eff_price: effective,
    effective_price: effective,
    verified_discount_pct: verified ? Number(product.verified_discount_pct || 0) : 0,
    price_evidence_status: verified ? 'VERIFIED_MENU' : 'UNVERIFIED',
    order_action_url: product.order_url || store.order_url,
    rating_value: store.rating || store.rating_value,
    review_count: Number(store.review_count || 0),
    locality: store.locality,
    street_address: store.address || store.street_address,
    city: window.UBER_DATA_CONTRACT.city(store.address || store.street_address, store.city),
    latitude: Number(store.latitude),
    longitude: Number(store.longitude),
    distance_km: store.distance_km ?? null,
    crawled_time: product.last_seen || releaseMeta.latest_processed_at,
    first_seen: product.first_seen,
    listing_started_at: product.listing_started_at || product.first_seen,
    original_price: originalPrice,
    reference_price: refPrice,
    seven_day_reference_price: weekReference,
    is_new_price_drop: isCurrent && Number(product.is_price_drop || 0) === 1,
    is_verified_delivery_deal: currentDrop && isCurrent,
    archive_reference_price: isCurrent ? Number(product.archive_reference_price || 0) : 0,
    archive_discount_pct: isCurrent ? Number(product.archive_discount_pct || 0) : 0,
    archive_savings_amount: isCurrent ? Number(product.archive_discount_amount || 0) : 0,
    archive_evidence_count: isCurrent ? Number(product.archive_evidence_count || 0) : 0,
    archive_is_price_deal: isCurrent ? Number(product.archive_is_price_deal || 0) : 0,
    current_price: effective,
    price_direction: direction,
    price_change_amount: Number(product.price_change_amount || 0),
    price_change_percent: Number(product.price_change_percent || 0),
    is_price_drop: currentDrop ? 1 : 0,
    discount_pct: currentDrop ? weekPct : 0,
    savings_amount: currentDrop ? historicalSaving : 0,
    is_price_deal: currentDrop ? Number(product.seven_day_is_price_deal || 0) : 0,
    is_current_price_observation: isCurrent,
    is_current_menu_observation: menuCurrent,
    is_menu_catalog_available: price > 0,
    delivery_available: menuCurrent ? isCurrent : null,
    status: product.status,
    is_open: (Number(product.is_open) === 1 && Number(store.is_open) === 1) ? 1 : 0
  };
}

async function bundlesForStores(storeIds) {
  const requested = [...new Set(storeIds.map(Number))].filter(Number.isInteger);
  const missing = requested.filter(id => !bundleCache.has(id) && !pendingBundleLoads.has(id));
  for (let i = 0; i < missing.length; i += 60) {
    const ids = missing.slice(i, i + 60);
    const epoch = cacheEpoch;
    const loader = (async () => {
      const rows = await sql(`select store_id, chunk_no, payload, checksum from store_bundles where store_id in (${ids.join(',')}) order by store_id, chunk_no`);
      const grouped = new Map(ids.map(id => [id, []]));
      for (const row of rows) {
        const sid = Number(row.store_id);
        if (grouped.has(sid)) grouped.get(sid).push(await unpack(row.payload, row.checksum));
      }
      // Ignore an obsolete fetch if a newer release invalidated this cache.
      if (epoch === cacheEpoch) {
        for (const [sid, chunks] of grouped) bundleCache.set(sid, chunks);
      }
    })();
    for (const id of ids) pendingBundleLoads.set(id, loader);
    loader.finally(() => {
      for (const id of ids) {
        if (pendingBundleLoads.get(id) === loader) pendingBundleLoads.delete(id);
      }
    }).catch(() => {}); // The original caller handles the load error.
  }
  await Promise.all(requested.map(id => pendingBundleLoads.get(id)).filter(Boolean));
  return requested.flatMap(id => bundleCache.get(id) || []);
}

export async function getStoresInLocation(location) {
  if (!location?.enabled) return null;
  const meta = await metadata();
  const latest = String(meta.latest_processed_at || '').replace(/'/g, "''");
  if (!latest) return { key: '', storeIds: new Set(), stores: [] };
  const key = JSON.stringify([meta.latest_batch, Number(location.latitude), Number(location.longitude), Number(location.radiusKm)]);
  if (locationStoresCache.key === key) return locationStoresCache;
  const lat = Number(location.latitude), lon = Number(location.longitude), r = Number(location.radiusKm);
  const { dy, dx } = locationBounds(lat, r);
  const rows = await sql(`select * from store_directory where status='active' and latitude between ${lat - dy} and ${lat + dy} and longitude between ${lon - dx} and ${lon + dx} order by rating desc nulls last, review_count desc limit 50000`);
  const allStores = rows.map(r => ({
    store_id: Number(r.store_id),
    store_uuid: r.store_uuid,
    store_name: r.name,
    name: r.name,
    city: window.UBER_DATA_CONTRACT.city(r.address, r.city),
    locality: r.locality,
    rating_value: r.rating === null ? null : Number(r.rating),
    rating: r.rating === null ? null : Number(r.rating),
    review_count: Number(r.review_count || 0),
    total_menu_items: menuCount(r.store_id),
    latitude: Number(r.latitude),
    longitude: Number(r.longitude),
    street_address: r.address,
    address: r.address,
    order_action_url: r.order_url,
    first_seen: r.first_seen,
    last_seen: r.last_seen
  })).filter(s => inLocation(s, location));
  allStores.sort((a, b) => (a.distance_km || 0) - (b.distance_km || 0) || (b.rating || 0) - (a.rating || 0));
  const storeIds = new Set(allStores.map(s => s.store_id));
  const stores = dedupeStores(allStores);
  locationStoresCache = { key, storeIds, stores };
  return locationStoresCache;
}

async function productsFromRefs(refs, location, limit = 50000, accepts = () => true) {
  const wanted = new Map();
  for (const ref of refs) {
    const sid = Math.floor(Number(ref) / 1048576);
    const idx = Number(ref) % 1048576;
    if (!wanted.has(sid)) wanted.set(sid, new Set());
    wanted.get(sid).add(idx);
  }

  let storeIds = [...wanted.keys()];
  if (storeIds.length === 0) return [];

  const storeMetaMap = new Map();

  if (location?.enabled) {
    const lat = Number(location.latitude), lon = Number(location.longitude), r = Number(location.radiusKm);
    // 預先過濾外接矩形 (Bounding Box)，加 5% 安全裕度避免地表橢球曲率造成邊界誤切
    const { dy, dx } = locationBounds(lat, r);
    const validStoreIds = [];

    // 針對搜尋命中的候選店家直接進行座標與精確距離測算，不再盲取全區前 5000 家
    for (let i = 0; i < storeIds.length; i += 100) {
      const batch = storeIds.slice(i, i + 100);
      const rows = await sql(`select store_id, name, latitude, longitude, rating, review_count from store_directory where store_id in (${batch.join(',')}) and latitude between ${lat - dy} and ${lat + dy} and longitude between ${lon - dx} and ${lon + dx}`);
      for (const row of rows) {
        const sid = Number(row.store_id);
        const sLat = Number(row.latitude), sLon = Number(row.longitude);
        if (Number.isFinite(sLat) && Number.isFinite(sLon)) {
          const d = distanceKm(lat, lon, sLat, sLon);
          if (d <= r) {
            row.distance_km = d;
            storeMetaMap.set(sid, row);
            validStoreIds.push(sid);
          }
        }
      }
    }
    // 由近至遠排序，確保最近的門市排在最前面
    validStoreIds.sort((a, b) => (storeMetaMap.get(a)?.distance_km || 0) - (storeMetaMap.get(b)?.distance_km || 0));
    storeIds = validStoreIds;
  }

  const out = [];
  for (let i = 0; i < storeIds.length; i += 30) {
    const batch = storeIds.slice(i, i + 30);
    await bundlesForStores(batch);
    for (const sid of batch) {
      const indexes = wanted.get(sid);
      let store, cols, offset = 0;
      for (const chunk of bundleCache.get(sid) || []) {
        if (chunk.store) {
          store = Object.fromEntries(chunk.store_columns.map((c, j) => [c, chunk.store[j]]));
          cols = chunk.product_columns;
        }
        if (!store) {
          offset += (chunk.products || []).length;
          continue;
        }
        if (storeMetaMap.has(sid)) {
          store.distance_km = storeMetaMap.get(sid).distance_km;
        } else if (location?.enabled) {
          if (!inLocation(store, location)) {
            offset += (chunk.products || []).length;
            continue;
          }
        }
        for (const pair of chunk.products || []) {
          if (indexes.has(offset)) {
            const p = Object.fromEntries(cols.map((c, j) => [c, pair[1][j]]));
            if (store.status === 'active' && p.status === 'active' &&
                Number(p.menu_quote_price || p.price) > 0) {
              const view = productView(p, store);
              if (accepts(view)) out.push(view);
            }
            if (out.length >= limit) return dedupeProducts(out);
          }
          offset++;
        }
      }
    }
    if (out.length >= limit) break;
  }
  return dedupeProducts(out);
}

export async function directory(location, limit = 50000) {
  if (cfg().SINGLE_POINT) limit = -1;
  if (location?.enabled) {
    const locInfo = await getStoresInLocation(location);
    return locInfo.stores;
  }
  const meta = await metadata();
  const latest = String(meta.latest_processed_at || '').replace(/'/g, "''");
  if (!latest) return [];
  const active = meta.definition_version ? "and status='active'" : '';
  const rows = await sql(`select * from store_directory where name != '' ${active} order by rating desc nulls last, review_count desc limit ${limit}`);
  const stores = rows.map(r => ({
    store_id: Number(r.store_id),
    store_uuid: r.store_uuid,
    store_name: r.name,
    name: r.name,
    city: window.UBER_DATA_CONTRACT.city(r.address, r.city),
    locality: r.locality,
    rating_value: r.rating === null ? null : Number(r.rating),
    rating: r.rating === null ? null : Number(r.rating),
    review_count: Number(r.review_count || 0),
    total_menu_items: menuCount(r.store_id),
    latitude: Number(r.latitude),
    longitude: Number(r.longitude),
    street_address: r.address,
    address: r.address,
    order_action_url: r.order_url,
    first_seen: r.first_seen,
    last_seen: r.last_seen
  })).filter(s => inLocation(s, location));
  return dedupeStores(stores);
}

export async function searchStores({ keyword = '', location = null, newOnly = false, limit = 50000 } = {}) {
  if (cfg().SINGLE_POINT) limit = -1;
  const meta = await metadata();
  const latest = String(meta.latest_processed_at || '').replace(/'/g, "''");
  if (!latest) return [];
  const keywords = String(keyword || '').trim().split(/\s+/).map(normalize).filter(Boolean);
  let where = "name != '' and status='active'";
  for (const word of keywords) {
    // Each token must match, but may appear in a different store field.
    // instr avoids treating user-entered % and _ as SQL LIKE wildcards.
    const safe = word.replace(/'/g, "''");
    if (word.includes('costco') || word.includes('好市多')) {
      where += " and (instr(lower(name),'costco')>0 or instr(name,'好市多')>0)";
    } else if (word.includes('mcdonald') || word.includes('麥當勞')) {
      where += " and (instr(lower(name),'mcdonald')>0 or instr(name,'麥當勞')>0)";
    } else {
      where += ` and (instr(lower(name),'${safe}')>0 or instr(lower(locality),'${safe}')>0 or instr(lower(address),'${safe}')>0)`;
    }
  }
  if (location?.enabled) {
    const lat = Number(location.latitude), lon = Number(location.longitude), r = Number(location.radiusKm);
    const { dy, dx } = locationBounds(lat, r);
    where += ` and latitude between ${lat - dy} and ${lat + dy} and longitude between ${lon - dx} and ${lon + dx}`;
  }
  if (newOnly) {
    if (!meta.latest_processed_at || !meta.definition_version) return [];
    const anchor = new Date(meta.latest_processed_at || Date.now());
    anchor.setDate(anchor.getDate() - 7);
    const cutoff = anchor.toISOString().replace(/'/g, "''");
    const baseline = String(meta.baseline_processed_at || '').replace(/'/g, "''");
    where += ` and datetime(first_seen) >= datetime('${cutoff}') and substr(first_seen,1,10) > substr('${baseline}',1,10)`;
  }
  const rows = await sql(`select * from store_directory where ${where} order by rating desc nulls last, review_count desc limit ${limit}`);
  const stores = rows.map(r => ({
    store_id: Number(r.store_id),
    store_uuid: r.store_uuid,
    store_name: r.name,
    name: r.name,
    city: window.UBER_DATA_CONTRACT.city(r.address, r.city),
    locality: r.locality,
    rating_value: r.rating === null ? null : Number(r.rating),
    rating: r.rating === null ? null : Number(r.rating),
    review_count: Number(r.review_count || 0),
    total_menu_items: menuCount(r.store_id),
    latitude: Number(r.latitude),
    longitude: Number(r.longitude),
    street_address: r.address,
    address: r.address,
    order_action_url: r.order_url,
    first_seen: r.first_seen,
    last_seen: r.last_seen
  })).filter(s => inLocation(s, location));
  if (location?.enabled) {
    stores.sort((a, b) => (a.distance_km || 0) - (b.distance_km || 0) || (b.rating || 0) - (a.rating || 0));
  }
  return dedupeStores(stores);
}

async function sampleProducts(stores, location, limit = 50000) {
  const sids = stores.map(s => s.store_id);
  await bundlesForStores(sids);
  const out = [];
  for (const d of stores) {
    let store, cols;
    for (const chunk of bundleCache.get(d.store_id) || []) {
      if (chunk.store) {
        store = Object.fromEntries(chunk.store_columns.map((c, i) => [c, chunk.store[i]]));
        cols = chunk.product_columns;
      }
      if (!store || !inLocation(store, location)) continue;
      for (const pair of chunk.products || []) {
        const p = Object.fromEntries(cols.map((c, i) => [c, pair[1][i]]));
        if (store.status === 'active' && p.status === 'active' &&
            Number(p.menu_quote_price || p.price) > 0) out.push(productView(p, store));
        if (out.length >= limit) return dedupeProducts(out);
      }
    }
  }
  return dedupeProducts(out);
}

// Read-side, materialized SQL index: only requested cards leave Turso.
let webIndexState = null;
let webIndexLastChecked = 0;
const pageResultsCache = new Map();
const totalResultsCache = new Map();
const MAX_CACHED_PAGES = 40;
function escapeSqlText(value) { return String(value ?? '').replace(/'/g, "''"); }
function keywordClauses(value, field = 'search_text') {
  return String(value || '').trim().split(/\s+/).filter(Boolean)
    .map(word => `instr(${field},'${escapeSqlText(normalize(word))}') > 0`);
}
function locationSql(location, alias = '') {
  if (!location?.enabled) return '1=1';
  const lat=Number(location.latitude), lon=Number(location.longitude), r=Number(location.radiusKm);
  if (![lat,lon,r].every(Number.isFinite) || r <= 0) return '0=1';
  const name = n => alias ? `${alias}.${n}` : n;
  const a=name('latitude'),b=name('longitude');
  const {dy,dx}=locationBounds(lat,r);
  const rad=Math.PI/180;
  const term=`(pow(sin((${a}-${lat})*${rad}/2),2)+cos(${lat}*${rad})*cos(${a}*${rad})*pow(sin((${b}-${lon})*${rad}/2),2))`;
  return `(${a} BETWEEN ${lat-dy} AND ${lat+dy} AND ${b} BETWEEN ${lon-dx} AND ${lon+dx} AND 12742.0176*asin(sqrt(min(1,${term})))<=${r})`;
}
async function cachedPage(key, query) {
  const cacheKey=cacheEpoch+':'+key;
  if (pageResultsCache.has(cacheKey)) return pageResultsCache.get(cacheKey);
  const promise=query();
  pageResultsCache.set(cacheKey,promise);
  if (pageResultsCache.size>MAX_CACHED_PAGES) pageResultsCache.delete(pageResultsCache.keys().next().value);
  try { return await promise; } catch(e) { pageResultsCache.delete(cacheKey); throw e; }
}
async function cachedCount(where, table) {
  const key=`${cacheEpoch}:${table}:${where}`;
  if (totalResultsCache.has(key)) return totalResultsCache.get(key);
  const promise=sql(`SELECT count(*) AS total FROM ${table} WHERE ${where}`)
    .then(rows=>Number(rows[0]?.total||0));
  totalResultsCache.set(key,promise);
  try { return await promise; } catch(e) {totalResultsCache.delete(key);throw e;}
}
export async function hasWebIndex() {
  await metadata();
  if (webIndexState === true || (webIndexState === false && Date.now()-webIndexLastChecked<30000)) return webIndexState;
  const r=await sql("SELECT name FROM sqlite_master WHERE type='table' AND name='web_products'");
  webIndexState=Boolean(r.length);
  webIndexLastChecked=Date.now();
  return webIndexState;
}
// The row is the exact native-price view produced by the Python packer, never
// a browser-inferred price or promotional label.
export async function searchProductsPage({
  kind='global', page=1, pageSize=50, keyword='', city='', category='',
  minDiscount=0, minSavings=0, promoType='全部', sort='', location=null
}={}) {
  if (!await hasWebIndex()) return null;
  const conditions=[...keywordClauses(keyword),locationSql(location)];
  if (city && city !== '全部') conditions.push(`instr(coalesce(city,''),'${escapeSqlText(city)}')>0`);
  if (category && category !== '全部') conditions.push(`instr(coalesce(category,''),'${escapeSqlText(category)}')>0`);
  if (kind==='discounts') {
    conditions.push('price_drop=1',`discount_pct>=${Number(minDiscount)||0}`,`savings>=${Number(minSavings)||0}`);
  } else if (kind==='newProducts') conditions.push('new_item=1');
  else if (kind==='promos') {
    conditions.push('promo=1');
    if (promoType==='平台優惠') conditions.push('platform_promo=1');
    else if (promoType==='實質降價30') conditions.push('price_drop=1','discount_pct>=30');
    else if (promoType==='買一送一') conditions.push('platform_promo=1',"json_extract(card,'$.promo_type') LIKE '買%送%'");
    else if (promoType==='折') conditions.push('platform_promo=1',"instr(card,'直接折價')>0");
    else if (['半價','加1元','多入'].includes(promoType)) conditions.push('0=1');
  }
  const order = ({
    discount_desc:'discount_pct DESC,savings DESC',
    savings_desc:'savings DESC,discount_pct DESC',
    price_asc:'unit_price ASC,rating DESC',
    price_desc:'unit_price DESC,rating DESC',
    rating_desc:'rating DESC,unit_price ASC',
    name_asc:'product_uuid ASC',
    promo_first:'promo DESC,rating DESC'
  })[sort] || (kind==='discounts'?'discount_pct DESC,savings DESC':
    kind==='promos'?'unit_price ASC':kind==='newProducts'?'new_item DESC,rating DESC':'rating DESC,unit_price ASC');
  const where=conditions.join(' AND ');
  const size=Math.max(1,Math.min(50,Math.floor(Number(pageSize)||50)));
  const pageNo=Math.max(1,Math.floor(Number(page)||1));
  const key=JSON.stringify(['products',where,order,pageNo,size]);
  return cachedPage(key,async()=>{
    const [rows,total]=await Promise.all([
      sql(`SELECT card,latitude,longitude FROM web_products WHERE ${where} ORDER BY ${order},store_uuid,product_uuid LIMIT ${size} OFFSET ${(pageNo-1)*size}`),
      cachedCount(where,'web_products')
    ]);
    const products=rows.map(row=>{
      const product=JSON.parse(row.card);
      if (location?.enabled) product.distance_km=distanceKm(Number(location.latitude),Number(location.longitude),Number(row.latitude),Number(row.longitude));
      return product;
    });
    return {rows:products,total};
  });
}
export async function searchStoresPage({page=1,pageSize=50,keyword='',city='',newOnly=true,sort='rating_desc',location=null}={}) {
  const meta=await metadata();
  const clauses=["status='active'","name!=''",locationSql(location),...keywordClauses(keyword,"(lower(name)||' '||lower(coalesce(locality,''))||' '||lower(coalesce(address,'')))")];
  if (city && city!=='全部') clauses.push(`instr(coalesce(city,''),'${escapeSqlText(city)}')>0`);
  if (newOnly) {
    const cutoff=new Date(new Date(meta.latest_processed_at).getTime()-7*86400000).toISOString();
    clauses.push(`datetime(first_seen)>=datetime('${escapeSqlText(cutoff)}')`);
    clauses.push(`substr(first_seen,1,10)>substr('${escapeSqlText(meta.baseline_processed_at||'')}',1,10)`);
  }
  const where=clauses.join(' AND ');
  const order=({rating_desc:'rating DESC,review_count DESC',reviews_desc:'review_count DESC,rating DESC',name_asc:'name ASC',items_desc:'rating DESC'})[sort]||'rating DESC,review_count DESC';
  const size=Math.min(50,Math.max(1,Math.floor(Number(pageSize)||50)));
  const pageNo=Math.max(1,Math.floor(Number(page)||1));
  return cachedPage(JSON.stringify(['stores',where,order,pageNo,size]),async()=>{
    const [rows,total]=await Promise.all([
      sql(`SELECT * FROM store_directory WHERE ${where} ORDER BY ${order},store_uuid LIMIT ${size} OFFSET ${(pageNo-1)*size}`),
      cachedCount(where,'store_directory')
    ]);
    return {total,rows:rows.map(r=>({
      store_id:Number(r.store_id),store_uuid:r.store_uuid,store_name:r.name,name:r.name,
      city:r.city,locality:r.locality,rating_value:r.rating===null?null:Number(r.rating),
      rating:r.rating===null?null:Number(r.rating),review_count:Number(r.review_count||0),
      total_menu_items:menuCount(r.store_id),latitude:Number(r.latitude),longitude:Number(r.longitude),
      street_address:r.address,address:r.address,order_action_url:r.order_url,
      first_seen:r.first_seen,last_seen:r.last_seen,
      distance_km:location?.enabled?distanceKm(Number(location.latitude),Number(location.longitude),Number(r.latitude),Number(r.longitude)):null
    }))};
  });
}
export async function fastSummary() {
  if (!await hasWebIndex()) return null;
  const meta=await metadata();
  const cutoff=new Date(new Date(meta.latest_processed_at).getTime()-7*86400000).toISOString();
  const conditions=`status='active' AND name!='' AND datetime(first_seen)>=datetime('${escapeSqlText(cutoff)}') AND substr(first_seen,1,10)>substr('${escapeSqlText(meta.baseline_processed_at||'')}',1,10)`;
  const [stores,newProducts,promotions,deals]=await Promise.all([
    cachedCount(conditions,'store_directory'),cachedCount('new_item=1','web_products'),
    cachedCount('promo=1','web_products'),
    sql('SELECT COALESCE(MAX(savings),0) AS maximum FROM web_products WHERE price_drop=1')
  ]);
  return {stores,newProducts,promotions,maxSavings:Number(deals[0]?.maximum||0)};
}

export async function loadPackedDashboard(location) {
  const meta = await metadata();
  const stores = await directory(location, 50000);
  const products = await searchPacked({ location, limit: 50000 });
  return { meta, stores, products };
}

export async function searchPacked({ keyword = '', city = '', promo = false, newOnly = false, minDiscount = null, minArchiveDiscount = null, location = null, limit = 50000 } = {}) {
  if (cfg().SINGLE_POINT) limit = Infinity;
  const meta = await metadata();
  if (newOnly && !meta.definition_version) return [];
  const count = Number(meta.buckets || 8192);
  const terms = [...queryTokens(keyword), ...queryTokens(city)];
  // The promotions view includes platform-labelled offers and verified unit-price drops.
  if (promo) terms.push('f:promo');
  if (newOnly) terms.push('f:new');
  const bIds = await Promise.all(terms.map(t => bucketId(t, count)));
  await fetchBuckets(bIds);

  const groups = [];
  for (const term of terms) {
    groups.push((bucketCache.get(await bucketId(term, count)) || {})[term] || []);
  }

  if (minDiscount !== null) {
    const minBand = Math.floor(Number(minDiscount) / 5);
    const discTerms = [];
    for (let n = minBand; n <= 20; n++) discTerms.push(`f:discount:${n}`);
    const discBids = await Promise.all(discTerms.map(t => bucketId(t, count)));
    await fetchBuckets(discBids);
    const union = new Set();
    for (const t of discTerms) {
      const bid = await bucketId(t, count);
      for (const ref of (bucketCache.get(bid) || {})[t] || []) union.add(ref);
    }
    groups.push([...union]);
  }

  if (minArchiveDiscount !== null) {
    const minBand = Math.floor(Number(minArchiveDiscount) / 5);
    const quoteTerms = [];
    for (let n = minBand; n <= 20; n++) quoteTerms.push(`f:archive_discount:${n}`);
    await fetchBuckets(await Promise.all(quoteTerms.map(t => bucketId(t, count))));
    const quoteRefs = new Set();
    for (const t of quoteTerms) {
      const bid = await bucketId(t, count);
      for (const ref of (bucketCache.get(bid) || {})[t] || []) quoteRefs.add(ref);
    }
    groups.push([...quoteRefs]);
  }
  if (!groups.length) {
    const catalog = await posting('f:catalog', count);
    if (!catalog.length && !meta.definition_version) return sampleProducts(await directory(location, 50), location, Math.min(limit, 2000));
    groups.push(catalog);
  }

  const k = normalize(keyword), c = normalize(city);
  const accepts = p =>
    p.status === 'active' && Number(p.is_open) === 1 &&
    p.is_menu_catalog_available === true &&
    (!k || matchesWords(`${p.product_name}${p.store_name}${p.category_name}`, keyword)) &&
    (!c || matchesWords(`${p.city}${p.locality}${p.street_address}`, city)) &&
    (minDiscount === null || Number(p.discount_pct || 0) >= Number(minDiscount)) &&
    (minArchiveDiscount === null || Number(p.archive_discount_pct || 0) >= Number(minArchiveDiscount)) &&
    (!newOnly || (String(p.listing_started_at || p.first_seen || '').slice(0,10) > String(meta.baseline_processed_at || '').slice(0,10)));
  return productsFromRefs(intersect(groups), location, limit, accepts);

}

// History charts show one observed price per crawl, not one row per
// PRICE_CHANGED / CONTENT_CHANGED event emitted by the same crawl.
export function normalizePriceHistoryEvents(events) {
  // A promotion change can change unit cost even when the raw menu price is stable.
  const priority = { MENU_QUOTE: 5, BASELINE: 0, NEW: 1, PRICE_OBSERVED: 2, PROMOTION_CHANGED: 3, PRICE_CHANGED: 4 };
  const priceEvents = (events || []).filter(e =>
    Object.prototype.hasOwnProperty.call(priority, e.event_type));

  // Some older products have no NEW event: reconstruct the pre-change price
  // only when there is no real price observation at that timestamp.
  const firstChange = priceEvents.find(e => e.event_type === 'PRICE_CHANGED' && e.old_state);
  if (firstChange) {
    try {
      const old = JSON.parse(firstChange.old_state);
      const oldTime = old.last_seen || old.first_seen;
      // Never reconstruct a baseline from a later off-hours observation.
      if (old.price_valid === true && oldTime && oldTime === old.first_seen &&
          !priceEvents.some(e => String(e.event_time) === String(oldTime))) {
        priceEvents.push({
          event_time: oldTime, new_state: firstChange.old_state,
          old_state: null, event_type: 'BASELINE'
        });
      }
    } catch (_) {
      // Malformed archived state cannot add a synthetic baseline.
    }
  }

  const byTime = new Map();
  for (const event of priceEvents) {
    const time = String(event.event_time || '');
    if (!time) continue;
    let state;
    try { state = JSON.parse(event.new_state || event.old_state || '{}'); }
    catch (_) { continue; }
    if (state.price_valid !== true && state.menu_quote_valid !== true) continue;
    if (state.menu_quote_valid !== true && state.is_open !== 1 && state.is_open !== true) continue;
    if (!(Number(state.price) > 0)) continue;
    const previous = byTime.get(time);
    // Prefer complete original HF evidence over sparse event snapshots.
    // A legacy unit-only quote is lower confidence than a verified event.
    const rank = event.event_type === 'MENU_QUOTE' && state.effective_unit_only === true
      ? 0 : priority[event.event_type];
    let priorRank = -1;
    if (previous) {
      const priorState = JSON.parse(previous.new_state || previous.old_state || '{}');
      priorRank = previous.event_type === 'MENU_QUOTE' &&
        priorState.effective_unit_only === true ? 0 : priority[previous.event_type];
    }
    if (!previous || rank > priorRank) {
      byTime.set(time, event);
    }
  }
  return [...byTime.values()]
    .sort((a, b) => String(a.event_time).localeCompare(String(b.event_time)))
    .slice(-50);
}

// Resolve the actual current comparison; change-event history omits unchanged crawls.
export async function packedCurrentProduct(storeUuid, productUuid) {
  await metadata(); // Same batch can be rebuilt, invalidating any prior bundle.
  const escaped = String(storeUuid).replace(/'/g, "''");
  const row = (await sql(`select store_id from store_directory where store_uuid='${escaped}' limit 1`))[0];
  if (!row) return null;
  const sid = Number(row.store_id);
  await bundlesForStores([sid]);
  let store = null, cols = null;
  for (const chunk of bundleCache.get(sid) || []) {
    if (chunk.store) {
      store = Object.fromEntries(chunk.store_columns.map((name, index) => [name, chunk.store[index]]));
      cols = chunk.product_columns;
    }
    if (!store || !cols) continue;
    for (const pair of chunk.products || []) {
      const item = Object.fromEntries(cols.map((name, index) => [name, pair[1][index]]));
      if (String(item.product_uuid) === String(productUuid)) {
        if (store.status !== 'active' || item.status !== 'active' ||
            !(Number(item.menu_quote_price || item.price) > 0)) return null;
        return productView(item, store);
      }
    }
  }
  return null;
}

export async function packedHistory(storeUuid, productUuid) {
  await metadata(); // Check dataset revision even when opening a modal directly.
  const row = (await sql(`select store_id from store_directory where store_uuid='${String(storeUuid).replace(/'/g, "''")}' limit 1`))[0];
  if (!row) return [];
  await bundlesForStores([Number(row.store_id)]);
  const events = [];
  let cols, productCols;
  for (const chunk of bundleCache.get(Number(row.store_id)) || []) {
    cols = chunk.event_columns || cols;
    productCols = chunk.product_columns || productCols;
    for (const pair of chunk.products || []) {
      if (!productCols) continue;
      const product = Object.fromEntries(productCols.map((c, i) => [c, pair[1][i]]));
      if (String(product.product_uuid) !== String(productUuid)) continue;
      let quotes = [];
      try { quotes = JSON.parse(product.menu_quote_history || '[]'); }
      catch (_) { quotes = []; }
      for (const point of quotes) {
        if (!Array.isArray(point) || point.length < 2) continue;
        const [at, unit, rawPrice, promoType, count] = point;
        if (!at || !(Number(unit) > 0)) continue;
        const hasRawEvidence = point.length >= 5 &&
          Number(rawPrice) > 0 && String(promoType || '').trim() !== '';
        // The packed history carries exact original RAW list price + promo
        // when present, rather than inventing a $29 menu item from a $58 BOGO.
        events.push({event_time:at, event_type:'MENU_QUOTE',
          new_state:JSON.stringify({
            price:hasRawEvidence ? Number(rawPrice) : Number(unit),
            quantity:hasRawEvidence ? Number(count || 1) : 1,
            promo_type:hasRawEvidence ? promoType : null,
            effective_unit_only:!hasRawEvidence,
            effective_price:Number(unit),
            price_evidence_status:'VERIFIED_MENU',
            menu_quote_valid:true,price_valid:false,
            delivery_available:false
          })});
      }
    }
    for (const event of chunk.events || []) {
      if (!cols) continue;
      const e = Object.fromEntries(cols.map((c, i) => [c, event[i]]));
      if (String(e.product_uuid) !== String(productUuid)) continue;
      events.push(e);
    }
  }
  return normalizePriceHistoryEvents(events);
}
