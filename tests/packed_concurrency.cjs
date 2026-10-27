// Race-regression tests for packed Turso bundle loads and exact location caches.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const source = fs.readFileSync('web/packed-turso.js', 'utf8');
const stubbed = source.replace(
  /async function unpack\(payload, checksum\) \{[\s\S]*?\n\}/,
  'async function unpack(payload) { return JSON.parse(new TextDecoder().decode(payload)); }'
);
assert.notEqual(stubbed, source, 'test stub must replace unpack');

globalThis.window = {UBER_RADAR_CONFIG: {SINGLE_POINT: true}, UBER_DATA_CONTRACT: {}};
globalThis.atob = globalThis.atob || (s => Buffer.from(s, 'base64').toString('binary'));
const value = JSON.stringify({price: 100, effective_price: 100, is_open: 1, price_valid: true});
const blob = {
  event_columns: ['product_uuid', 'id', 'event_type', 'event_time', 'old_state', 'new_state'],
  events: [['p', 1, 'NEW', '2026-10-22T12:00:00+08:00', null, value]]
};
let bundleSelects = 0, locationSelects = 0, lastStoreSearch = '', lastLocationSearch = '';
function result(rows) {
  const cols = Object.keys(rows[0] || {}).map(name => ({name}));
  return {results: [{type:'ok', response:{result:{cols, rows:rows.map(r =>
    cols.map(c => typeof r[c.name] === 'object' && r[c.name].base64
      ? {type:'blob', base64:r[c.name].base64}
      : {type:typeof r[c.name] === 'number' ? 'integer':'text',value:r[c.name]})
  )}}}]};
}
globalThis.fetch = async (_url, opts) => {
  const sql = JSON.parse(opts.body).requests[0].stmt.sql;
  if (sql.includes('from metadata')) {
    return {ok:true,json:async()=>result([
      {key:'latest_processed_at',value:JSON.stringify('2026-10-27T14:46:28+08:00')},
      {key:'latest_batch',value:JSON.stringify('20261027144628')}
    ])};
  }
  if (sql.includes('from store_bundles')) {
    bundleSelects++;
    await new Promise(resolve => setTimeout(resolve, 20)); // force both callers to overlap
    const bytes = Buffer.from(JSON.stringify(blob)).toString('base64');
    return {ok:true,json: async()=>result([{store_id:4,chunk_no:0,payload:{base64:bytes},checksum:'unused'}])};
  }
  if (sql.includes('select store_id from store_directory')) {
    return {ok:true,json:async()=>result([{store_id:4}])};
  }
  if (sql.includes('from store_directory') && sql.includes('latitude between')) {
    locationSelects++;
    lastLocationSearch = sql;
    return {ok:true,json:async()=>result([])};
  }
  if (sql.includes('from store_directory') && sql.includes('status=')) {
    lastStoreSearch = sql;
    return {ok:true,json:async()=>result([])};
  }
  throw Error('Unexpected SQL: '+sql);
};
(async () => {
  const reader = await import('data:text/javascript;base64,' + Buffer.from(stubbed).toString('base64'));
  const [first,second] = await Promise.all([reader.packedHistory('s','p'),reader.packedHistory('s','p')]);
  assert.equal(bundleSelects, 1, 'parallel reads must share a single store-bundle fetch');
  assert.equal(first.length,1);
  assert.equal(second.length,1);
  const pos1={enabled:true,latitude:25.123240,longitude:121.529520,radiusKm:5};
  const pos2={enabled:true,latitude:25.123249,longitude:121.529520,radiusKm:5};
  await reader.getStoresInLocation(pos1);
  await reader.getStoresInLocation(pos1);
  await reader.getStoresInLocation(pos2);
  assert.equal(locationSelects,2,'distinct input coordinates cannot share a rounded cache key');
  const bound = lastLocationSearch.match(/latitude between ([\d.-]+) and ([\d.-]+)/);
  assert.ok(bound,'location filter query must have a latitude bounding box');
  assert.ok(Number(bound[2]) - Number(bound[1]) > 2 * 5 / 111.32,
    'geospatial SQL prefilter must include edge locations for exact distance checks');
  await reader.searchStores({keyword:'好市多 天母'});
  assert.match(lastStoreSearch,/instr\(lower\(name\),'costco'\)/);
  assert.match(lastStoreSearch,/instr\(lower\(name\),'天母'\)/);
  assert.equal((lastStoreSearch.match(/ and \(/g) || []).length,2,
    'two whitespace-delimited keywords must each constrain the store search');
  await reader.searchStores({keyword:'100%_特價'});
  assert.ok(lastStoreSearch.includes('100%_特價'), 'search symbols must remain literal');
  console.log('PASS: concurrent bundle reads and precise location-cache invalidation');
})().catch(err => {console.error(err); process.exitCode=1;});
