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
let bundleSelects = 0, locationSelects = 0;
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
  console.log('PASS: concurrent bundle reads and precise location-cache invalidation');
})().catch(err => {console.error(err); process.exitCode=1;});
