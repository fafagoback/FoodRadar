// Price-history regression: same-second price and content changes are NOT two prices.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

async function main() {
  const moduleSource = fs.readFileSync('web/packed-turso.js', 'utf8');
  const client = await import('data:text/javascript;base64,' + Buffer.from(moduleSource).toString('base64'));
  const normalize = client.normalizePriceHistoryEvents;
  assert.equal(typeof normalize, 'function');

  const state = (price, more = {}) => JSON.stringify({
    price, effective_price: price, is_open: 1, price_valid: true, promo_type: '無', quantity: 1, ...more
  });
  const event = (id, event_type, event_time, price, old_state = null) => ({
    id, event_type, event_time, old_state, new_state: state(price)
  });

  // Matches the actual event IDs and price transitions inspected in Turso.
  const t0 = '2026-10-07T00:52:06+08:00';
  const t1 = '2026-10-14T23:15:43+08:00';
  const t2 = '2026-10-17T21:49:28+08:00';
  const t3 = '2026-10-20T01:03:44+08:00';
  const t4 = '2026-10-20T23:13:43+08:00';
  const old = (price, last_seen) => state(price, { last_seen, first_seen: t0 });
  const events = [
    event(13148, 'NEW', t0, 250),
    event(88908, 'PRICE_CHANGED', t1, 175, old(250,t0)),
    event(88909, 'CONTENT_CHANGED', t1, 175, old(250,t0)),
    event(111740, 'CONTENT_CHANGED', t2, 175, old(175,t1)),
    event(137714, 'PRICE_CHANGED', t3, 250, old(175,t2)),
    event(137715, 'CONTENT_CHANGED', t3, 250, old(175,t2)),
    event(142140, 'PRICE_CHANGED', t4, 175, old(250,t3)),
    event(142141, 'CONTENT_CHANGED', t4, 175, old(250,t3))
  ];

  const normalized = normalize(events);
  assert.deepEqual(normalized.map(e => e.id), [13148,88908,137714,142140],
    '8 database events become 4 unique actual price observations');
  assert.deepEqual(normalized.map(e => JSON.parse(e.new_state).price), [250,175,250,175]);
  assert.deepEqual(normalized.map(e => e.event_time), [t0,t1,t3,t4]);

  const withoutNew = normalize(events.filter(e => e.id !== 13148));
  assert.equal(withoutNew.length, 4, 'baseline added only if NEW was not retained');
  assert.equal(withoutNew[0].event_type, 'BASELINE');
  assert.equal(withoutNew[0].event_time, t0);
  assert.equal(normalized.some(e => e.event_type === 'BASELINE'), false, 'no duplicate baseline when NEW exists');

  // Same listed price can have a different effective unit price as promotions start/end.
  const promotionChange = event(142143, 'PROMOTION_CHANGED', '2026-10-21T09:00:00+08:00', 89);
  promotionChange.new_state = state(89, {promo_type:'買1送1', quantity:2, effective_price:44.5});
  const withPromotion = normalize([...events, promotionChange]);
  assert.equal(withPromotion.length, 5, 'promotion-only unit-cost change must appear in the chart');
  assert.equal(withPromotion[4].event_type, 'PROMOTION_CHANGED');

  const duplicatePrice = normalize([...events, event(142142, 'PRICE_CHANGED', t4, 175)]);
  assert.equal(duplicatePrice.length, 4, 'same-second duplicate price events are collapsed');
  assert.equal(normalize([...events, event(143333,'PRICE_CHANGED','2026-10-21T00:00:00+08:00',0)]).length,4,
    'zero-price events do not create observations');
  assert.equal(normalize([...events, event(143334,'PRICE_CHANGED','2026-10-21T01:00:00+08:00',300)]).length, 5);
  const closed = event(143335,'PRICE_CHANGED','2026-10-21T01:01:00+08:00',300);
  closed.new_state = state(300, {price_valid:false});
  assert.equal(normalize([...events,closed]).length,4, 'closed-hours events never enter price graph');

  const app = fs.readFileSync('web/app.js', 'utf8');
  const start = app.indexOf('function formatBatchDate(');
  assert.ok(start >= 0);
  const open = app.indexOf('{', start);
  let level = 0, end = open;
  for (; end < app.length; end++) {
    if (app[end] === '{') level++;
    else if (app[end] === '}' && --level === 0) break;
  }
  assert.ok(end < app.length);
  const ctx = vm.createContext({});
  vm.runInContext(app.slice(start, end + 1), ctx);
  assert.equal(ctx.formatBatchDate(t3), '2026/10/20 01:03');
  assert.equal(ctx.formatBatchDate(t4), '2026/10/20 23:13');
  assert.equal(ctx.formatBatchDate('20261020010344'), '2026/10/20 01:03');
  assert.equal(ctx.formatBatchDate('2026-10-20T15:13:43Z'), '2026/10/20 23:13');
  assert.ok(app.includes('const rowsHtml = [...history].reverse().map(h =>'),
    'history table newest first');
  assert.ok(app.includes('const uniqueHistory = new Map()'),
    'table and graph share a deduplicated set');
  assert.ok(app.includes("const dayKey = value => formatBatchDate(value)"),
    'chart labels use formatted Taiwan dates');
  console.log('Price history deduplication, baseline, date formatting, and table order: PASS');
}
main().catch(err => { console.error(err); process.exitCode=1; });
