// Regression coverage for cross-page pricing and promotions.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('web/app.js', 'utf8');
function extract(name) {
  const start = source.indexOf('function ' + name + '(');
  assert.ok(start >= 0, name + ' is present');
  const brace = source.indexOf('{', start);
  let level = 0, end = brace;
  for (; end < source.length; ++end) {
    if (source[end] === '{') ++level;
    else if (source[end] === '}' && --level === 0) break;
  }
  assert.ok(end < source.length);
  return source.slice(start, end + 1);
}
const context = vm.createContext({});
context.window = context;
vm.runInContext(fs.readFileSync('web/data-contract.js', 'utf8'), context);
for (const name of ['formatTaiwanDiscount','calculateEffectivePromo','isPlatformPromotion','isVerifiedPriceDeal','isAnyPromotion','displayUnitPrice']) {
  vm.runInContext(extract(name), context);
}
const run = (name, ...args) => context[name](...args);
const deal = (price, pct, savings) => ({price, current_price: price, discount_pct: pct, savings_amount: savings});
assert.equal(run('isVerifiedPriceDeal', deal(168, 34.12, 87)), true);
assert.equal(run('isVerifiedPriceDeal', deal(168, 34.12, 0)), false);
assert.equal(run('isVerifiedPriceDeal', deal(168, 20, 87)), false);
assert.equal(run('isAnyPromotion', deal(168, 34.12, 87)), true);
assert.equal(run('isAnyPromotion', {price: 200, quantity: 2, promo_type: ''}), false, 'plain multipacks are not offers');
assert.equal(run('isPlatformPromotion', {price: 200, quantity: 2, promo_type: ''}), false);
assert.equal(run('isPlatformPromotion', {price: 100, promo_type: '買一送一'}), true);
assert.equal(run('displayUnitPrice', {price: 168, promo_type: '7折', quantity: 1}), 168, 'already discounted payable price is not discounted twice');
assert.equal(run('displayUnitPrice', {price: 168, promo_type: '買一送一', quantity: 1}), 84);
assert.equal(run('displayUnitPrice', {price: 100, promo_type: '第2件半價', quantity: 1}), 75);
assert.equal(run('displayUnitPrice', {price: 100, promo_type: '', quantity: 2}), 50);
assert.equal(run('displayUnitPrice', {price: 89, promo_type: '買1送1', quantity: 2}), 44.5);
assert.equal(run('displayUnitPrice', {price: 89, promo_type: 'BOGO', quantity: 1}), 44.5);
assert.equal(run('calculateEffectivePromo', 89, 'BOGO', 1).effPrice, 44.5);
assert.equal(context.UBER_DATA_CONTRACT.unitMoney(44.5), '$44.5', 'derived unit cost retains fractional TWD');
assert.equal(run('displayUnitPrice', {price: 55, promo_type: '無', quantity: 1}), 55);
assert.equal(Math.round((55 / 44.5 - 1) * 1000) / 10, 23.6,
  '89 buy-one-get-one followed by 55 no promotion is a 23.6% price INCREASE');
assert.equal(run('isVerifiedPriceDeal', {current_price:55,discount_pct:0,savings_amount:0}),false,
  'promotion ended is not a verified discount');
assert.equal(run('calculateEffectivePromo', 100, '', 2).discountText, '', 'multipack must not show a fake discount badge');
const packed = fs.readFileSync('scripts/prototype_pack_db.py', 'utf8');
assert.ok(!packed.includes('int(product["quantity"] or 1) > 1 or product["promo_type"]'), 'bundled products must not be indexed as platform promotions');
const reader = fs.readFileSync('web/packed-turso.js', 'utf8');
assert.ok(reader.includes('const effective = window.UBER_DATA_CONTRACT.unitPrice(price, product.promo_type, product.quantity);'),
  'packed reader uses actual unit cost, not raw list price or a legacy double-discount');

assert.ok(reader.includes('Number(product.is_open) === 1 && Number(store.is_open) === 1'), 'both product and store must explicitly be open');
assert.ok(source.includes('state.is_open === 1 || state.is_open === true'), 'price-history events require explicit open status');
assert.ok(source.includes('(found.is_open === 1 || found.is_open === true)'), 'fallback price must not bypass open-state validation');

const moneyContext = vm.createContext({});
vm.runInContext(fs.readFileSync('web/data-contract.js', 'utf8'), moneyContext);
const money = moneyContext.UBER_DATA_CONTRACT.money;
for (const [input, expected] of [
  [10.3, '$10'],
  [75.99, '$76'],
  [88.87, '$89'],
  [114.63, '$115'],
  [140.39, '$140'],
  [191.91, '$192'],
  [1000.5, '$1,001'],
  [168, '$168'],
]) {
  assert.equal(money(input), expected, 'Taiwan-facing price must match Uber integer display');
}
assert.equal(run('displayUnitPrice', {price: 114.63, promo_type: '', quantity: 1}), 114.63,
  'the source price must be preserved for sorting and historical comparisons');
assert.ok(source.includes('const prices = history.map(h => Number(h.eff_price))'),
  'history graph must retain accurate fractional effective unit prices');
assert.ok(source.includes('window.UBER_DATA_CONTRACT.money(h.eff_price)'),
  'history table and tooltip must use shared TWD formatting');
console.log('FoodRadar pricing and cross-page promotion contract: PASS');
