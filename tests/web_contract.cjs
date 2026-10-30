// V2 browser contract: native numeric evidence is the only price source.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('web/app.js', 'utf8');
function extract(name) {
  const start = source.indexOf('function ' + name + '(');
  assert.ok(start >= 0, name + ' exists');
  const begin = source.indexOf('{', start);
  let depth=0, end=begin;
  for (; end<source.length; ++end) {
    if (source[end] === '{') ++depth;
    else if (source[end] === '}' && --depth === 0) break;
  }
  assert.ok(end<source.length);
  return source.slice(start, end+1);
}
const context = vm.createContext({});
context.window = context;
vm.runInContext(fs.readFileSync('web/data-contract.js', 'utf8'), context);
for (const name of ['formatTaiwanDiscount','productPromo','isPlatformPromotion','isVerifiedPriceDeal','isAnyPromotion','displayUnitPrice','matchesProductKeyword']) {
  vm.runInContext(extract(name), context);
}
const run=(n,...args)=>context[n](...args);
const verified = (price, effective, promo, discount=0)=>({
  price, effective_price:effective, eff_price:effective, promo_type:promo,
  quantity: promo==='買1送1' ? 2 : 1, verified_discount_pct:discount,
  price_evidence_status:'VERIFIED_MENU'
});
const raw = (price, promo)=>({
  price, promo_type:promo, quantity:2, price_evidence_status:'UNVERIFIED'
});
assert.equal(run('matchesProductKeyword',{store_name:'漢堡王',product_name:'華堡',category_name:'漢堡'},'漢堡王 華堡'),true);
assert.equal(run('matchesProductKeyword',{store_name:'COSTCO',product_name:'Chocolate Cookie'},'Costco cookie'),true);
assert.equal(run('matchesProductKeyword',{store_name:'漢堡王',product_name:'華堡'},'漢堡王 薯條'),false);
assert.equal(run('displayUnitPrice',verified(60,30,'買1送1',50)),30);
assert.equal(run('displayUnitPrice',verified(71,71,'直接折價',10.12658)),71);
assert.equal(run('displayUnitPrice',verified(89,44.5,'買1送1',50)),44.5);
assert.equal(run('displayUnitPrice',raw(100,'買1送1')),0,'unverified text does not imply discount');
assert.equal(run('displayUnitPrice',raw(100,'買A送B')),0,'BOGA may not be halved');
assert.equal(run('isPlatformPromotion',verified(60,30,'買1送1',50)),true);
assert.equal(run('isPlatformPromotion',verified(71,71,'直接折價',10.13)),true);
assert.equal(run('isPlatformPromotion',raw(100,'買1送1')),false);
assert.equal(run('isPlatformPromotion',verified(50,25,'買A送B',50)),false,
  'even a forged verified BOGA must not enter promotions');
assert.equal(run('isPlatformPromotion',verified(50,50,'無',0)),false);
assert.equal(run('productPromo',verified(60,30,'買1送1',50)).effPrice,30);
assert.equal(run('productPromo',verified(60,30,'買1送1',50)).discountText,'折扣率 50.00%');
assert.equal(run('productPromo',raw(100,'BOGO')).isPromo,false);
assert.equal(run('isVerifiedPriceDeal',{...verified(30,30,'直接折價',40),is_price_drop:1}),true);
assert.equal(run('isVerifiedPriceDeal',{...raw(30,'3折'),is_price_drop:1}),false);
assert.equal(run('isAnyPromotion',verified(60,30,'買1送1',50)),true);
assert.equal(run('isAnyPromotion',raw(60,'BOGO')),false);
assert.equal(context.UBER_DATA_CONTRACT.unitPrice(100,'買1送1',2),null,
  'legacy text-based price calculator must be disabled');
assert.equal(context.UBER_DATA_CONTRACT.unitMoney(44.5),'$44.50');
assert.equal(context.UBER_DATA_CONTRACT.compareUnitPrices,undefined);
const packed = fs.readFileSync('scripts/prototype_pack_db.py','utf8');
assert.ok(packed.includes('product["price_evidence_status"] == "VERIFIED_MENU"'),
  'packed promotion index must be native evidence only');
const reader = fs.readFileSync('web/packed-turso.js','utf8');
assert.ok(!reader.includes('window.UBER_DATA_CONTRACT.unitPrice('));
assert.ok(reader.includes('verified_discount_pct'));
assert.ok(reader.includes('price_evidence_status'));
assert.ok(!source.includes('calculateEffectivePromo('));
assert.ok(!source.includes('window.UBER_DATA_CONTRACT.unitPrice('));
assert.ok(source.includes('client.packedCurrentProduct(storeUuid, productId)'));
assert.ok(source.includes('const prices = history.map(h => Number(h.eff_price))'));
const money = context.UBER_DATA_CONTRACT.money;
for (const [n, expected] of [[10.3,'$10'],[75.99,'$76'],[88.87,'$89'],[114.63,'$115'],[1000.5,'$1,001']]) {
  assert.equal(money(n),expected);
}
console.log('V2 numeric verified pricing / promotions web contract: PASS');
