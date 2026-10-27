// Live GitHub Pages browser acceptance test. Never against local fixtures.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const errors = [];
  try {
    page.on('pageerror', error => errors.push(String(error)));
    const url = process.env.FOODRADAR_SITE || 'https://fafagoback.github.io/FoodRadar/';
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 90000 });
    // Verify the ACTUALLY DEPLOYED GitHub Pages release, not just the CI build.
    const manifest = await page.evaluate(async () => {
      const response = await fetch('version.json?_t=' + Date.now(), {cache:'no-store'});
      if (!response.ok) throw new Error('Live version.json HTTP ' + response.status);
      return response.json();
    });
    const expectedCommit = process.env.GITHUB_SHA || '';
    assert.ok(expectedCommit, 'GitHub Actions must provide GITHUB_SHA');
    assert.equal(manifest.commit, expectedCommit, 'Live Pages are not running the tested commit');
    assert.ok(String(manifest.version || '').includes(expectedCommit.slice(0,12)),
      'Live version ID must change when the source commit changes');
    console.log('LIVE_PAGES_RELEASE_VERIFIED ' + JSON.stringify(manifest));
    await page.waitForFunction(() => {
      const value = document.querySelector('#stat-big-discounts')?.textContent?.trim();
      return value && /^\d[\d,]*$/.test(value);
    }, null, { timeout: 90000 });

    const homeText = (await page.locator('#stat-big-discounts').innerText()).trim();
    assert.match(homeText, /^\d[\d,]*$/, 'dashboard must display a numeric count');
    const home = Number(homeText.replace(/,/g, ''));
    const coverage = (await page.locator('#discounts-coverage').innerText()).trim();
    assert.match(coverage, /本批有效商品 [\d,]+ 件，其中 [\d,]+ 件有前次有效單價可比較/,
      'dashboard must disclose the valid comparison denominator, not only a zero');
    console.log('DEAL_COMPARISON_COVERAGE ' + coverage);

    // The promotions filter belongs to a hidden tab; activate it as a real user does.
    const promotionsTab = page.locator('.nav-tab[data-target="tab-promos"]');
    await promotionsTab.click();
    const filter = page.locator('#promo-type-select');
    await filter.waitFor({ state: 'visible', timeout: 30000 });
    await filter.selectOption('實質降價30');

    // The counter is initially "載入中..." and can hold stale totals after a filter change.
    // Wait for the UI's loading state to end, then compare the actual filtered result.
    await page.waitForFunction(() => {
      const section = document.querySelector('#tab-promos');
      const filter = document.querySelector('#promo-type-select');
      const count = document.querySelector('#promos-counter')?.textContent?.trim();
      const counter = document.querySelector('#promos-counter');
      return section && !section.classList.contains('hidden') &&
        filter?.value === '實質降價30' &&
        counter?.dataset.filterReady === 'true' &&
        counter?.dataset.filterType === '實質降價30' &&
        /^\d[\d,]* 筆特惠$/.test(count || '');
    }, null, { timeout: 90000 });

    const promoText = (await page.locator('#promos-counter').innerText()).trim();
    const promo = Number(promoText.match(/^[\d,]+/)[0].replace(/,/g, ''));
    assert.equal(promo, home, '30% deal count must match between homepage and promotions');
    const diagnosis = await page.evaluate(async () => {
      const m = await import('./packed-turso.js');
      const matches = await m.searchPacked({keyword:'麥香燻雞起司',limit:100});
      const product = matches.find(x => x.product_uuid === '98fe837b-3c5a-508a-9070-364d457c2e74');
      if (!product) return {matches: matches.slice(0,5).map(x=>({id:x.product_uuid,name:x.product_name,store:x.store_name}))};
      const events = await m.packedHistory(product.store_uuid,product.product_uuid);
      return {product:{id:product.product_uuid,store:product.store_uuid,name:product.product_name},events:events.map(e=>({time:e.event_time,type:e.event_type,old:e.old_state?JSON.parse(e.old_state):null,new:e.new_state?JSON.parse(e.new_state):null}))};
    });
    console.log('LOUISA_HISTORY_DIAGNOSTIC '+JSON.stringify(diagnosis));
    // Open-hour pricing regression from real archived Uber menus:
    // 麵線陳 at 01:03 had a closed-menu 300, at 09:08 an in-hours 210.
    const noodle = await page.evaluate(async () => {
      const client = await import('./packed-turso.js');
      const products = await client.searchPacked({keyword:'大苑子飲品果汁', limit:50000});
      const product = products.find(p => p.product_uuid === '7e3d2e5b-b210-5ea4-b66d-1accfcb79d1e');
      if (!product) return {found:false, candidates:products.length};
      const events = await client.packedHistory(product.store_uuid, product.product_uuid);
      return {found:true, events:events.map(e=>({time:e.event_time,type:e.event_type,
        price:JSON.parse(e.new_state || e.old_state || '{}').price,
        valid:JSON.parse(e.new_state || e.old_state || '{}').price_valid}))};
    });
    console.log('NOODLE_VALID_HISTORY '+JSON.stringify(noodle));
    // The product may be absent from the search index when its store is closed.
    // Absence is correct under the verified-open ingestion rule; never force
    // stale closed-hour menu items into the live site just to satisfy a test.
    if (noodle.found) {
      assert.ok(noodle.events.length > 0, 'Searchable product has in-hours history');
      assert.ok(noodle.events.every(e => e.valid === true && Number(e.price) !== 300),
        'Closed-hours 300 must not show up as a price observation');
    }

    // Regression reported for 鬆餅家士林店: 89 BOGO (unit 44.5) -> 55 plain.
    // Missing/retired products do not fail future deployments, but whenever
    // the item is available we verify the published database and graph inputs.
    const lemon = await page.evaluate(async () => {
      const client = await import('./packed-turso.js');
      const products = await client.searchPacked({keyword:'鮮檸冬瓜露', limit:500});
      const product = products.find(p => p.product_uuid === '643f21a6-7cce-48c0-8c1a-87b2c282d936');
      if (!product) return {found:false, matches:products.length};
      const events = await client.packedHistory(product.store_uuid, product.product_uuid);
      return {found:true,price:product.price,promo:product.promo_type,
        discount:product.discount_pct,savings:product.savings_amount,
        currentUnit:product.current_price,
        history:events.map(e => {
          const state=JSON.parse(e.new_state || e.old_state || '{}');
          return {time:e.event_time,price:state.price,promo:state.promo_type,
            unit:window.UBER_DATA_CONTRACT.unitPrice(state.price,state.promo_type,state.quantity)};
        })};
    });
    console.log('LEMON_GOURD_EFFECTIVE_PRICE_REGRESSION ' + JSON.stringify(lemon));
    if (lemon.found) {
      const old = lemon.history.find(h => h.price === 89 && /買.*送/.test(h.promo || ''));
      const regular = lemon.history.find(h => h.price === 55 && (!h.promo || h.promo === '無'));
      if (old && regular) {
        assert.equal(old.unit,44.5, 'archived BOGO effective price must be $44.50');
        assert.equal(regular.unit,55, 'regular price must be $55 per unit');
        assert.ok(regular.unit > old.unit, 'BOGO expiration was a price increase, not a drop');
      }
      if (lemon.price === 55 && (!lemon.promo || lemon.promo === '無')) {
        assert.equal(lemon.currentUnit,55);
        assert.equal(lemon.discount,0, 'current item cannot falsely claim a 38.2% discount');
        assert.equal(lemon.savings,0);
      }
    }

    assert.deepEqual(errors, [], 'browser must have no uncaught errors');
    console.log('LIVE PAGE PASS: 30%-discounts homepage=' + home + ', promotions=' + promo);
  } catch (error) {
    fs.mkdirSync('test-results', { recursive: true });
    await page.screenshot({ path: 'test-results/live-site-failure.png', fullPage: true }).catch(() => {});
    fs.writeFileSync('test-results/error.txt', String(error?.stack || error) + '\nBrowser errors:\n' + errors.join('\n'));
    throw error;
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exit(1); });
