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
    await page.waitForFunction(() => {
      const value = document.querySelector('#stat-big-discounts')?.textContent?.trim();
      return value && /^\d[\d,]*$/.test(value);
    }, null, { timeout: 90000 });

    const homeText = (await page.locator('#stat-big-discounts').innerText()).trim();
    assert.match(homeText, /^\d[\d,]*$/, 'dashboard must display a numeric count');
    const home = Number(homeText.replace(/,/g, ''));

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
