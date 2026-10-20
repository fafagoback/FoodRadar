// Live GitHub Pages browser acceptance test. Runs after deployment, never against local fixtures.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
(async () => {
  const browser = await chromium.launch({headless:true});
  try {
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(process.env.FOODRADAR_SITE || 'https://fafagoback.github.io/FoodRadar/', {waitUntil:'domcontentloaded',timeout:90000});
    await page.waitForFunction(() => {const v = document.querySelector('#stat-big-discounts')?.textContent?.trim();return v && v !== '載入中...' && v !== '待同步';}, null, {timeout:90000});
    const home = Number((await page.locator('#stat-big-discounts').innerText()).replace(/,/g,''));
    assert.ok(Number.isFinite(home),'dashboard must return a real discount count');
    // Promotions is a hidden tab on initial page load; activate it before interacting.\n    await page.locator('.nav-tab[data-target="tab-promos"]').click();\n    await page.locator('#promo-type-select').waitFor({state:'visible',timeout:30000});\n    await page.locator('#promo-type-select').selectOption('實質降價30');
    await page.waitForFunction(() => document.querySelector('#promos-counter')?.textContent?.includes('筆特惠'), {timeout:30000});
    await page.waitForTimeout(2000);
    const promo = Number((await page.locator('#promos-counter').innerText()).match(/[\d,]+/)?.[0].replace(/,/g,''));
    assert.equal(promo,home,'30% deal count must match between homepage and promotions');
    assert.deepEqual(errors,[],'browser must have no uncaught errors');
    console.log('LIVE PAGE PASS: 30%-discounts homepage=' + home + ', promotions=' + promo);
  } finally { await browser.close(); }
})().catch(err=>{console.error(err);process.exit(1)});
