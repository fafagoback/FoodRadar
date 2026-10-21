// End-to-end check against deployed GitHub Pages and read-only Turso.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const storeUuid = '64db7fd1-7219-5ebb-9b1b-1d866af5a999';
const productUuid = '1d639843-c4e7-488a-bfcc-e9acf762cf3a';
(async () => {
  const browser = await chromium.launch({headless: true});
  const page = await browser.newPage({viewport: {width: 1280, height: 900}});
  const errors=[];
  page.on('pageerror', e => errors.push(String(e)));
  try {
    await page.goto('https://fafagoback.github.io/FoodRadar/', {waitUntil: 'domcontentloaded',timeout:90000});
    await page.waitForFunction(() =>
      typeof window.showPriceHistoryModal === 'function' &&
      window.UBER_RADAR_CONFIG?.ENABLE_TURSO === true, null, {timeout:90000});
    await page.evaluate(async ids => {
      await window.showPriceHistoryModal(ids.store, ids.product,
        '齊食養生菜飯餐(純素)','齊好食 養生 泡菜 水果 特調飲','');
    }, {store: storeUuid,product:productUuid});
    const result=await page.evaluate(() => {
      const rows=[...document.querySelectorAll('#modal-history-tbody tr')]
        .map(tr=>[...tr.querySelectorAll('td')].map(td=>td.textContent.trim()))
        .filter(cells=>cells.length===4);
      const chart=Chart.getChart(document.getElementById('priceHistoryChart'));
      return {
        rows,
        chartPoints:chart?.data.datasets[0]?.data?.length,
        chartLabels:chart?.data.labels,
        modalVisible:!document.getElementById('price-history-modal').classList.contains('hidden')
      };
    });
    console.log('LIVE_PRICE_HISTORY '+JSON.stringify(result));
    assert.equal(result.modalVisible,true);
    assert.equal(result.rows.length,4, '4 price observations, not 8+ duplicate event rows');
    assert.equal(result.chartPoints,4, 'chart has 4 points, no duplicate events');
    assert.equal(new Set(result.rows.map(row=>row[0])).size,4);
    assert.deepEqual(result.rows.map(row=>row[0]), [
      '2026/10/20 23:13','2026/10/20 01:03',
      '2026/10/14 23:15','2026/10/07 00:52']);
    assert.deepEqual(result.rows.map(row=>row[3]), ['$175','$250','$175','$250']);
    assert.deepEqual(errors,[]);
    console.log('PASS: deployed price-history modal shows unique chronological prices');
  } catch(err) {
    fs.mkdirSync('test-results',{recursive:true});
    await page.screenshot({path:'test-results/price-history-failure.png',fullPage:true}).catch(()=>{});
    throw err;
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
