// Live GitHub Pages browser acceptance test. Never against local fixtures.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const errors = [];
  const forbiddenDatabaseRequests = [];
  const initialBlobRequests = [];
  const initialBlobSizes = [];
  let measuringInitialLoad = true;
  try {
    page.on('pageerror', error => errors.push(String(error)));
    page.on('requestfinished', request => {
      if (!measuringInitialLoad || !/\/data\/blobs\/[a-f0-9]{64}\.zst/.test(request.url())) return;
      initialBlobRequests.push(request.url());
      initialBlobSizes.push(request.sizes().then(s => s.responseBodySize));
    });
    page.on('request', request => {
      if (/turso\.io\/v2\/pipeline/.test(request.url())) forbiddenDatabaseRequests.push(request.url());
    });
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

    measuringInitialLoad = false;
    const bytes = await Promise.all(initialBlobSizes);
    assert.ok(bytes.every(n => n <= 2 * 1024 * 1024),
      'initial response may never contain an oversized static database shard');
    assert.ok(bytes.reduce((sum,n)=>sum+n,0) <= 4 * 1024 * 1024,
      'first view must download at most 4 MB of compressed static blobs');
    const schema = await page.evaluate(async () =>
      (await (await fetch('data/manifest.json',{cache:'no-store'})).json()).schema);
    assert.equal(schema, 'foodradar-static-v2',
      'live site must use the paged client schema');
    // Exact-radius coordinate search must use local geo tiles, not global candidates.
    const spatial = await page.evaluate(async () => {
      const m = await (await fetch('data/manifest.json', {cache:'no-store'})).json();
      if (!m.geo || !Object.keys(m.geo.products || {}).length)
        throw new Error('No published grid index');
      const [cell] = Object.keys(m.geo.products).sort((a,b)=>
        m.geo.products[b].length - m.geo.products[a].length);
      const [latIndex, lonIndex] = cell.split(':').map(Number);
      const lat = (latIndex + .5) * m.geo.cell_degrees;
      const lon = (lonIndex + .5) * m.geo.cell_degrees;
      const client = await import('./static-catalog.js');
      const location = {latitude:lat, longitude:lon, radiusKm:2.2, enabled:true};
      const products = await client.searchProductsPage({
        kind:'global', page:1, pageSize:50, location,
      });
      const stores = await client.searchStoresPage({
        page:1, pageSize:50, newOnly:false, location,
      });
      const radius5 = {...location, radiusKm:5};
      const wider = await client.searchProductsPage({
        kind:'global', page:1, pageSize:50, location:radius5,
      });
      return {
        cell, lat, lon,
        productCount:products.total, returnedProducts:products.rows.length,
        allProductsInRange:products.rows.every(p => Number(p.distance_km) <= 2.2 + 1e-7),
        storeCount:stores.total, returnedStores:stores.rows.length,
        radius5Total:wider.total, radius5Returned:wider.rows.length,
        radius5InRange:wider.rows.every(p=>Number(p.distance_km)<=5+1e-7),
        allStoresInRange:stores.rows.every(p => Number(p.distance_km) <= 2.2 + 1e-7),
      };
    });
    assert.ok(spatial.productCount > 0, 'a populated grid cell must return nearby products');
    assert.ok(spatial.radius5Total >= spatial.productCount && spatial.radius5InRange,
      '5 km local search must return the complete bounded nearby candidate set');
    assert.ok(spatial.allProductsInRange && spatial.allStoresInRange,
      'nearby products and stores must pass exact radius verification');
    assert.ok(spatial.returnedProducts <= 50 && spatial.returnedStores <= 50,
      'spatial queries must remain paginated');
    console.log('LIVE_GEO_RADIUS_VERIFIED ' + JSON.stringify(spatial));

    console.log('FOODRADAR_INITIAL_BLOB_BYTES=' +
      JSON.stringify({files:initialBlobRequests.length,total:bytes.reduce((sum,n)=>sum+n,0)}));

    const homeText = (await page.locator('#stat-big-discounts').innerText()).trim();
    assert.match(homeText, /^\d[\d,]*$/, 'dashboard must display a numeric count');
    const home = Number(homeText.replace(/,/g, ''));
    const coverage = (await page.locator('#discounts-coverage').innerText()).trim();
    assert.match(coverage, /目前已收錄可搜尋商品 [\d,]+ 件，其中最近採集批次確認可外送 [\d,]+ 件；[\d,]+ 件具有七日價格比較基準；最近七天有 \d+ 個菜單價格採集日；[\d,]+ 件屬平台促銷/,
      'dashboard must disclose the verified deliverable-price and seven-day-comparison denominators');
    console.log('DEAL_COMPARISON_COVERAGE ' + coverage);

    // The legacy HF archive quote section was intentionally removed.
    // Guard against stale releases still exposing it.
    assert.equal(await page.locator('#archive-quote-section').count(), 0,
      'retired HF archive menu section must not appear on live site');
    assert.equal(await page.locator('#archive-quote-grid').count(), 0,
      'retired HF archive menu cards must not appear on live site');
    console.log('LIVE_ARCHIVE_SECTION_REMOVED');

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
    // Platform offers are a DIFFERENT measure from price drops. Verify the
    // nonzero platform count independently rather than celebrating a zero.
    const platformExpectedText = coverage.match(/；([\d,]+) 件屬平台促銷/);
    assert.ok(platformExpectedText, 'comparison coverage must disclose platform offers');
    const platformExpected = Number(platformExpectedText[1].replace(/,/g, ''));
    await filter.selectOption('平台優惠');
    await page.waitForFunction(() => {
      const counter = document.querySelector('#promos-counter');
      return counter?.dataset.filterReady === 'true' &&
             counter?.dataset.filterType === '平台優惠' &&
             /^[\d,]+ 筆特惠$/.test(counter.textContent?.trim() || '');
    }, null, { timeout: 90000 });
    const platformCount = Number((await page.locator('#promos-counter').innerText()).trim().match(/^[\d,]+/)[0].replace(/,/g, ''));
    assert.equal(platformCount, platformExpected, 'visible platform promotions must match current-batch packed metadata');
    console.log('LIVE_PROMOTION_COUNTS '+JSON.stringify({newDrop30:home,existingPlatformOffers:platformCount}));
    const diagnosis = await page.evaluate(async () => {
      const m = await import('./static-catalog.js');
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
      const client = await import('./static-catalog.js');
      const products = await client.searchPacked({keyword:'大苑子飲品果汁', limit:50000});
      const product = products.find(p => p.product_uuid === '7e3d2e5b-b210-5ea4-b66d-1accfcb79d1e');
      if (!product) return {found:false, candidates:products.length};
      const events = await client.packedHistory(product.store_uuid, product.product_uuid);
      return {found:true, events:events.map(e=>({time:e.event_time,type:e.event_type,
        price:JSON.parse(e.new_state || e.old_state || '{}').price,
        valid:JSON.parse(e.new_state || e.old_state || '{}').price_valid}))};
    });
    console.log('NOODLE_VALID_HISTORY '+JSON.stringify(noodle));
    // Closed stores can retain their last known menu, but *verified* price
    // history must not incorporate closed-hour menu quotes.
    if (noodle.found) {
      const verified = noodle.events.filter(e => e.valid === true);
      assert.ok(verified.every(e => Number(e.price) !== 300),
        'Closed-hours 300 must not show up as a verified price observation');
    }

    // Regression reported for 鬆餅家士林店: 89 BOGO (unit 44.5) -> 55 plain.
    // Missing/retired products do not fail future deployments, but whenever
    // the item is available we verify the published database and graph inputs.
    const lemon = await page.evaluate(async () => {
      const client = await import('./static-catalog.js');
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
            verified:state.price_evidence_status==='VERIFIED_MENU',
            unit:state.price_evidence_status==='VERIFIED_MENU' ? Number(state.effective_price || 0) : null};
        })};
    });
    console.log('LEMON_GOURD_EFFECTIVE_PRICE_REGRESSION ' + JSON.stringify(lemon));
    if (lemon.found) {
      const old = lemon.history.find(h => h.price === 89 && h.verified && /買.*送/.test(h.promo || ''));
      const regular = lemon.history.find(h => h.price === 55 && h.verified && (!h.promo || h.promo === '無'));
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

    // Reported real SKU: near ABA (459 -> 899 -> 449.50). The
    // item can remain searchable; it must not claim a new seven-day discount.
    const abaCases = await page.evaluate(async () => {
      const client = await import('./static-catalog.js');
      const products = await client.searchPacked({keyword:'YBARRA',limit:500});
      const beef = await client.searchPacked({keyword:'美國冷藏牛腱',limit:500});
      const target = [...products.filter(p => /益百萊/.test(p.product_name || '') &&
        /1000\s*ml/i.test(p.product_name || '') && /北投/.test(p.store_name || '')),
        ...beef.filter(p => /美國冷藏牛腱/.test(p.product_name || '') && /350\s*g/i.test(p.product_name || '') && /北投行義/.test(p.store_name || ''))];
      const output = [];
      for (const p of target) {
        const events = await client.packedHistory(p.store_uuid,p.product_uuid);
        const points = events.map(e => {
          const s=JSON.parse(e.new_state || e.old_state || '{}');
          return {at:Date.parse(e.event_time),unit:Number(s.effective_price || 0)};
        }).filter(x=>Number.isFinite(x.at) && x.unit>0).sort((a,b)=>a.at-b.at);
        const runs=[];
        for(const x of points) {
          if(runs.length && runs.at(-1).unit===x.unit) runs.at(-1).last=x.at;
          else runs.push({unit:x.unit,first:x.at,last:x.at});
        }
        const day=86400000, now=points.at(-1)?.at || 0;
        let blocked=false;
        for(let i=0;i+2<runs.length;i++){
          const [a,b,c]=runs.slice(i,i+3);
          const near=Math.abs(c.unit-a.unit)<=0.15*a.unit &&
            (b.unit>Math.max(a.unit,c.unit)||b.unit<Math.min(a.unit,c.unit)) &&
            Math.abs(b.unit-a.unit)>=0.2*a.unit && Math.abs(b.unit-c.unit)>=0.2*c.unit;
          const reversal=(a.unit===c.unit && a.unit!==b.unit)||near;
          if(reversal && c.first-a.last<=14*day &&
             (a.last>=now-14*day || (b.unit>a.unit?b.last>=now-7*day:
               a.last>=now-7*day||c.first>=now-7*day))) blocked=true;
        }
        output.push({id:p.product_uuid,name:p.product_name,blocked,discount:p.discount_pct,
          deal:p.is_price_deal,reference:p.seven_day_reference_price,
          current:p.current_price,points:points.length});
      }
      return output;
    });
    console.log('PRICE_ABA_REGRESSION '+JSON.stringify(abaCases));
    for(const item of abaCases.filter(p=>p.blocked)){
      assert.equal(Number(item.discount||0),0,'ABA product must not claim a new discount');
      assert.equal(Number(item.deal||0),0,'ABA product must not enter the deals index');
      assert.equal(item.reference,null,'ABA product must have no disputed reference');
    }

    // Live search pagination regression. For the audited Raw batch the
    // canonical Costco count is 63; later batches may legitimately vary.
    const expectedCostco = manifest.batch === '20261102133219' ? 63 :
      await page.evaluate(async () => {
        const client = await import('./static-catalog.js');
        const result = await client.searchProductsPage({kind:'global',keyword:'costco',page:1});
        return result.total;
      });
    assert.ok(Number.isInteger(expectedCostco) && expectedCostco >= 0,
      'live Costco count must be a nonnegative integer');
    await page.locator('.nav-tab[data-target="tab-global-search"]').click();
    await page.locator('#global-search-input').fill('costco');
    if (expectedCostco > 0) {
      await page.waitForFunction(expected => {
        const grid = document.querySelector('#global-products-grid');
        const pagination = document.querySelector('#global-pagination')?.textContent || '';
        const reported = Number((pagination.match(/共\s*([\d,]+)\s*筆/)?.[1] || '').replace(/,/g,''));
        return grid?.querySelectorAll('.radar-card').length === Math.min(50,expected) &&
          reported === expected;
      }, expectedCostco, {timeout:90000});
      console.log('COSTCO_LIVE_FIRST_PAGE_PASS ' + (await page.locator('#global-pagination').innerText()).replace(/\s+/g,' '));
      if (expectedCostco > 50) {
        await page.locator('#global-pagination button', {hasText:'下一頁'}).click();
        await page.waitForFunction(expected => {
          const grid = document.querySelector('#global-products-grid');
          const pagination = document.querySelector('#global-pagination')?.textContent || '';
          const reported = Number((pagination.match(/共\s*([\d,]+)\s*筆/)?.[1] || '').replace(/,/g,''));
          return grid?.querySelectorAll('.radar-card').length === Math.min(50,expected-50) &&
            reported === expected;
        }, expectedCostco, {timeout:90000});
        console.log('COSTCO_LIVE_SECOND_PAGE_PASS ' + (await page.locator('#global-pagination').innerText()).replace(/\s+/g,' '));
      }
    } else {
      console.log('COSTCO_LIVE_ZERO_ITEMS: no current menu listings in this release');
    }
    // Real catalog roundtrip: choose one existing current card, then find it
    // again by store name + item name. A missing Costco/Louisa listing must
    // never be mistaken for proof that all keyword searches work.
    const lookup = await page.evaluate(async () => {
      const client = await import('./static-catalog.js');
      const initial = await client.searchProductsPage({kind:'global',page:1,pageSize:50});
      const selected = initial.rows.find(p => p.store_name && p.product_name);
      if (!selected) return {passed:false,reason:'No current product from static data'};
      const query = selected.store_name + ' ' + selected.product_name;
      const [products, stores] = await Promise.all([
        client.searchProductsPage({kind:'global',keyword:query,page:1,pageSize:50}),
        client.searchStoresPage({keyword:selected.store_name,newOnly:false,page:1}),
      ]);
      const found = products.rows.some(p =>
        p.store_uuid===selected.store_uuid && p.product_uuid===selected.product_uuid);
      const storeFound = stores.rows.some(s => s.store_uuid===selected.store_uuid);
      const hist = await client.packedHistory(selected.store_uuid,selected.product_uuid);
      return {passed:found && storeFound && Array.isArray(hist),query,
        store_name:selected.store_name,product_name:selected.product_name,city:selected.city,
        product: selected.product_uuid,store:selected.store_uuid,
        matched_products:products.total,matched_stores:stores.total,
        found,storeFound,historyLength:hist.length};
    });
    assert.equal(lookup.passed, true,
      'static search must round-trip a real product and its store: '+JSON.stringify(lookup));
    console.log('LIVE_STATIC_SEARCH_ROUNDTRIP ' + JSON.stringify(lookup));

    // Full user journey across every tab: rows must be backed by the same verified
    // static client, not stale fallback sample cards or uninitialized placeholders.
    const liveCatalog = await page.evaluate(async () => {
      const catalog = await import('./static-catalog.js');
      const [discounts, stores, products, promos, all, metadata] = await Promise.all([
        catalog.searchProductsPage({kind:'discounts',minDiscount:30,page:1}),
        catalog.searchStoresPage({newOnly:false,page:1}),
        catalog.searchProductsPage({kind:'newProducts',page:1}),
        catalog.searchProductsPage({kind:'promos',page:1}),
        catalog.searchProductsPage({kind:'global',page:1}),
        catalog.metadata()
      ]);
      return {discounts:discounts.total,stores:stores.total,newProducts:products.total,
        promos:promos.total,global:all.total,published:metadata.published_searchable_products,
        sampleNew:products.rows.find(p => p.product_name)?.product_name||null};
    });
    assert.equal(liveCatalog.global, liveCatalog.published, 'published catalog count must equal browse count');
    assert.ok(liveCatalog.global > 100, 'a nonempty real catalog must be publicly searchable');
    const screens = [
      {tab:'tab-discounts',counter:null,grid:'#discounts-grid',total:liveCatalog.discounts},
      {tab:'tab-stores',counter:'#new-stores-counter',grid:'#new-stores-grid',total:liveCatalog.stores},
      {tab:'tab-products',counter:'#new-products-counter',grid:'#new-products-grid',total:liveCatalog.newProducts},
      {tab:'tab-promos',counter:'#promos-counter',grid:'#promos-grid',total:liveCatalog.promos},
      {tab:'tab-global-search',counter:null,grid:'#global-products-grid',total:liveCatalog.global}
    ];
    for (const s of screens) {
      await page.locator('.nav-tab[data-target="'+s.tab+'"]').click();
      // Restore default filters changed by the earlier targeted promotion/Costco regression.
      if (s.tab === 'tab-promos') {
        await page.locator('#promo-type-select').selectOption('全部');
        await page.waitForFunction(expected => {
          const counter=document.querySelector('#promos-counter');
          const count=Number((counter?.textContent?.match(/[0-9,]+/)||[])[0]?.replace(/,/g,''));
          return counter?.dataset.filterReady==='true' &&
            counter?.dataset.filterType==='全部' && count===expected;
        },s.total,{timeout:60000});
      }
      if (s.tab === 'tab-global-search') await page.locator('#global-search-input').fill('');
      await page.waitForFunction(({grid,total}) => {
        const root=document.querySelector(grid);
        return root && (total===0 || root.querySelectorAll('.radar-card').length===Math.min(total,50));
      }, {grid:s.grid,total:s.total},{timeout:60000});
      if(s.counter) {
        const counter=await page.locator(s.counter).innerText();
        const value=Number((counter.match(/[\d,]+/)||[])[0]?.replace(/,/g,''));
        assert.equal(value,s.total,'wrong '+s.tab+' count');
      }
    }
    // A real store is searched through the visible form (rather than a direct JS API call).
    await page.locator('.nav-tab[data-target="tab-stores"]').click();
    await page.locator('#store-scope-select').selectOption('all');
    await page.locator('#store-search-input').fill(lookup.store_name);
    await page.waitForFunction(name => {
      const grid=document.querySelector('#new-stores-grid');
      return grid?.querySelectorAll('.radar-card').length > 0 &&
        grid?.textContent?.includes(name);
    },lookup.store_name,{timeout:30000});
    if(liveCatalog.sampleNew) {
      await page.locator('.nav-tab[data-target="tab-products"]').click();
      await page.locator('#product-search-input').fill(liveCatalog.sampleNew);
      await page.waitForFunction(name => {
        const grid=document.querySelector('#new-products-grid');
        return grid?.querySelectorAll('.radar-card').length>0 &&
          grid?.textContent?.includes(name);
      },liveCatalog.sampleNew,{timeout:30000});
    }
    // Verify global browser UI and history modal on a REAL product, not just the API.
    await page.locator('.nav-tab[data-target="tab-global-search"]').click();
    await page.locator('#global-search-input').fill(lookup.query);
    await page.waitForFunction(name =>
      document.querySelector('#global-products-grid')?.textContent?.includes(name) &&
      document.querySelector('#global-products-grid')?.querySelectorAll('.radar-card').length>0,
    lookup.product_name,{timeout:30000});
    await page.locator('#global-products-grid [data-action="history"]').first().click();
    await page.locator('#price-history-modal').waitFor({state:'visible',timeout:30000});
    await page.locator('#modal-history-tbody tr').first().waitFor({state:'visible',timeout:30000});
    await page.locator('#modal-close-btn').click();
    await page.locator('#price-history-modal').waitFor({state:'hidden',timeout:30000});
    console.log('LIVE_ALL_TABS_AND_SEARCH_PASS '+JSON.stringify(liveCatalog));

    // Mobile Chromium: navigation, typing, horizontal overflow, and static search.
    const mobile = await browser.newPage({
      viewport:{width:390,height:844}, deviceScaleFactor:2, isMobile:true, hasTouch:true
    });
    const mobileErrors=[];
    mobile.on('pageerror',e=>mobileErrors.push(String(e)));
    try {
      await mobile.goto(url,{waitUntil:'domcontentloaded',timeout:90000});
      await mobile.waitForFunction(() =>
        /^\d[\d,]*$/.test(document.querySelector('#stat-big-discounts')?.textContent?.trim()||''),
      null,{timeout:90000});
      await mobile.locator('.nav-tab[data-target="tab-global-search"]').click();
      await mobile.locator('#global-search-input').fill(lookup.query);
      await mobile.waitForFunction(name =>
        document.querySelector('#global-products-grid')?.textContent?.includes(name) &&
        document.querySelector('#global-products-grid')?.querySelectorAll('.radar-card').length>0,
      lookup.product_name,{timeout:30000});
      const size=await mobile.evaluate(()=>({
        viewport:window.innerWidth,body:document.documentElement.scrollWidth,
        visibleCards:document.querySelectorAll('#global-products-grid .radar-card').length
      }));
      assert.ok(size.body<=size.viewport+6,'mobile horizontal overflow: '+JSON.stringify(size));
      assert.deepEqual(mobileErrors,[],'mobile browser uncaught errors');
      console.log('LIVE_MOBILE_SEARCH_PASS '+JSON.stringify(size));
    } finally {
      await mobile.close();
    }
    assert.deepEqual(forbiddenDatabaseRequests, [], 'the live site must not call Turso');
    const configured = await page.evaluate(() => window.UBER_RADAR_CONFIG);
    assert.equal(configured.ENABLE_STATIC, true, 'live browser must use static catalog');
    assert.equal(configured.ENABLE_TURSO, false, 'legacy Turso must be disabled');
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
