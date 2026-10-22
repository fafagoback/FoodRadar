/* Shared by the packed reader and the small, explicitly dated fallback. */
(function (root) {
  const cities = '基隆市 台北市 新北市 桃園市 新竹市 新竹縣 苗栗縣 台中市 彰化縣 南投縣 雲林縣 嘉義市 嘉義縣 台南市 高雄市 屏東縣 宜蘭縣 花蓮縣 台東縣 澎湖縣 金門縣 連江縣'.split(' ');
  const english = 'Keelung City|Taipei City|New Taipei City|Taoyuan City|Hsinchu City|Hsinchu County|Miaoli County|Taichung City|Changhua County|Nantou County|Yunlin County|Chiayi City|Chiayi County|Tainan City|Kaohsiung City|Pingtung County|Yilan County|Hualien County|Taitung County|Penghu County|Kinmen County|Lienchiang County'.split('|');
  function city(address, region = '') {
    for (const value of [address, region]) {
      const text = String(value || '').replaceAll('臺', '台').trim();
      const matches = cities.filter(c => text.includes(c));
      if (matches.length === 1) return matches[0];
      if (matches.length > 1) return '';
      const i = english.findIndex(c => c.toLowerCase() === text.toLowerCase());
      if (i >= 0) return cities[i];
    }
    return '';
  }
  function positive(value) {
    if (value == null || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : null;
  }
  // Shared unit-equivalent price for all product lists and historical trends.
  // Direct discounted menu prices (e.g. 7折) are already payable prices.
  function unitPrice(price, promoType, quantity = 1) {
    const p = positive(price);
    if (p === null) return null;
    const type = String(promoType || '').trim();
    const digit = {'1':1,'2':2,'3':3,'4':4,'5':5,'一':1,'二':2,'兩':2,'三':3,'四':4,'五':5};
    const buy = type.match(/買\s*([1-5一二兩三四五])\s*送\s*([1-5一二兩三四五])/);
    const round = n => Math.round((n + Number.EPSILON) * 100) / 100;
    if (buy) {
      const b = digit[buy[1]], f = digit[buy[2]];
      return round(p * b / (b + f));
    }
    if (type === '買A送B' || type.toUpperCase() === 'BOGO') return round(p / 2);
    if (/加\s*[1一]\s*元多\s*[1一]\s*件/.test(type)) return round((p + 1) / 2);
    if (/第\s*[2二兩]\s*[件杯項份]\s*半價/.test(type)) return round(p * 0.75);
    const q = positive(quantity) || 1;
    return round(p / q);
  }
  function product(row) {
    const price = positive(row.price ?? row.curr_raw_price);
    const quantity = positive(row.quantity ?? row.curr_qty) || 1;
    const effective = positive(row.effective_price ?? row.eff_price ?? row.current_price);
    const legacy = row.curr_raw_price !== undefined || row.prev_raw_price !== undefined;
    return {...row, price, quantity, eff_price: effective, effective_price: effective,
      // Old static exporters guessed cities from ambiguous street names.
      city: city(row.street_address || row.address, legacy ? '' : row.city),
      valid_price: price !== null};
  }
  // Same last-valid-observation comparison as src/price_change.py.
  function compareUnitPrices(previous, current) {
    const prior = positive(previous), now = positive(current);
    if (prior === null || now === null) return null;
    const delta = Math.round((now - prior) * 100) / 100;
    const savings = Math.round(Math.max(0, -delta) * 100) / 100;
    return {
      previous: prior, current: now, delta, savings,
      direction: delta < 0 ? 'down' : delta > 0 ? 'up' : 'flat',
      dropPct: savings > 0 ? Math.round(savings / prior * 10000) / 100 : 0,
      percent: Math.round(Math.abs(delta) / prior * 10000) / 100
    };
  }
  function money(value) {
    const n = positive(value);
    return n === null ? '價格未提供' : '$' + n.toLocaleString('zh-TW', {minimumFractionDigits: 0, maximumFractionDigits: 0});
  }
  function unitMoney(value) {
    const n = positive(value);
    const decimals = Number.isInteger(n) ? 0 : 2;
    return n === null ? '價格未提供' : String.fromCharCode(36) + n.toLocaleString('zh-TW', {minimumFractionDigits: decimals, maximumFractionDigits: 2});
  }
  root.UBER_DATA_CONTRACT = {city, positive, product, money, unitPrice, unitMoney, compareUnitPrices};
})(typeof window === 'undefined' ? globalThis : window);
