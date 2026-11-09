/* Contract tests: method B reads visible product cards from DOM, never API JSON. */
const fs = require("fs");
const assert = require("assert");
const vm = require("vm");

const py = fs.readFileSync("scripts/daily_triple_audit.py", "utf8");
const match = py.match(/JS_DOM\s*=\s*r"""([\s\S]*?)"""/);
assert(match, "Method B must expose an unchanged raw JavaScript DOM scanner");

function card(name,price,sku) {
  return {
    innerText: name + "\n" + price + "\n加入購物車",
    getClientRects() { return [{width:100,height:100}]; },
    getAttribute(key) { return key==="data-item-id" ? sku : null; },
    querySelector(key) {
      if (key.includes("h2")) return {innerText:name};
      if (key.includes("img")) return {getAttribute(){return name;}};
      return null;
    }
  };
}
const productA=card("Häagen-Dazs 草莓冰淇淋 473ml","$259","uuid-1");
const productB=card("鮮奶 936ml","NT$95","uuid-2");
const document={
  querySelectorAll(selector) {
    if (selector === '[data-testid*="menu-item"]') return [productA,productB];
    if (selector === "li") return [productA,productB];
    return [];
  }
};
const rows=vm.runInNewContext("(" + match[1] + ")()",{document},{timeout:3000});
assert.strictEqual(rows.length,2,"DOM must deduplicate duplicate selector matches");
assert.strictEqual(rows[0].name,"Häagen-Dazs 草莓冰淇淋 473ml");
assert.strictEqual(rows[0].price_text,"$259");
assert.strictEqual(rows[0].item_id,"uuid-1");
assert.strictEqual(rows[1].price_text,"NT$95");
assert(!py.includes('JS_DOM = """'),"Parser must retain JavaScript regexes as raw string");
console.log("PASS: DOM product+price extraction, UUID evidence and no duplicate cards");
