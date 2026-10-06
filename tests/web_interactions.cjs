const fs=require('node:fs');
const vm=require('node:vm');
const assert=require('node:assert/strict');
const elements=new Map();
function element(id) {
  if(!elements.has(id)) {
    const classes=new Set(id==='price-history-modal'?['hidden']:[]);
    elements.set(id,{textContent:'',innerHTML:'',href:'#',listeners:{},classList:{add:x=>classes.add(x),remove:x=>classes.delete(x),contains:x=>classes.has(x),toggle(x){if(classes.has(x)){classes.delete(x);return false;}classes.add(x);return true;}},addEventListener(event,fn){this.listeners[event]=fn;},getContext:()=>({})});
  }
  return elements.get(id);
}
const context={console,URL,setTimeout,clearTimeout,document:{readyState:'loading',addEventListener(){},getElementById:element,documentElement:element('root')},localStorage:{getItem:()=>null,setItem(){}},window:{matchMedia:()=>({matches:false}),UBER_RADAR_CONFIG:{ENABLE_TURSO:false},isSecureContext:true,open(){context.opened=true;}},navigator:{clipboard:{writeText:()=>new Promise(()=>{})}},lucide:{createIcons(){}},Chart:function(ctx,config){this.options=config.options;this.data=config.data;this.destroy=()=>{};this.update=()=>{context.updated=true;};}};
vm.createContext(context);
vm.runInContext(fs.readFileSync('web/app.js','utf8'),context);
(async()=>{
  vm.runInContext('initTheme()',context);
  element('theme-toggle').listeners.click();
  assert(element('price-history-modal').classList.contains('hidden'));
  await context.showPriceHistoryModal('s','p','商品','店家','https://www.ubereats.com/tw');
  element('theme-toggle').listeners.click();
  assert.equal(element('modal-product-name').textContent,'商品');
  assert.equal(element('modal-store-name').textContent,'店家');
  assert(context.updated);
  assert.equal(vm.runInContext('APP_STATE.chartInstance.data.datasets[0].data.length',context),0);
  context.hidePriceHistoryModal();
  element('theme-toggle').listeners.click();
  assert(element('price-history-modal').classList.contains('hidden'));
  assert.equal(vm.runInContext('APP_STATE.chartInstance',context),null);
  vm.runInContext("APP_STATE.allProducts = [{store_id:'wrong',product_id:'p',price:999},{store_id:'s',product_id:'p',price:100,eff_price:50}]",context);
  await context.showPriceHistoryModal('s','p','商品','店家','');
  assert.equal(vm.runInContext('APP_STATE.chartInstance.data.datasets[0].data[0]',context),50);
  context.window.UBER_RADAR_CONFIG.ENABLE_TURSO=true;
  let resolve;
  context.getPackedTursoClient=async()=>({packedHistory:()=>new Promise(r=>resolve=r)});
  const pending=context.showPriceHistoryModal('s','p','商品','店家','');
  await new Promise(setImmediate);
  context.hidePriceHistoryModal();
  resolve([]); await pending;
  assert.equal(vm.runInContext('APP_STATE.chartInstance',context),null);
  context.showToast=()=>{};
  context.openUberEatsOrder({preventDefault(){},stopPropagation(){}},'https://www.ubereats.com/tw','商品','店家');
  assert(context.opened,'Order must open before clipboard promise completes');
  console.log('PASS: theme switching, chart recoloring, close/reopen regression, empty history, store identity, stale history cancellation, order gesture');
})().catch(e=>{console.error(e);process.exitCode=1;});

