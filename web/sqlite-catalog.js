/** Original FoodRadar website API, served directly from the ONE normalized
 *  and compact 60-day HF SQLite. No Pages products, no HF tokens, no HTTP to UE.
 *  SQL stores/price terms are shared; current items and history stay per shop.
 */
import {sqlite} from "./sqlite-storage.js";
import {decompress} from "https://cdn.jsdelivr.net/npm/fzstd@0.1.1/+esm";
import {decode} from "https://cdn.jsdelivr.net/npm/@msgpack/msgpack@3.1.3/+esm";
let loading=null, state=null;
const norm=x=>String(x??"").normalize("NFKC").toLocaleLowerCase().replace(/\s+/g,"");
const money=x=>Number(x)||0;
const unpack=x=>decode(decompress(new Uint8Array(x)));
function uuid(x){
  if(typeof x==="string")return x;
  const h=Array.from(x||[],b=>b.toString(16).padStart(2,"0")).join("");
  return h.length===32?[h.slice(0,8),h.slice(8,12),h.slice(12,16),h.slice(16,20),h.slice(20)].join("-"):h;
}
function distance(a,b,c,d){
  if(![a,b,c,d].every(Number.isFinite))return Infinity;
  const r=Math.PI/180,lat=(c-a)*r,lon=(d-b)*r;
  const h=Math.sin(lat/2)**2+Math.cos(a*r)*Math.cos(c*r)*Math.sin(lon/2)**2;
  return 12742*Math.asin(Math.min(1,Math.sqrt(h)));
}
function nearby(s,location){
  if(!location?.enabled)return true;
  return distance(Number(location.latitude),Number(location.longitude),Number(s[4]),Number(s[5]))
    <=Number(location.radiusKm);
}
function priceOf(d,p){return d.prices.get(Number(p[3]))||null;}
function originalPrice(d,p){const t=priceOf(d,p);return t?Number(t.effective_cents)/100:0;}
async function load(){
  if(state)return state;
  if(loading)return loading;
  loading=(async()=>{
    const db=await sqlite();
    const [index,dicts,priceRows,storeRows]=await Promise.all([
      db.query("SELECT payload FROM relational_frontend_index WHERE key='shops'"),
      db.query("SELECT key,payload FROM dictionary"),
      db.query("SELECT id,original_cents,effective_cents,quantity,promotion FROM relational_prices"),
      db.query("SELECT id,uuid FROM relational_stores")
    ]);
    if(index.length!==1||!dicts.length||!storeRows.length)throw Error("SQLite reference inventory is missing");
    const d={};
    for(const entry of dicts)d[entry.key]=unpack(entry.payload);
    const shops=unpack(index[0].payload);
    const ids=new Map(storeRows.map(x=>[Number(x.id),uuid(x.uuid)]));
    const byId=new Map(shops.map(s=>[Number(s[0]),s]));
    const byUuid=new Map(shops.map(s=>[ids.get(Number(s[0])),s]));
    const prices=new Map(priceRows.map(p=>[Number(p.id),p]));
    const batches=d.__crawl_batches||[];
    const times=batches.map(r=>String(r[1]||"")).filter(Boolean);
    if(!times.length)throw Error("No verified source batch times");
    const latest=times.reduce((x,y)=>x>y?x:y);
    const baseline=times.reduce((x,y)=>x<y?x:y);
    const cutoff=new Date(Date.parse(latest)-7*86400000).toISOString().slice(0,10);
    const observations=d.__batch_keys||[];
    const meta=Object.fromEntries(d.__metadata||[]);
    const count={products:0,current:0,ref:0,promos:0,newProducts:0,deals:0,stores:0,maxSavings:0};
    for(const s of shops){
      if(!s[8])continue;
      count.stores++;
      for(const p of s[11]){
        if(!(p[4]&1))continue;
        count.products++;
        if(p[4]&8)count.current++;
        if(p[4]&2)count.deals++;
        if(p[10])count.promos++;
        if(p[8])count.newProducts++;
        if(p[5]>0)count.ref++;
        count.maxSavings=Math.max(count.maxSavings,money(p[6]));
      }
    }
    const publicMeta={
      ...meta,application:"FoodRadar",latest_batch:db.meta.batch,
      latest_processed_at:latest,baseline_processed_at:baseline,
      source_counts:{stores:shops.length,products:db.meta.counts?.links||count.products},
      published_searchable_products:count.products,
      published_searchable_stores:count.stores,
      active_products:count.products,current_verified_products:count.current,
      active_comparable_products:count.ref,
      seven_day_reference_products:count.ref,
      seven_day_verified_dates:[...new Set(times.map(x=>x.slice(0,10)))].sort(),
      seven_day_history_incomplete:false,active_platform_promotions:count.promos
    };
    const next={db,d,shops,ids,byId,byUuid,prices,latest,baseline,cutoff,
      observations,meta:publicMeta,summary:{
        stores:shops.filter(s=>newStore(s,baseline,cutoff)).length,
        newProducts:count.newProducts,promotions:count.promos,
        maxSavings:count.maxSavings,deals:count.deals},
      cache:new Map()};
    if(db.meta.counts?.links!==shops.reduce((n,s)=>n+s[11].length,0))
      throw Error("Incomplete compact relational inventory");
    state=next;
    return next;
  })().catch(e=>{loading=null;throw e;});
  return loading;
}
function newStore(s,baseline,cutoff){
  const start=String(s[9]||"");
  return Boolean(s[8]&&start.slice(0,10)>baseline.slice(0,10)&&start.slice(0,10)>=cutoff);
}
function decodedValue(col,v,d){
  if(col.endsWith("_uuid"))return uuid(v);
  if(col==="catalog_web"){
    const fields={};
    for(const entry of v||[]){
      const key=d.catalog_key?.[entry[0]];
      if(key===undefined)throw Error("Invalid catalog field key");
      fields[key]=JSON.parse(d["catalog:"+key]?.[entry[1]]||"null");
    }
    return JSON.stringify(fields);
  }
  return Array.isArray(d[col])&&Number.isInteger(v)?d[col][v]:v;
}
function productRow(row,cols,d){
  if(row.length===cols.length)return row;
  const pool=d.__catalog_templates_v1||[];
  const shared=["product_name","category","description"].map(k=>cols.indexOf(k));
  if(shared.some(i=>i<0)||!Number.isInteger(row[0])||!pool[row[0]])
    throw Error("Invalid shared product template reference");
  const out=new Array(cols.length),indices=cols.map((_,i)=>i).filter(i=>!shared.includes(i));
  if(row.length!==indices.length+1)throw Error("Invalid compressed product row width");
  shared.forEach((index,i)=>out[index]=pool[row[0]][i]);
  indices.forEach((index,i)=>out[index]=row[i+1]);
  return out;
}
function queryStoreKey(sid){return /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(sid)
  ?"X'"+sid.replace(/-/g,"")+"'":"'"+sid.replace(/'/g,"''")+"'";}
async function readShop(s){
  const d=await load(),id=Number(s[0]);
  if(d.cache.has(id))return d.cache.get(id);
  const pending=(async()=>{
    const sid=d.ids.get(id),key=queryStoreKey(sid);
    const records=await d.db.query("SELECT payload FROM shop_state WHERE store="+key);
    if(records.length!==1)throw Error("Missing canonical store");
    const [storeValues,rawRows]=unpack(records[0].payload);
    const sc=d.d.__store_columns,pc=d.d.__product_columns;
    const store=Object.fromEntries(sc.map((k,i)=>[k,decodedValue(k,storeValues[i],d.d)]));
    store.store_uuid=sid;
    const products=rawRows.map(row=>{
      const values=productRow(row,pc,d.d);
      return Object.fromEntries(pc.map((k,i)=>[k,decodedValue(k,values[i],d.d)]));
    });
    if(products.length!==s[11].length)throw Error("Per-shop SKU count changed");
    return {store,products,sid};
  })();
  d.cache.set(id,pending);
  while(d.cache.size>32)d.cache.delete(d.cache.keys().next().value);
  try{return await pending;}catch(e){d.cache.delete(id);throw e;}
}
function productCard(s,p,product,store,d,location){
  const catalog=JSON.parse(product.catalog_web||"{}");
  const native=product.price_evidence_status==="VERIFIED_MENU";
  const raw=money(product.menu_quote_price||product.price);
  const verified=native?money(product.menu_quote_effective_price):0;
  const effective=verified>0?verified:raw;
  const current=(store.last_seen===d.latest&&product.last_seen===d.latest&&
    Number(store.is_open)===1&&Number(product.is_open)===1&&native);
  const available=current&&Number(product.delivery_available)===1&&
    catalog.is_sold_out!==true&&catalog.is_available!==false;
  const present=available&&money(product.price)>0;
  const reference=money(product.seven_day_reference_price);
  const pct=current?money(product.seven_day_discount_pct):0;
  const drop=current&&pct>0&&reference>effective;
  const ptype=product.menu_quote_promo_type||product.promo_type;
  const platform=native&&money(product.verified_discount_pct)>0&&
    (/^買[0-9]+送[0-9]+$/.test(String(ptype||""))||ptype==="直接折價");
  const latitude=store.latitude,longitude=store.longitude;
  return {
    product_id:product.product_uuid,product_uuid:product.product_uuid,
    store_id:d.ids.get(Number(s[0])),store_uuid:d.ids.get(Number(s[0])),
    store_name:store.name,product_name:product.product_name,
    category_name:product.category,description:product.description,
    display_original_price:Number.isInteger(catalog.display_original_price_cents)?
      catalog.display_original_price_cents/100:null,
    native_promo_type:catalog.native_promo_type,promotion_uuid:catalog.promotion_uuid,
    cart_campaign_uuid:catalog.cart_campaign_uuid,cart_promo_label:catalog.cart_promo_label,
    is_available:catalog.is_available,is_sold_out:catalog.is_sold_out,
    availability_state:catalog.availability_state,category_uuids:catalog.section_uuids||[],
    price:raw>0?raw:money(product.price),
    quantity:Number(product.menu_quote_quantity||product.quantity||1),
    promo_type:ptype,eff_price:verified||null,effective_price:verified||null,
    verified_discount_pct:native?money(product.verified_discount_pct):0,
    price_evidence_status:product.price_evidence_status,
    order_action_url:product.order_url||store.order_url,
    rating_value:store.rating,review_count:Number(store.review_count||0),
    locality:store.locality,street_address:store.address,city:store.city,
    latitude,longitude,
    crawled_time:product.last_seen||d.latest,first_seen:product.first_seen,
    listing_started_at:product.listing_started_at||product.first_seen,
    original_price:drop?reference:effective,
    reference_price:money(product.reference_price)>0?Number(product.reference_price):null,
    seven_day_reference_price:reference>0?reference:null,
    is_new_price_drop:Number(present&&Number(product.is_price_drop)===1),
    is_verified_delivery_deal:Boolean(drop&&present),
    archive_reference_price:present?money(product.archive_reference_price):0,
    archive_discount_pct:present?money(product.archive_discount_pct):0,
    archive_savings_amount:present?money(product.archive_discount_amount):0,
    archive_evidence_count:present?Number(product.archive_evidence_count||0):0,
    archive_is_price_deal:present?Number(product.archive_is_price_deal||0):0,
    current_price:effective,price_direction:product.price_direction||"unknown",
    price_change_amount:money(product.price_change_amount),
    price_change_percent:money(product.price_change_percent),
    is_price_drop:Number(drop),discount_pct:drop?pct:0,
    savings_amount:drop?money(product.seven_day_discount_amount):0,
    is_price_deal:Number(drop&&Number(product.seven_day_is_price_deal)===1),
    is_current_price_observation:Boolean(present),
    is_current_menu_observation:Boolean(current),
    is_menu_catalog_available:true,delivery_available:current?Boolean(present):null,
    status:product.status,is_open:1,
    distance_km:location?.enabled?distance(Number(location.latitude),Number(location.longitude),
      Number(latitude),Number(longitude)):null
  };
}
function matchProduct(d,s,p,opts){
  if(!s[8]||!(p[4]&1)||!nearby(s,opts.location))return false;
  if(opts.city&&opts.city!=="全部"&&!String(s[2]||"").includes(opts.city))return false;
  const category=d.d.category?.[Number(p[2])]||"";
  if(opts.category&&opts.category!=="全部"&&!category.includes(opts.category))return false;
  const words=String(opts.keyword||"").trim().split(/\s+/).filter(Boolean);
  if(words.length){
    const hay=norm([d.d.product_name?.[Number(p[1])],category,s[1],s[3],s[12]].join(" "));
    if(!words.every(w=>hay.includes(norm(w))))return false;
  }
  if(opts.kind==="discounts"||opts.promoType==="實質降價30"){
    if(!(p[4]&2)||money(p[5])<money(opts.minDiscount||30)||
      money(p[6])<money(opts.minSavings||0))return false;
  }else if(opts.kind==="newProducts"){
    if(!p[8])return false;
  }else if(opts.kind==="promos"){
    if(!p[10])return false;
    if(opts.promoType==="平台優惠"&&!p[9])return false;
    if(opts.promoType==="買一送一"&&(!p[9]||!/^買[0-9]+送[0-9]+$/.test(String(p[11]||""))))return false;
    if(opts.promoType==="折"&&(!p[9]||p[11]!=="直接折價"))return false;
    if(["半價","加1元","多入"].includes(opts.promoType))return false;
  }else if(opts.kind==="global"&&opts.sort==="promo_only"&&!p[10])return false;
  return true;
}
function compareProducts(d,a,b,sort,kind){
  const [s,p]=a,[t,q]=b;
  const o=sort||(kind==="discounts"?"discount_desc":kind==="promos"?"price_asc":
    kind==="newProducts"?"new_desc":"rating_desc");
  let n=0;
  if(o==="discount_desc")n=money(q[5])-money(p[5])||money(q[6])-money(p[6]);
  else if(o==="savings_desc")n=money(q[6])-money(p[6])||money(q[5])-money(p[5]);
  else if(o==="price_asc")n=originalPrice(d,p)-originalPrice(d,q);
  else if(o==="price_desc")n=originalPrice(d,q)-originalPrice(d,p);
  else if(o==="new_desc")n=Number(q[8])-Number(p[8]);
  else if(o==="promo_first")n=Number(q[10])-Number(p[10]);
  else if(o==="name_asc")n=String(d.d.product_name?.[p[1]]||"").localeCompare(String(d.d.product_name?.[q[1]]||""),"zh-TW");
  return n||money(t[6])-money(s[6])||Number(s[0])-Number(t[0])||p[0]-q[0];
}
export async function searchProductsPage(opts={}){
  const d=await load(),page=Math.max(1,Math.floor(Number(opts.page)||1));
  const size=Math.max(1,Math.min(50,Math.floor(Number(opts.pageSize)||50)));
  const found=[];
  for(const s of d.shops)for(const p of s[11])if(matchProduct(d,s,p,opts))found.push([s,p]);
  found.sort((a,b)=>compareProducts(d,a,b,opts.sort,opts.kind));
  if(typeof opts.onCount==="function"&&(!opts.shouldCount||opts.shouldCount()))opts.onCount(found.length);
  const rows=await Promise.all(found.slice((page-1)*size,page*size).map(async([s,p])=>{
    const {products,store}=await readShop(s);
    const product=products[Number(p[0])];
    if(!product)throw Error("Missing canonical product ordinal");
    return productCard(s,p,product,store,d,opts.location);
  }));
  return {rows,total:found.length,hasNext:page*size<found.length};
}
function storeView(d,s,location){
  return {store_id:Number(s[0]),store_uuid:d.ids.get(Number(s[0])),
    store_name:s[1],name:s[1],city:s[2],locality:s[3],
    rating_value:s[6],rating:s[6],review_count:Number(s[7]||0),
    total_menu_items:s[11].length,latitude:s[4],longitude:s[5],
    street_address:s[12],address:s[12],order_action_url:s[13],
    first_seen:s[9],last_seen:s[10],
    distance_km:location?.enabled?distance(Number(location.latitude),Number(location.longitude),
      Number(s[4]),Number(s[5])):null};
}
function compareStores(a,b,sort){
  if(sort==="reviews_desc")return money(b[7])-money(a[7])||money(b[6])-money(a[6]);
  if(sort==="items_desc")return b[11].length-a[11].length;
  if(sort==="name_asc")return String(a[1]||"").localeCompare(String(b[1]||""),"zh-TW");
  return money(b[6])-money(a[6])||money(b[7])-money(a[7]);
}
export async function searchStoresPage({page=1,pageSize=50,keyword="",city="",newOnly=true,sort="rating_desc",location=null}={}){
  const d=await load(),words=String(keyword).trim().split(/\s+/).filter(Boolean);
  const found=d.shops.filter(s=>s[8]&&s[1]&&nearby(s,location)&&
    (!newOnly||newStore(s,d.baseline,d.cutoff))&&
    (!city||city==="全部"||String(s[2]||"").includes(city))&&
    words.every(w=>norm([s[1],s[3],s[12]].join(" ")).includes(norm(w))));
  found.sort((a,b)=>compareStores(a,b,sort)||Number(a[0])-Number(b[0]));
  const p=Math.max(1,Math.floor(Number(page)||1)),n=Math.max(1,Math.min(50,Number(pageSize)||50));
  return {rows:found.slice((p-1)*n,p*n).map(s=>storeView(d,s,location)),
    total:found.length,hasNext:p*n<found.length};
}
export async function metadata(){return (await load()).meta;}
export async function fastSummary(){return (await load()).summary;}
export async function hasWebIndex(){await load();return true;}
export async function refreshCatalog(){state=null;loading=null;return load();}
export async function searchStores({keyword="",location=null,newOnly=false,limit=50000}={}){
  const out=[];for(let page=1;page<=30&&out.length<limit;page++){
    const r=await searchStoresPage({keyword,location,newOnly,page});
    out.push(...r.rows);if(!r.hasNext)break;
  }
  return out.slice(0,Math.min(limit,1500));
}
export async function directory(location=null,limit=50000){return searchStores({location,limit});}
export async function getStoresInLocation(location){
  const stores=await searchStores({location});
  return {key:JSON.stringify(location||{}),storeIds:new Set(stores.map(s=>s.store_id)),stores};
}
export async function searchPacked({keyword="",city="",promo=false,newOnly=false,minDiscount=null,minArchiveDiscount=null,location=null,limit=50000}={}){
  const rows=[],n=Math.min(Math.max(Number(limit)||50,0),1500);
  for(let page=1;rows.length<n;page++){
    const r=await searchProductsPage({kind:promo?"promos":newOnly?"newProducts":"global",
      keyword,city,location,page,minDiscount:minDiscount||0});
    rows.push(...r.rows);if(!r.hasNext)break;
  }
  return rows.filter(p=>minArchiveDiscount==null||money(p.archive_discount_pct)>=Number(minArchiveDiscount)).slice(0,n);
}
export async function loadPackedDashboard(){throw Error("Unbounded dashboard reads are disabled");}
export async function packedCurrentProduct(storeUuid,productUuid){
  const d=await load(),s=d.byUuid.get(String(storeUuid));
  if(!s)return null;
  const {products,store}=await readShop(s);
  const index=products.findIndex(p=>String(p.product_uuid)===String(productUuid));
  if(index<0)return null;
  const p=s[11].find(p=>Number(p[0])===index);
  return p?productCard(s,p,products[index],store,d,null):null;
}
export async function packedHistory(storeUuid,productUuid){
  const d=await load(),s=d.byUuid.get(String(storeUuid));
  if(!s)return [];
  const {products}=await readShop(s);
  const ordinal=products.findIndex(p=>String(p.product_uuid)===String(productUuid));
  if(ordinal<0)return [];
  const sql="SELECT payload FROM price_history WHERE store="+queryStoreKey(String(storeUuid));
  const rows=await d.db.query(sql);
  if(rows.length!==1)throw Error("Missing immutable store price evidence");
  const variants=unpack(rows[0].payload).find(group=>Number(group[0])===ordinal)?.[1]||[];
  const events=[];
  for(const [fields,mask] of variants){
    const [menu,kind,buy,free,received,,payable]=fields;
    if(!(menu>0&&received>0&&payable>0))continue;
    let bits=0n;
    for(let i=mask.length-1;i>=0;i--)bits=(bits<<8n)|BigInt(mask[i]);
    for(let pos=0;bits>0n;pos++,bits>>=1n){
      if(!(bits&1n))continue;
      const pair=d.observations[pos];
      if(!pair)throw Error("Price mask refers to an unknown batch");
      const state={price:menu/100,quantity:Number(received),
        promo_type:kind===0?"無":kind===1?"買"+buy+"送"+free:"直接折價",
        effective_price:payable/(100*received),
        price_evidence_status:"VERIFIED_MENU",menu_quote_valid:true,
        price_valid:false,delivery_available:false};
      events.push({event_time:pair[1],event_type:"MENU_QUOTE",new_state:JSON.stringify(state)});
    }
  }
  return events.sort((a,b)=>String(a.event_time).localeCompare(String(b.event_time)));
}
export function normalizePriceHistoryEvents(events){return (events||[]).slice().sort(
  (a,b)=>String(a.event_time||"").localeCompare(String(b.event_time||"")));}
export function geoTileKeys(location,cellDegrees=0.025){
  if(!location?.enabled)return [];
  const lat=Number(location.latitude),lon=Number(location.longitude),radius=Number(location.radiusKm);
  if(![lat,lon,radius].every(Number.isFinite))return [];
  const delta=radius/111,keys=[];
  for(let a=Math.floor((lat-delta)/cellDegrees);a<=Math.floor((lat+delta)/cellDegrees);a++)
    for(let b=Math.floor((lon-delta)/cellDegrees);b<=Math.floor((lon+delta)/cellDegrees);b++)
      keys.push(a+":"+b);
  return keys;
}
