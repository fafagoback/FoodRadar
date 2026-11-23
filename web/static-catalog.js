/* FoodRadar static V2: nationwide, bounded client downloads; no Turso/Worker. */
const DATA_URL="./data/";
const MAX_QUERY_BYTES=4*1024*1024;
const MAX_BLOB_BYTES=2*1024*1024;
const MAX_DECOMPRESSED_BYTES=8*1024*1024;
const MAX_CACHE_BYTES=24*1024*1024;
const COL=Object.freeze({SID:0,PID:1,CITY:2,CATEGORY:3,DISC:4,SAVINGS:5,
  UNIT:6,RATING:7,NEW:8,PROMO:9,DROP:10,PLATFORM:11,LAT:12,LON:13,
  TEXT:14,PROMOTYPE:15,NAME:16});
const decoder=new TextDecoder();
let manifestPromise=null, zstdPromise=null, cacheBytes=0;
const cache=new Map();
function normalize(s){return String(s||"").normalize("NFKC").toLocaleLowerCase().replace(/\s+/g,"");}
function queryTerms(s){
  return [...new Set(String(s||"").trim().split(/\s+/).filter(Boolean).flatMap(part=>{
    const x=normalize(part);
    if(x.length>=3)return Array.from({length:x.length-2},(_,i)=>"t:"+x.slice(i,i+3));
    return x.length===2?["b:"+x]:x?["u:"+x]:[];
  }))];
}
async function bucket(term,count){
  const h=new Uint8Array(await crypto.subtle.digest("SHA-256",new TextEncoder().encode(String(term))));
  return count>256?(((h[0]<<8)|h[1])%count):h[0]%count;
}
function budget(){return {bytes:0};}
function tooBroad(){return new Error("查詢範圍過大，為保護手機流量與效能，請增加關鍵字或選擇縣市後再搜尋。");}
async function boundedBytes(response, max, usage=null){
  if(!response.ok)throw new Error("FoodRadar dataset HTTP "+response.status);
  const length=Number(response.headers?.get?.("content-length")||0);
  if(length>max || (usage&&length+usage.bytes>MAX_QUERY_BYTES))throw tooBroad();
  if(!response.body?.getReader){
    const bytes=new Uint8Array(await response.arrayBuffer());
    if(bytes.byteLength>max || (usage&&(usage.bytes+=bytes.byteLength)>MAX_QUERY_BYTES))throw tooBroad();
    return bytes;
  }
  const reader=response.body.getReader(), parts=[];let total=0;
  try {
    while(true){
      const {done,value}=await reader.read();
      if(done)break;
      total+=value.length;
      if(usage)usage.bytes+=value.length;
      if(total>max || (usage&&usage.bytes>MAX_QUERY_BYTES))throw tooBroad();
      parts.push(value);
    }
  }catch(error){await reader.cancel().catch(()=>{});throw error;}
  const out=new Uint8Array(total);let offset=0;
  for(const part of parts){out.set(part,offset);offset+=part.length;}
  return out;
}
async function zstd(){if(!zstdPromise)zstdPromise=import("https://cdn.jsdelivr.net/npm/fzstd@0.1.1/+esm");return zstdPromise;}
async function current(){
  if(!manifestPromise)manifestPromise=(async()=>{
    const r=await fetch(DATA_URL+"manifest.json",{cache:"no-store"});
    const bytes=await boundedBytes(r,5*1024*1024);
    const m=JSON.parse(decoder.decode(bytes));
    if(m.schema!=="foodradar-static-v2"||m.metadata?.application!=="FoodRadar")
      throw new Error("FoodRadar 分頁索引尚未發布，請稍後重新整理。");
    return m;
  })().catch(e=>{manifestPromise=null;throw e;});
  return manifestPromise;
}
async function readBlob(hash,usage=null){
  if(!/^[a-f0-9]{64}$/.test(String(hash||"")))throw new Error("Invalid FoodRadar blob digest");
  if(cache.has(hash))return cache.get(hash).value;
  const promise=(async()=>{
    const response=await fetch(DATA_URL+"blobs/"+hash+".zst",{cache:"force-cache"});
    const compressed=await boundedBytes(response,MAX_BLOB_BYTES,usage);
    const digest=await crypto.subtle.digest("SHA-256",compressed);
    const actual=Array.from(new Uint8Array(digest),b=>b.toString(16).padStart(2,"0")).join("");
    if(actual!==hash)throw new Error("FoodRadar static checksum mismatch");
    const {decompress}=await zstd();
    const decompressed=decompress(compressed);
    if(decompressed.byteLength>MAX_DECOMPRESSED_BYTES)throw tooBroad();
    const result=JSON.parse(decoder.decode(decompressed));
    const entry=cache.get(hash);
    if(entry){
      entry.weight=decompressed.byteLength;
      cacheBytes+=entry.weight;
      for(const [key,item] of cache){
        if(cacheBytes<=MAX_CACHE_BYTES)break;
        if(item.weight&&key!==hash){cache.delete(key);cacheBytes-=item.weight;}
      }
    }
    return result;
  })().catch(e=>{cache.delete(hash);throw e;});
  cache.set(hash,{value:promise,weight:0});
  return promise;
}
function distanceKm(latA,lonA,latB,lonB){
  if(![latA,lonA,latB,lonB].every(Number.isFinite))return Infinity;
  const r=Math.PI/180,x=(latB-latA)*r,y=(lonB-lonA)*r;
  const z=Math.sin(x/2)**2+Math.cos(latA*r)*Math.cos(latB*r)*Math.sin(y/2)**2;
  return 12742*Math.asin(Math.min(1,Math.sqrt(z)));
}
function inLocation(lat,lon,location){
  if(!location?.enabled)return true;
  return distanceKm(Number(location.latitude),Number(location.longitude),
    lat==null?NaN:Number(lat),lon==null?NaN:Number(lon))<=Number(location.radiusKm);
}
function storeView(s,location){
  const lat=s.latitude==null?NaN:Number(s.latitude),lon=s.longitude==null?NaN:Number(s.longitude);
  return {store_id:Number(s.store_id),store_uuid:s.store_uuid,store_name:s.name,name:s.name,
    city:s.city,locality:s.locality,rating_value:s.rating==null?null:Number(s.rating),
    rating:s.rating==null?null:Number(s.rating),review_count:Number(s.review_count||0),
    total_menu_items:Number(s.menu_item_count||0),latitude:lat,longitude:lon,
    street_address:s.address,address:s.address,order_action_url:s.order_url,
    first_seen:s.first_seen,last_seen:s.last_seen,
    distance_km:location?.enabled?distanceKm(Number(location.latitude),Number(location.longitude),lat,lon):null};
}
function matches(text,keyword){
  return String(keyword||"").trim().split(/\s+/).filter(Boolean).every(w=>text.includes(normalize(w)));
}
function compare(a,b,sort,kind){
  const n=(x,p)=>Number(x[p]||0);
  const o=sort||(kind==="discounts"?"discount_desc":kind==="promos"?"price_asc":
    kind==="newProducts"?"new_desc":"rating_desc");let diff=0;
  if(o==="discount_desc")diff=n(b,COL.DISC)-n(a,COL.DISC)||n(b,COL.SAVINGS)-n(a,COL.SAVINGS);
  else if(o==="savings_desc")diff=n(b,COL.SAVINGS)-n(a,COL.SAVINGS)||n(b,COL.DISC)-n(a,COL.DISC);
  else if(o==="price_asc")diff=n(a,COL.UNIT)-n(b,COL.UNIT)||n(b,COL.RATING)-n(a,COL.RATING);
  else if(o==="price_desc")diff=n(b,COL.UNIT)-n(a,COL.UNIT)||n(b,COL.RATING)-n(a,COL.RATING);
  else if(o==="promo_first")diff=n(b,COL.PROMO)-n(a,COL.PROMO)||n(b,COL.RATING)-n(a,COL.RATING);
  else if(o==="new_desc")diff=n(b,COL.NEW)-n(a,COL.NEW)||n(b,COL.RATING)-n(a,COL.RATING);
  else if(o==="name_asc")diff=String(a[COL.NAME]||"").localeCompare(String(b[COL.NAME]||""),"zh-TW");
  else diff=n(b,COL.RATING)-n(a,COL.RATING)||n(a,COL.UNIT)-n(b,COL.UNIT);
  return diff;
}
function eligible(r,o){
  if(o.keyword&&!matches(r[COL.TEXT],o.keyword))return false;
  if(o.city&&o.city!=="全部"&&!String(r[COL.CITY]||"").includes(o.city))return false;
  if(o.category&&o.category!=="全部"&&!String(r[COL.CATEGORY]||"").includes(o.category))return false;
  if(!inLocation(r[COL.LAT],r[COL.LON],o.location))return false;
  if(o.sort==="promo_only"&&o.kind==="global"&&!r[COL.PROMO])return false;
  if(o.kind==="discounts"||o.promoType==="實質降價30"){
    if(!r[COL.DROP]||Number(r[COL.DISC])<Number(o.minDiscount||30)||
       Number(r[COL.SAVINGS])<Number(o.minSavings||0))return false;
  }else if(o.kind==="newProducts"&&!r[COL.NEW])return false;
  else if(o.kind==="promos"){
    if(!r[COL.PROMO])return false;
    if(o.promoType==="平台優惠"&&!r[COL.PLATFORM])return false;
    if(o.promoType==="買一送一"&&(!r[COL.PLATFORM]||!/買[0-9]+送[0-9]+/.test(r[COL.PROMOTYPE])))return false;
    if(o.promoType==="折"&&(!r[COL.PLATFORM]||r[COL.PROMOTYPE]!=="直接折價"))return false;
    if(["半價","加1元","多入"].includes(o.promoType))return false;
  }
  return true;
}
async function miniRows(ids,usage){
  const m=await current(),size=m.index_chunk_rows,groups=new Map();
  for(const id of ids){
    const k=Math.floor(id/size);
    if(!groups.has(k))groups.set(k,[]);
    groups.get(k).push(id);
  }
  const result=new Map(),keys=[...groups.keys()];
  if(keys.length>150)throw tooBroad();
  for(let i=0;i<keys.length;i+=6){
    await Promise.all(keys.slice(i,i+6).map(async k=>{
      const h=m.files.catalog[k];
      if(!h)throw new Error("Missing catalog shard "+k);
      const rows=await readBlob(h,usage);
      for(const id of groups.get(k)){
        const r=rows[id%size];if(!r)throw new Error("Missing catalog row "+id);
        result.set(id,r);
      }
    }));
  }
  return result;
}
async function cardsForIds(ids,mini=null,location=null,usage=null){
  const m=await current(),size=m.chunk_rows,groups=new Map(),pieces=new Map();
  for(const id of ids){
    const k=Math.floor(id/size);
    if(!groups.has(k))groups.set(k,[]);
    groups.get(k).push(id);
  }
  const keys=[...groups.keys()];
  for(let i=0;i<keys.length;i+=6){
    await Promise.all(keys.slice(i,i+6).map(async k=>{
      const h=m.files.products[k];
      if(!h)throw new Error("Missing card shard "+k);
      pieces.set(k,await readBlob(h,usage));
    }));
  }
  return ids.map(id=>{
    const raw=pieces.get(Math.floor(id/size))?.[id%size];
    if(!raw)throw new Error("Missing card "+id);
    const card={...raw};
    if(card.crawled_time==="__CURRENT_BATCH__")card.crawled_time=m.metadata.latest_processed_at;
    if(location?.enabled){
      const row=mini?.get(id);
      if(row)card.distance_km=distanceKm(Number(location.latitude),Number(location.longitude),
        row[COL.LAT]==null?NaN:Number(row[COL.LAT]),row[COL.LON]==null?NaN:Number(row[COL.LON]));
    }
    return card;
  });
}
// Coordinate-first search: choose only nearby spatial cells before loading products.
export function geoTileKeys(location,cellDegrees=0.025){
  if(!location?.enabled)return [];
  const lat=Number(location.latitude),lon=Number(location.longitude),radius=Number(location.radiusKm);
  if(![lat,lon,radius,cellDegrees].every(Number.isFinite)||
      Math.abs(lat)>90||Math.abs(lon)>180||radius<=0||cellDegrees<=0)
    throw new Error("無效的經緯度或搜尋半徑");
  const latSpan=radius/111.195+0.00001;
  // Use the most poleward latitude in the circle for a conservative longitude bound.
  const cos=Math.cos(Math.min(89.9,Math.abs(lat)+latSpan)*Math.PI/180);
  const lonSpan=radius/(111.195*Math.max(Math.abs(cos),0.01))+0.00001;
  const loLat=Math.floor(Math.max(-90,lat-latSpan)/cellDegrees);
  const hiLat=Math.floor(Math.min(90,lat+latSpan)/cellDegrees);
  const loLon=Math.floor(Math.max(-180,lon-lonSpan)/cellDegrees);
  const hiLon=Math.floor(Math.min(180,lon+lonSpan)/cellDegrees);
  const cellCount=(hiLat-loLat+1)*(hiLon-loLon+1);
  if(cellCount>900)throw tooBroad();
  const keys=[];
  for(let y=loLat;y<=hiLat;y++)for(let x=loLon;x<=hiLon;x++)keys.push(y+":"+x);
  return keys;
}
async function spatialRows(location,usage,table){
  const m=await current(),geo=m.geo;
  if(!geo?.cell_degrees||!geo[table])return null;
  const keys=geoTileKeys(location,geo.cell_degrees);
  const hashes=[...new Set(keys.flatMap(k=>geo[table][k]||[]))];
  const rows=[];
  for(let i=0;i<hashes.length;i+=6){
    const batch=await Promise.all(hashes.slice(i,i+6).map(h=>readBlob(h,usage)));
    for(const list of batch)rows.push(...list);
  }
  return table==="products"?rows.map(([id,fields])=>[id,[null,null,...fields]]):rows;
}

async function keywordCandidates(keyword,usage,city=""){
  const m=await current(),terms=queryTerms(keyword);
  const bucketIds=[...new Set(await Promise.all(terms.map(t=>bucket(t,m.search_buckets))))];
  const shards=await Promise.all(bucketIds.map(async id=>[id,m.files.search[String(id)]?await readBlob(m.files.search[String(id)],usage):{}]));
  const byId=new Map(shards);
  const lists=await Promise.all(terms.map(async term=>{
    const id=await bucket(term,m.search_buckets);
    const record=byId.get(id)?.[term]||[];
    if(Array.isArray(record))return record;
    const selected=city&&city!=="全部"&&record.cities?.[city]?
      [record.cities[city]]:Object.values(record.cities||{});
    const count=selected.reduce((sum,part)=>sum+part.count,0);
    if(count>100000)throw tooBroad();
    const output=[];
    for(const part of selected){
      for(const hash of part.chunks||[])output.push(...await readBlob(hash,usage));
    }
    return output;
  }));
  lists.sort((a,b)=>a.length-b.length);
  if(!lists.length||!lists[0]?.length)return [];
  if(lists[0].length>100000)throw tooBroad();
  let ids=lists[0];
  for(let k=1;k<lists.length;k++){
    const set=new Set(lists[k]);
    ids=ids.filter(id=>set.has(id));
    if(!ids.length)break;
  }
  return ids;
}
async function browseChunk(entry,at,usage){
  if(!entry.positions)return readBlob(entry.chunks[at],usage);
  const m=await current(),base=m.files.browse[entry.base];
  if(!base)throw new Error("Missing regional sort parent");
  const deltas=await readBlob(entry.positions[at],usage),positions=[];
  let total=0;
  for(let i=0;i<deltas.length;i++){
    total=i===0?deltas[i]:total+deltas[i];
    positions.push(total);
  }
  const groups=new Map(),resolved=new Map(),size=m.browse_chunk_rows;
  for(const position of positions){
    const k=Math.floor(position/size);
    if(!groups.has(k))groups.set(k,[]);
    groups.get(k).push(position);
  }
  const keys=[...groups.keys()];
  for(let i=0;i<keys.length;i+=6){
    await Promise.all(keys.slice(i,i+6).map(async k=>{
      const ids=await readBlob(base.chunks[k],usage);
      for(const position of groups.get(k))resolved.set(position,ids[position%size]);
    }));
  }
  return positions.map(position=>resolved.get(position));
}
async function browseIds(entry,start,size,usage){
  const m=await current(),end=Math.min(start+size,entry.total),chunkSize=m.browse_chunk_rows;
  if(start>=entry.total)return [];
  const result=[];
  for(let at=Math.floor(start/chunkSize);at<=Math.floor((end-1)/chunkSize);at++){
    const ids=await browseChunk(entry,at,usage);
    const lo=Math.max(start-at*chunkSize,0),hi=Math.min(end-at*chunkSize,ids.length);
    result.push(...ids.slice(lo,hi));
  }
  return result;
}
export async function metadata(){
  const m=await current();
  return {...m.metadata,published_searchable_products:Number(m.counts?.products||0),
    published_searchable_stores:Number(m.counts?.stores||0)};
}
export async function refreshCatalog(){
  manifestPromise=null;cache.clear();cacheBytes=0;
  return current();
}
export async function hasWebIndex(){await current();return true;}
export async function fastSummary(){return (await current()).summary;}
export async function searchProductsPage({
  kind="global",page=1,pageSize=50,keyword="",city="",category="",
  minDiscount=0,minSavings=0,promoType="全部",sort="",location=null,onCount=null,shouldCount=null
}={}){
  const m=await current(),use=budget(),p=Math.max(1,Math.floor(Number(page)||1));
  const size=Math.max(1,Math.min(50,Math.floor(Number(pageSize)||50)));
  let group=kind,order=sort||(kind==="discounts"?"discount_desc":kind==="promos"?"price_asc":
    kind==="newProducts"?"new_desc":"rating_desc");
  if(kind==="discounts"){
    const pct=Number(minDiscount);
    group=[20,30,40,50].includes(pct)?"discounts"+pct:"discounts";
  }
  if(kind==="promos"&&promoType==="實質降價30"){group="discounts30";}
  if(kind==="promos"&&promoType==="平台優惠"){group="promosPlatform";}
  if(kind==="global"&&order==="promo_only"){group="promos";order="rating_desc";}
  const o={kind,keyword,city,category,minDiscount,minSavings,promoType,sort,location};
  const start=(p-1)*size,needed=p*size+1;
  const viewKey=group+":"+order;
  const regional=city&&city!=="全部"?m.files.browse[viewKey+"@"+city]:null;
  const entry=regional||m.files.browse[viewKey];
  if(!entry)throw new Error("Missing paged browse view "+viewKey);
  // A radius lookup MUST NOT traverse nationwide keyword or browse shards.
  if(location?.enabled && m.geo){
    const near=await spatialRows(location,use,"products");
    const rows=new Map(near);
    const ids=near.filter(([id,r])=>eligible(r,o)).map(([id])=>id);
    ids.sort((a,b)=>compare(rows.get(a),rows.get(b),order,kind)||a-b);
    const cards=await cardsForIds(ids.slice(start,start+size),rows,location,use);
    if(typeof onCount==="function"&&(!shouldCount||shouldCount()))onCount(ids.length);
    return {rows:cards,total:ids.length,hasNext:start+size<ids.length};
  }
  if(keyword.trim()){
    const candidates=await keywordCandidates(keyword,use,city);
    const rows=await miniRows(candidates,use);
    const ids=candidates.filter(id=>eligible(rows.get(id),o));
    ids.sort((a,b)=>compare(rows.get(a),rows.get(b),order,kind)||a-b);
    const cards=await cardsForIds(ids.slice(start,start+size),rows,location,use);
    if(typeof onCount==="function"&&(!shouldCount||shouldCount()))onCount(ids.length);
    return {rows:cards,total:ids.length,hasNext:start+size<ids.length};
  }
  const exact=(!location?.enabled && (!city||city==="全部"||Boolean(regional)) &&
    (!category||category==="全部") && Number(minSavings)===0 &&
    (kind!=="discounts"||group==="discounts"+Number(minDiscount)) &&
    (kind!=="promos"||["全部","實質降價30","平台優惠"].includes(promoType)) &&
    (kind!=="global"||sort!=="promo_only"||group==="promos"));
  if(exact){
    const ids=await browseIds(entry,start,size,use);
    const cards=await cardsForIds(ids,null,null,use);
    if(typeof onCount==="function"&&(!shouldCount||shouldCount()))onCount(entry.total);
    return {rows:cards,total:entry.total,hasNext:start+size<entry.total};
  }
  // Filtered lists stop at the requested page, never scan an entire national catalog.
  const selected=[];let scanned=0;
  const nChunks=(entry.positions||entry.chunks).length;
  for(let chunk=0;chunk<nChunks;chunk++){
    if(selected.length>=needed)break;
    const ids=await browseChunk(entry,chunk,use);
    for(let at=0;at<ids.length&&selected.length<needed;at+=48){
      const slice=ids.slice(at,at+48),rows=await miniRows(slice,use);
      for(const id of slice){
        if(eligible(rows.get(id),o))selected.push(id);
        scanned++;
      }
    }
  }
  const ids=selected.slice(start,start+size),meta=await miniRows(ids,use);
  return {rows:await cardsForIds(ids,meta,location,use),total:scanned>=entry.total?selected.length:null,
    hasNext:selected.length>start+size||scanned<entry.total};
}
async function storeKeywordCandidates(keyword,use){
  const m=await current(),terms=queryTerms(keyword);
  const keys=[...new Set(await Promise.all(terms.map(t=>bucket(t,m.store_search_buckets))))];
  const parts=await Promise.all(keys.map(async id=>[id,m.files.store_search[String(id)]?
    await readBlob(m.files.store_search[String(id)],use):{}]));
  const index=new Map(parts),sets=[];
  for(const term of terms){
    const id=await bucket(term,m.store_search_buckets);
    sets.push(index.get(id)?.[term]||[]);
  }
  sets.sort((a,b)=>a.length-b.length);
  if(!sets.length||!sets[0]?.length)return [];
  if(sets[0].length>15000)throw tooBroad();
  let ids=sets[0];
  for(let i=1;i<sets.length;i++){
    const valid=new Set(sets[i]);
    ids=ids.filter(id=>valid.has(id));
    if(!ids.length)break;
  }
  return ids;
}
async function storeRecords(ids,use){
  const m=await current(),groups=new Map(),rows=new Map(),size=m.store_chunk_rows;
  for(const id of ids){
    const k=Math.floor(id/size);
    if(!groups.has(k))groups.set(k,[]);
    groups.get(k).push(id);
  }
  const keys=[...groups.keys()];
  if(keys.length>150)throw tooBroad();
  for(let pos=0;pos<keys.length;pos+=6){
    await Promise.all(keys.slice(pos,pos+6).map(async k=>{
      const hash=m.files.store_catalog[k],part=await readBlob(hash,use);
      for(const id of groups.get(k)){
        const s=part[id%size];
        if(s)rows.set(id,s);
      }
    }));
  }
  return ids.map(id=>rows.get(id)).filter(Boolean);
}
function newStore(s,m){
  const baseline=String(m.metadata?.baseline_processed_at||"").slice(0,10);
  const latest=new Date(m.metadata?.latest_processed_at);
  const first=new Date(s.first_seen);
  return String(s.first_seen||"").slice(0,10)>baseline &&
    Number.isFinite(latest.getTime())&&Number.isFinite(first.getTime())&&
    first.getTime()>=latest.getTime()-7*86400000;
}
function compareStore(a,b,sort){
  const n=(s,k)=>Number(s[k]||0);
  if(sort==="reviews_desc")return n(b,"review_count")-n(a,"review_count")||
    n(b,"rating")-n(a,"rating");
  if(sort==="items_desc")return n(b,"menu_item_count")-n(a,"menu_item_count");
  if(sort==="name_asc")return String(a.name||"").localeCompare(String(b.name||""),"zh-TW");
  return n(b,"rating")-n(a,"rating")||n(b,"review_count")-n(a,"review_count");
}

export async function searchStoresPage({
  page=1,pageSize=50,keyword="",city="",newOnly=true,sort="rating_desc",location=null
}={}){
  const m=await current(),use=budget(),p=Math.max(1,Math.floor(Number(page)||1));
  const size=Math.max(1,Math.min(50,Math.floor(Number(pageSize)||50)));
  if(location?.enabled && m.geo){
    const nearby=await spatialRows(location,use,"stores");
    const found=nearby.filter(store=>store.status==="active"&&store.name&&
      (!newOnly||newStore(store,m))&&(!city||city==="全部"||String(store.city||"").includes(city))&&
      (!keyword.trim()||matches(normalize([store.name,store.locality,store.address].join(" ")),keyword))&&
      inLocation(store.latitude,store.longitude,location));
    found.sort((a,b)=>compareStore(a,b,sort)||String(a.store_uuid).localeCompare(String(b.store_uuid)));
    const offset=(p-1)*size;
    return {rows:found.slice(offset,offset+size).map(store=>storeView(store,location)),
      total:found.length,hasNext:offset+size<found.length};
  }
  if(keyword.trim()){
    const ids=await storeKeywordCandidates(keyword,use);
    const all=await storeRecords(ids,use);
    const found=all.filter(s=>s.status==="active"&&s.name&&
      (!newOnly||newStore(s,m))&&(!city||city==="全部"||String(s.city||"").includes(city))&&
      matches(normalize([s.name,s.locality,s.address].join(" ")),keyword)&&
      inLocation(s.latitude,s.longitude,location));
    found.sort((a,b)=>compareStore(a,b,sort)||String(a.store_uuid).localeCompare(String(b.store_uuid)));
    const start=(p-1)*size;
    return {rows:found.slice(start,start+size).map(s=>storeView(s,location)),
      total:found.length,hasNext:start+size<found.length};
  }
  const viewKey=(newOnly?"new":"all")+":"+sort;
  const regional=city&&city!=="全部"?m.files.store_browse[viewKey+"@"+city]:null;
  const entry=regional||m.files.store_browse[viewKey];
  if(!entry)throw new Error("Missing store view");
  const start=(p-1)*size,needed=p*size+1;
  const filtered=Boolean(keyword||(city&&city!=="全部"&&!regional)||location?.enabled);
  if(!filtered){
    const data=[];
    for(let at=Math.floor(start/256);at<=Math.floor(Math.min(entry.total-1,start+size-1)/256);at++){
      if(entry.total<=start)break;
      const part=await readBlob(entry.chunks[at],use);
      data.push(...part.slice(Math.max(0,start-at*256),Math.min(part.length,start+size-at*256)));
    }
    return {rows:data.map(s=>storeView(s,location)),total:entry.total,hasNext:start+size<entry.total};
  }
  const selected=[];let scanned=0;
  for(const hash of entry.chunks){
    if(selected.length>=needed)break;
    const chunk=await readBlob(hash,use);
    for(const s of chunk){
      if((!city||city==="全部"||String(s.city||"").includes(city))&&
         (!keyword||matches(normalize([s.name,s.locality,s.address].join(" ")),keyword))&&
         inLocation(s.latitude,s.longitude,location))selected.push(s);
    }
    scanned+=chunk.length;
  }
  return {rows:selected.slice(start,start+size).map(s=>storeView(s,location)),
    total:scanned===entry.total?selected.length:null,
    hasNext:selected.length>start+size||scanned<entry.total};
}
export async function searchStores({keyword="",location=null,newOnly=false,limit=50000}={}){
  const output=[];let page=1;
  while(output.length<limit){
    const result=await searchStoresPage({page,pageSize:50,keyword,newOnly,location});
    output.push(...result.rows);
    if(!result.hasNext||page>=30)break;
    page++;
  }
  return output.slice(0,Math.min(limit,1500));
}
export async function directory(location=null,limit=50000){return searchStores({location,limit});}
export async function getStoresInLocation(location){
  const stores=await searchStores({location});
  return {key:JSON.stringify(location||{}),storeIds:new Set(stores.map(s=>s.store_id)),stores};
}
export async function searchPacked({keyword="",city="",promo=false,newOnly=false,
  minDiscount=null,minArchiveDiscount=null,location=null,limit=50000}={}){
  // Legacy diagnostic callers are bounded; never reconstruct the national database.
  const output=[],max=Math.min(Math.max(0,Number(limit)||50),1500);
  for(let page=1;output.length<max;page++){
    const response=await searchProductsPage({
      kind:promo?"promos":newOnly?"newProducts":"global",page,pageSize:50,
      keyword,city,minDiscount:minDiscount||0,location});
    output.push(...response.rows);
    if(!response.hasNext)break;
  }
  return (minArchiveDiscount===null?output:output.filter(p=>
    Number(p.archive_discount_pct||0)>=Number(minArchiveDiscount))).slice(0,max);
}
export async function loadPackedDashboard(){
  throw new Error("Unbounded dashboard reads are disabled; use paginated search");
}
export async function packedCurrentProduct(storeUuid,productUuid){
  const m=await current(),use=budget(),id=await bucket(String(storeUuid)+"\u001f"+String(productUuid),m.lookup_buckets);
  const hash=m.files.lookup[String(id)];
  if(!hash)return null;
  const lookup=await readBlob(hash,use),rowId=lookup[String(storeUuid)+"\u001f"+String(productUuid)];
  if(rowId===undefined)return null;
  const cards=await cardsForIds([rowId],null,null,use);
  return cards[0];
}
export async function packedHistory(storeUuid,productUuid){
  const m=await current(),use=budget(),id=await bucket(String(storeUuid)+"\u001f"+String(productUuid),m.history_buckets);
  const hash=m.files.history[String(id)];
  if(!hash)return [];
  const events=await readBlob(hash,use);
  return normalizePriceHistoryEvents(events[String(storeUuid)+"\u001f"+String(productUuid)]||[]);
}

export function normalizePriceHistoryEvents(events) {
  // A promotion change can change unit cost even when the raw menu price is stable.
  const priority = { MENU_QUOTE: 5, BASELINE: 0, NEW: 1, PRICE_OBSERVED: 2, PROMOTION_CHANGED: 3, PRICE_CHANGED: 4 };
  const priceEvents = (events || []).filter(e =>
    Object.prototype.hasOwnProperty.call(priority, e.event_type));

  // Some older products have no NEW event: reconstruct the pre-change price
  // only when there is no real price observation at that timestamp.
  const firstChange = priceEvents.find(e => e.event_type === 'PRICE_CHANGED' && e.old_state);
  if (firstChange) {
    try {
      const old = JSON.parse(firstChange.old_state);
      const oldTime = old.last_seen || old.first_seen;
      // Never reconstruct a baseline from a later off-hours observation.
      if (old.price_valid === true && oldTime && oldTime === old.first_seen &&
          !priceEvents.some(e => String(e.event_time) === String(oldTime))) {
        priceEvents.push({
          event_time: oldTime, new_state: firstChange.old_state,
          old_state: null, event_type: 'BASELINE'
        });
      }
    } catch (_) {
      // Malformed archived state cannot add a synthetic baseline.
    }
  }

  const byTime = new Map();
  for (const event of priceEvents) {
    const time = String(event.event_time || '');
    if (!time) continue;
    let state;
    try { state = JSON.parse(event.new_state || event.old_state || '{}'); }
    catch (_) { continue; }
    if (state.price_valid !== true && state.menu_quote_valid !== true) continue;
    if (state.menu_quote_valid !== true && state.is_open !== 1 && state.is_open !== true) continue;
    if (!(Number(state.price) > 0)) continue;
    const previous = byTime.get(time);
    // Prefer complete original HF evidence over sparse event snapshots.
    // A legacy unit-only quote is lower confidence than a verified event.
    const rank = event.event_type === 'MENU_QUOTE' && state.effective_unit_only === true
      ? 0 : priority[event.event_type];
    let priorRank = -1;
    if (previous) {
      const priorState = JSON.parse(previous.new_state || previous.old_state || '{}');
      priorRank = previous.event_type === 'MENU_QUOTE' &&
        priorState.effective_unit_only === true ? 0 : priority[previous.event_type];
    }
    if (!previous || rank > priorRank) {
      byTime.set(time, event);
    }
  }
  return [...byTime.values()]
    .sort((a, b) => String(a.event_time).localeCompare(String(b.event_time)))
    .slice(-50);
}

// Resolve the actual current comparison; change-event history omits unchanged crawls.

