/** FR live HTTP Range SQLite preview. Public data only; no HF credentials.
 * Designed to fail explicitly if HF cannot serve browser byte ranges.
 */
import {createDbWorker} from "./vendor/relational-vfs.mjs";
import {decompress} from "https://cdn.jsdelivr.net/npm/fzstd@0.1.1/+esm";
import {decode} from "https://cdn.jsdelivr.net/npm/@msgpack/msgpack@3.1.3/+esm";

const BASE="https://huggingface.co/datasets/hub-google/FoodRadar/resolve/main/";
const MANIFEST_URL=BASE+"DB/preview.json";
const enc=new TextEncoder();
let client=null,version=null,allNames=null;
const nameCache=new Map();
const loadedBlocks=new Set();
const byId=id=>document.getElementById(id);
const message=s=>{byId("state").textContent=s;};
function integer(v,fallback=1){const n=Number(v);return Number.isInteger(n)&&n>=0?n:fallback;}
function encodeSql(v){return "'"+String(v??"").replaceAll("'","''")+"'";}
function normalize(v){return String(v||"").normalize("NFKC").toLocaleLowerCase().replace(/\s+/g,"");}
function terms(v){
  const s=normalize(v);if(!s)return [];
  if(s.length>=3)return [...new Set(Array.from({length:s.length-2},(_,i)=>"t:"+s.slice(i,i+3)))];
  if(s.length===2)return ["b:"+s];return ["u:"+s];
}
async function hashBucket(term){
  const buf=new Uint8Array(await crypto.subtle.digest("SHA-256",enc.encode(term)));
  return ((buf[0]<<8)|buf[1])%2048;
}
function expandDeltas(a){let n=0;return a.map(d=>(n+=d));}
function intersection(arrays){
  if(!arrays.length)return [];
  arrays.sort((a,b)=>a.length-b.length);
  let ids=new Set(arrays[0]);
  for(const data of arrays.slice(1)){
    const next=new Set(data);
    ids=new Set([...ids].filter(x=>next.has(x)));
    if(ids.size===0)break;
  }
  return [...ids];
}
async function loadNames(tokenIds){
  const blocks=[...new Set(tokenIds.filter(Number.isInteger).map(i=>Math.floor(i/128)))]
      .filter(i=>!loadedBlocks.has(i));
  if(blocks.length){
    const rows=await client.db.query("SELECT block,payload FROM relational_name_blocks "+
       "WHERE block IN ("+blocks.join(",")+")");
    for(const row of rows){
      const offset=Number(row.block)*128;
      const words=decode(decompress(row.payload));
      for(let i=0;i<words.length;i++)nameCache.set(offset+i,words[i]);
      loadedBlocks.add(Number(row.block));
    }
    if(blocks.some(i=>!loadedBlocks.has(i)))
      throw new Error("Missing product-name block");
  }
  return tokenIds.map(i=>nameCache.get(i)||"");
}
async function searchNameIds(phrase){
  const tokens=terms(phrase);
  if(!tokens.length)return null;
  if(tokens[0].startsWith("u:")){
    // Single-character search uses the existing compressed global text
    // dictionary. No additional unbounded SQL index for one-character keys.
    if(!allNames){
      const d=await client.db.query("SELECT payload FROM dictionary WHERE key='product_name'");
      allNames=decode(decompress(d[0].payload));
    }
    return allNames.map((s,i)=>normalize(s).includes(normalize(phrase))?i:-1)
      .filter(i=>i>=0);
  }
  const buckets=await Promise.all(tokens.map(hashBucket));
  const unique=[...new Set(buckets)];
  const rows=await client.db.query("SELECT bucket,payload FROM relational_terms "+
    "WHERE kind='product' AND bucket IN ("+unique.join(",")+")");
  const decoded=new Map(rows.map(row=>[Number(row.bucket),decode(decompress(row.payload))]));
  return intersection(tokens.map((t,i)=>expandDeltas(decoded.get(buckets[i])?.[t]||[])));
}
function distance(lat1,lon1,lat2,lon2){
  const rad=Math.PI/180;
  const a=Math.sin((lat2-lat1)*rad/2)**2+
    Math.cos(lat1*rad)*Math.cos(lat2*rad)*
    Math.sin((lon2-lon1)*rad/2)**2;
  return 12742.0176*Math.asin(Math.sqrt(Math.min(1,a)));
}
async function connect(){
  if(client)return;
  const manifestResponse=await fetch(MANIFEST_URL,{mode:"cors",cache:"no-store"});
  if(!manifestResponse.ok)throw new Error("HF manifest "+manifestResponse.status);
  const meta=await manifestResponse.json();
  if(meta.format!=="foodradar-relational-overlay-v7" ||
     !/^DB\/previews\/\d{14}\/[a-f0-9]{16}\/catalog\.db$/.test(meta.path) ||
     !Number.isSafeInteger(meta.size)||meta.size<=0)
    throw new Error("Unsafe or unsupported FR SQL release");
  const url=BASE+meta.path;
  const resp=await fetch(url,{mode:"cors",headers:{Range:"bytes=0-1023"}});
  const range=resp.headers.get("Content-Range")||"";
  if(resp.status!==206||!range.startsWith("bytes 0-1023/")||
     Number(range.split("/")[1])!==meta.size)
    throw new Error("HF HTTP Range/CORS unavailable");
  const workerUrl=new URL("./vendor/sqlite.worker.js",import.meta.url).href;
  const wasmUrl=new URL("./vendor/sql-wasm.wasm",import.meta.url).href;
  message("正在建立唯讀 SQLite 連線…");
  client=await createDbWorker([{from:"inline",config:{
    serverMode:"full",url,requestChunkSize:meta.page_size
  }}],workerUrl,wasmUrl);
  const fmt=await client.db.query("SELECT value FROM relational_meta WHERE key='format'");
  if(JSON.parse(fmt?.[0]?.value||"null")!==meta.format)
    throw new Error("SQLite 內部版本與 HF manifest 不相符");
  version=meta;
  byId("release").textContent="版本："+meta.batch+
    "｜SQLite "+(meta.size/1048576).toFixed(2)+" MiB";
  message("SQLite 已連線，商品名稱與價格透過 HF 直接讀取");
}
async function search(){
  await connect();
  const start=performance.now();
  const keyword=byId("keyword").value.trim();
  const city=byId("city").value.trim();
  const page=Math.max(1,integer(byId("page").value,1));
  const termsFound=keyword?await searchNameIds(keyword):null;
  if(termsFound && !termsFound.length){
    byId("rows").replaceChildren();
    message("沒有符合條件的商品");return;
  }
  const wh=["sp.active=1","rs.active=1"];
  if(city)wh.push("instr(rs.city,"+encodeSql(city)+")>0");
  if(termsFound){
    // Exact keyword verification follows the ngram prefilter, preventing
    // false matches when trigram terms appear in a different order.
    await loadNames(termsFound);
    const exact=termsFound.filter(i=>normalize(nameCache.get(i)).includes(normalize(keyword)));
    if(exact.length>12000)throw new Error("搜尋範圍過廣，請增加關鍵字");
    if(!exact.length){byId("rows").replaceChildren();message("沒有符合條件的商品");return;}
    wh.push("rp.name_token IN ("+exact.join(",")+")");
  }
  const lat=Number(byId("lat").value),lon=Number(byId("lon").value);
  const radius=Number(byId("radius").value);
  const geoEnabled=byId("lat").value.trim()!==""&&byId("lon").value.trim()!=="";
  if(geoEnabled){
    if(!Number.isFinite(lat)||!Number.isFinite(lon)||!Number.isFinite(radius)||
       Math.abs(lat)>90||Math.abs(lon)>180||radius<=0)
       throw new Error("經緯度或公里數無效");
    const dLat=radius/111.195,dLon=radius/Math.max(0.1,111.195*Math.cos(lat*Math.PI/180));
    wh.push("rs.lat BETWEEN "+(lat-dLat)+" AND "+(lat+dLat));
    wh.push("rs.lon BETWEEN "+(lon-dLon)+" AND "+(lon+dLon));
  }
  const count=51;
  const sql="SELECT rp.name_token,rs.name store_name,rs.city,rs.lat,rs.lon,"+
    "rs.rating,p.original_cents,p.effective_cents,sp.deal,sp.promotion,"+
    "sp.discount_pct,hex(rs.uuid) store_uuid,hex(sp.sku) sku "+
    "FROM relational_stores rs "+
    "CROSS JOIN relational_store_products sp ON sp.store_id=rs.id "+
    "JOIN relational_products rp ON rp.id=sp.product_id "+
    "JOIN relational_prices p ON p.id=sp.price_id "+
    "WHERE "+wh.join(" AND ")+
    " ORDER BY rs.rating DESC,rs.id ASC,sp.sku ASC LIMIT "+count+
    " OFFSET "+((page-1)*50);
  const rows=(await client.db.query(sql)).slice(0,50);
  await loadNames(rows.map(r=>Number(r.name_token)));
  const target=byId("rows");target.replaceChildren();
  for(const row of rows){
    const tr=document.createElement("tr");
    const values=[nameCache.get(Number(row.name_token))||"",row.store_name,row.city,
      "$"+(Number(row.original_cents)/100).toLocaleString("zh-TW"),
      Number(row.discount_pct||0).toFixed(1)+"%",
      geoEnabled?distance(lat,lon,Number(row.lat),Number(row.lon)).toFixed(2)+" km":"—"];
    for(const val of values){const td=document.createElement("td");td.textContent=val;tr.append(td);}
    target.append(tr);
  }
  message("直接查詢 HF SQLite，取得 "+rows.length+" 筆，查詢耗時 "+
          Math.round(performance.now()-start)+" ms");
}
byId("search").addEventListener("click",()=>search().catch(e=>message("查詢失敗："+e.message)));
search().catch(e=>message("初始化失敗："+e.message));
