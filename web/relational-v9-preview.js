/** FR read-only relational SQLite v9 - one compact numeric serving index.
 * Product names are NOT replicated in the index; they come from the
 * checkpoint's existing shared product_name dictionary.
 * No HF token, no web-side product snapshot.
 */
import {createDbWorker} from "./vendor/relational-vfs.mjs";
import {decompress} from "https://cdn.jsdelivr.net/npm/fzstd@0.1.1/+esm";
import {decode} from "https://cdn.jsdelivr.net/npm/@msgpack/msgpack@3.1.3/+esm";
const BASE="https://huggingface.co/datasets/hub-google/FoodRadar/resolve/main/";
const NAME="foodradar-relational-overlay-v9";
let loaded=null,loading=null,searching=false;
const el=id=>document.getElementById(id);
const say=s=>{el("state").textContent=s;};
const norm=s=>String(s||"").normalize("NFKC").toLocaleLowerCase().replace(/\s+/g,"");
function distance(lat1,lon1,lat2,lon2){
  const r=Math.PI/180,dlat=(lat2-lat1)*r,dlon=(lon2-lon1)*r;
  const h=Math.sin(dlat/2)**2+Math.cos(lat1*r)*Math.cos(lat2*r)*Math.sin(dlon/2)**2;
  return 12742.0176*Math.asin(Math.sqrt(Math.min(1,h)));
}
async function readIndex(){
  if(loaded)return loaded;
  if(loading)return loading;
  loading=(async()=>{
    const started=performance.now();
    const response=await fetch(BASE+"DB/preview.json",{cache:"no-store",mode:"cors"});
    if(!response.ok)throw Error("HF preview manifest: "+response.status);
    const meta=await response.json();
    if(meta.format!==NAME ||
      !/^DB\/previews\/\d{14}\/[a-f0-9]{16}\/catalog\.db$/.test(meta.path) ||
      !Number.isSafeInteger(meta.size)||meta.size<=0)throw Error("SQLite release is not v9");
    const dbUrl=BASE+meta.path;
    const probe=await fetch(dbUrl,{headers:{Range:"bytes=0-1023"},mode:"cors"});
    const range=probe.headers.get("Content-Range")||"";
    if(probe.status!==206||range!=="bytes 0-1023/"+meta.size)
      throw Error("HF does not allow browser byte-range reads");
    say("正在從 HF SQLite 載入共用商品索引…");
    const worker=await createDbWorker([{from:"inline",config:{
      serverMode:"full",url:dbUrl,requestChunkSize:meta.page_size
    }}],new URL("./vendor/sqlite.worker.js",import.meta.url).href,
         new URL("./vendor/sql-wasm.wasm",import.meta.url).href);
    const fmt=await worker.db.query("SELECT value FROM relational_meta WHERE key='format'");
    if(JSON.parse(fmt[0]?.value||"null")!==NAME)throw Error("SQLite version mismatch");
    console.info("FR_SQLITE_OPEN_MS",Math.round(performance.now()-started));
    const [index,nameDict,prices]=await Promise.all([
      worker.db.query("SELECT payload FROM relational_frontend_index WHERE key='shops'"),
      worker.db.query("SELECT payload FROM dictionary WHERE key='product_name'"),
      worker.db.query("SELECT id,original_cents,effective_cents,quantity,promotion FROM relational_prices")
    ]);
    if(index.length!==1||nameDict.length!==1||!prices.length)
      throw Error("Invalid product reference or price dictionary");
    const shops=decode(decompress(index[0].payload));
    const names=decode(decompress(nameDict[0].payload));
    const priceMap=new Map(prices.map(p=>[Number(p.id),p]));
    const links=shops.reduce((n,s)=>n+s[11].length,0);
    if(links!==meta.counts.links||shops.length!==meta.counts.stores)
      throw Error("Incomplete product link inventory");
    loaded={shops,names,priceMap,meta};
    const ms=Math.round(performance.now()-started);
    const bytes=Number(await worker.worker.bytesRead||0);
    window.__FR_SQLITE_DIAGNOSTICS={initialLoadMs:ms,bytesRead:bytes,
      stores:shops.length,links,totalDatabaseBytes:meta.size,batch:meta.batch};
    console.info("FR_SQLITE_INDEX_READY_MS",ms,"stores",shops.length,"links",links,
      "bytes",bytes);
    el("release").textContent="資料批次："+meta.batch+"｜SQLite "+
      (meta.size/1048576).toFixed(1)+" MiB｜"+links.toLocaleString("zh-TW")+" 筆商品關聯";
    return loaded;
  })().catch(err=>{loading=null;throw err;});
  return loading;
}
function search(){
  if(!loaded)throw Error("SQLite 尚未就緒");
  const at=performance.now();
  const {shops,names,priceMap}=loaded;
  const term=norm(el("keyword").value),city=norm(el("city").value);
  const page=Math.max(1,Math.floor(Number(el("page").value)||1));
  const latStr=el("lat").value.trim(),lonStr=el("lon").value.trim();
  const geographic=latStr!==""&&lonStr!=="";
  const lat=Number(latStr),lon=Number(lonStr);
  const radius=Number(el("radius").value)||5;
  if(geographic&&(!Number.isFinite(lat)||!Number.isFinite(lon)||
      Math.abs(lat)>90||Math.abs(lon)>180||radius<=0))throw Error("經緯度或搜尋半徑無效");
  const min=(page-1)*50,max=min+50;
  const found=[];
  let matches=0;
  for(const s of shops){
    if(!s[8]||(city&&!norm(s[2]).includes(city)))continue;
    const miles=geographic?distance(lat,lon,Number(s[4]),Number(s[5])):null;
    if(geographic&&(!Number.isFinite(miles)||miles>radius))continue;
    for(const p of s[11]){
      // [ordinal,nameToken,categoryToken,priceId,flags,discountPct,savings,firstSeen]
      if(!(Number(p[4])&1))continue;
      const name=names[Number(p[1])]||"";
      if(term&&!norm(name).includes(term))continue;
      const price=priceMap.get(Number(p[3]));
      if(!price)throw Error("Missing referenced product price");
      if(matches>=min&&matches<max)found.push({
        name,store:s[1],city:s[2],original:Number(price.original_cents)/100,
        effective:Number(price.effective_cents)/100,
        discount:Number(p[5]||0),kilometers:miles,
        storeId:s[0],ordinal:p[0],
      });
      matches++;
    }
  }
  const table=el("rows");table.replaceChildren();
  for(const p of found){
    const row=document.createElement("tr");
    const vals=[p.name,p.store,p.city,
      "$"+p.original.toLocaleString("zh-TW"),
      p.discount.toFixed(1)+"%",
      geographic?p.kilometers.toFixed(2)+" km":"—"];
    for(const v of vals){const cell=document.createElement("td");cell.textContent=v;row.append(cell);}
    table.append(row);
  }
  const ms=Math.round(performance.now()-at);
  console.info("FR_SQLITE_BROWSER_SEARCH_MS",ms,"hits",matches,"rows",found.length);
  say("直接從 HF SQLite 索引查詢完成｜"+matches.toLocaleString("zh-TW")+
      " 筆結果｜本機篩選 "+ms+" ms");
}
async function execute(){
  if(searching)return;
  searching=true;el("search").disabled=true;
  try{
    if(!loaded){say("正在載入 HF SQLite…");await readIndex();}
    search();
  }catch(e){say("查詢失敗："+e.message);console.error(e);}
  finally{searching=false;el("search").disabled=false;}
}
el("search").addEventListener("click",execute);
execute();
