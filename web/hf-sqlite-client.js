/** Experimental read-only HF SQLite HTTP VFS; NEVER used by production by default.
 * Browser must have HTTPS, CORS and byte-Range support. No HF tokens in JS.
 */
const state={worker:null,manifest:null};
const need=(ok,message)=>{if(!ok)throw new Error(message);};
const esc=s=>String(s??"").replaceAll("'","''");
const num=v=>{const n=Number(v);need(Number.isFinite(n),"Invalid number");return n;};

export async function connectHFSQLite({manifestUrl,createDbWorker,workerUrl,wasmUrl,requestChunkSize=4096}){
  need(!state.worker,"Already connected");
  need(typeof createDbWorker==="function","Missing tested SQLite HTTP VFS worker");
  need(/^https:\/\//.test(manifestUrl),"Manifest requires HTTPS");
  need(workerUrl&&wasmUrl,"WASM assets not bundled");
  const response=await fetch(manifestUrl,{cache:"no-store",mode:"cors"});
  need(response.ok,"HF manifest HTTP "+response.status);
  const meta=await response.json();
  need(meta.format==="foodradar-relational-v1","Unexpected DB format");
  need(/^DB\/versions\/\d{14}\/catalog\.db$/.test(meta.path),"Unsafe DB path");
  need(meta.path.includes("/"+meta.batch+"/"),"DB version mismatch");
  need(Number.isSafeInteger(meta.size)&&meta.size>0,"Invalid DB length");
  const url=new URL(manifestUrl);
  need(url.pathname.endsWith("/DB/current.json"),"Unexpected HF manifest URL");
  url.pathname=url.pathname.slice(0,-"DB/current.json".length)+meta.path;
  url.search="";
  const probe=await fetch(url.href,{mode:"cors",cache:"no-store",
                                  headers:{Range:"bytes=0-1023"}});
  const range=probe.headers.get("content-range")||"";
  need(probe.status===206 && /^bytes 0-1023\/\d+$/.test(range),
       "HF cross-origin byte ranges unavailable; refusing full download");
  need(Number(range.split("/")[1])===meta.size,"HF file size mismatch");
  const worker=await createDbWorker([{from:"inline",config:{
    serverMode:"full",url:url.href,requestChunkSize
  }}],String(workerUrl),String(wasmUrl));
  const boot=await worker.db.exec("SELECT value FROM metadata WHERE key='batch'");
  const rows=(boot?.[0]?.values||[]).map(v=>({value:v[0]}));
  need(rows.length===1 && JSON.parse(rows[0].value)===meta.batch,"Wrong DB version");
  state.worker=worker;state.manifest=meta;
  return {manifest:meta,url:url.href};
}
function db(){need(state.worker,"Not connected to HF SQLite");return state.worker.db;}
async function query(sql){
  const sets=await db().exec(sql);
  need(Array.isArray(sets),"SQLite HTTP VFS returned invalid results");
  return sets.flatMap(set=>(set.values||[]).map(row=>
    Object.fromEntries((set.columns||[]).map((key,i)=>[key,row[i]]))));
}
export async function sqlMetadata(){
  const rows=await query("SELECT key,value FROM metadata");
  return Object.fromEntries(rows.map(({key,value})=>[key,JSON.parse(value)]));
}
export async function sqlProducts({keyword="",city="",page=1,pageSize=50}={}){
  const size=Math.max(1,Math.min(50,Math.trunc(num(pageSize))));
  const offset=(Math.max(1,Math.trunc(num(page)))-1)*size;
  const where=["s.status='active'","sp.status='active'"];
  if(keyword.trim())where.push("instr(lower(p.product_name),lower('"+esc(keyword.trim())+"'))>0");
  if(city.trim())where.push("s.city='"+esc(city.trim())+"'");
  const sql="SELECT s.uuid store_uuid,s.name store_name,s.city,s.latitude,s.longitude,"+
    "p.product_name,p.category,p.description,sp.sku product_uuid,pt.price_cents,"+
    "pt.effective_cents,sp.is_price_deal,sp.discount_pct,sp.last_seen "+
    "FROM store_products sp JOIN stores s ON s.id=sp.store_id "+
    "JOIN products p ON p.id=sp.product_id "+
    "JOIN price_terms pt ON pt.id=sp.price_term_id WHERE "+
    where.join(" AND ")+" ORDER BY s.rating DESC,p.product_name LIMIT "+
    (size+1)+" OFFSET "+offset;
  const rows=await query(sql);
  return {rows:rows.slice(0,size),hasNext:rows.length>size};
}
export async function sqlNearbyStores({latitude,longitude,radiusKm=5,limit=50}={}){
  const lat=num(latitude),lon=num(longitude),km=num(radiusKm);
  need(Math.abs(lat)<=90&&Math.abs(lon)<=180&&km>0,"Invalid location");
  const dy=km/110.5,dx=km/Math.max(1,111.32*Math.cos(lat*Math.PI/180));
  const sql="SELECT s.uuid,s.name,s.latitude,s.longitude,s.rating,s.review_count,"+
    "s.city,s.locality FROM store_geo g JOIN stores s ON g.id=s.id "+
    "WHERE s.status='active' "+
    "AND g.min_lat BETWEEN "+(lat-dy)+" AND "+(lat+dy)+
    " AND g.min_lon BETWEEN "+(lon-dx)+" AND "+(lon+dx)+
    " LIMIT "+Math.min(5000,Math.max(1,Math.trunc(num(limit)))*10);
  const rows=await query(sql);
  const distance=(y,x)=>{
    const r=Math.PI/180,a=Math.sin((y-lat)*r/2)**2+
      Math.cos(lat*r)*Math.cos(y*r)*Math.sin((x-lon)*r/2)**2;
    return 12742.0176*Math.asin(Math.sqrt(Math.min(1,a)));
  };
  return rows.map(s=>({...s,distance_km:distance(s.latitude,s.longitude)}))
    .filter(s=>s.distance_km<=km).sort((a,b)=>a.distance_km-b.distance_km)
    .slice(0,Math.max(1,Math.trunc(num(limit))));
}
export async function sqlHistory(storeUuid,sku){
  const sql="SELECT q.batch_id,q.observed_at,t.price_cents,t.promotion,"+
    "t.buy_quantity,t.free_quantity,t.received_quantity,"+
    "t.original_total_cents,t.payable_total_cents FROM price_observations q "+
    "JOIN stores s ON q.store_id=s.id "+
    "JOIN price_terms t ON q.price_term_id=t.id "+
    "WHERE s.uuid='"+esc(storeUuid)+"' AND q.sku='"+esc(sku)+
    "' ORDER BY q.observed_at DESC LIMIT 300";
  return query(sql);
}
export function currentHFManifest(){return state.manifest;}
