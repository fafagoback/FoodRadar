/** Verified read-only HF SQLite Range/CORS client; no client secrets or UE API. */
const BASE="https://huggingface.co/datasets/hub-google/FoodRadar/resolve/main/";
const FORMAT="foodradar-relational-overlay-v9";
let opening=null;
export async function sqlite(){
  if(opening)return opening;
  opening=(async()=>{
    const pointer=window.UBER_RADAR_CONFIG?.HF_SQLITE_POINTER||"DB/current.json";
    if(!["DB/current.json","DB/preview.json"].includes(pointer))
      throw Error("Unexpected database pointer");
    const res=await fetch(BASE+pointer,{cache:"no-store",mode:"cors"});
    if(!res.ok)throw Error("HF database metadata HTTP "+res.status);
    const meta=await res.json();
    if(meta.format!==FORMAT||
       !/^DB\/previews\/\d{14}\/[a-f0-9]{16}\/catalog\.db$/.test(meta.path)||
       !Number.isSafeInteger(meta.size)||meta.size<=0||
       ![1024,2048,4096,8192].includes(meta.page_size))
      throw Error("Invalid relational SQLite release");
    const url=BASE+meta.path;
    const response=await fetch(url,{headers:{Range:"bytes=0-1023"},mode:"cors"});
    if(response.status!==206||
       response.headers.get("Content-Range")!=="bytes 0-1023/"+meta.size)
      throw Error("HF byte-range read/CORS unavailable");
    const {createDbWorker}=await import("./vendor/relational-vfs.mjs");
    const worker=await createDbWorker([{from:"inline",config:{
      serverMode:"full",url,requestChunkSize:meta.page_size
    }}],new URL("./vendor/sqlite.worker.js",import.meta.url).href,
       new URL("./vendor/sql-wasm.wasm",import.meta.url).href);
    const [batch,counts]=await Promise.all([
      worker.db.query("SELECT value FROM relational_meta WHERE key='batch'"),
      worker.db.query("SELECT value FROM relational_meta WHERE key='counts'")
    ]);
    if(JSON.parse(batch[0]?.value||"null")!==meta.batch)
      throw Error("SQLite source batch and pointer mismatch");
    meta.counts=JSON.parse(counts[0]?.value||"null");
    if(!meta.counts?.links||!meta.counts?.stores)
      throw Error("Empty relational source");
    let tail=Promise.resolve();
    const query=sql=>{
      const pending=tail.then(()=>worker.db.query(sql));
      tail=pending.catch(()=>{});
      return pending;
    };
    window.__FR_SQLITE_RELEASE__={batch:meta.batch,bytes:meta.size,
      products:meta.counts.links,stores:meta.counts.stores};
    return {query,meta};
  })().catch(err=>{opening=null;throw err;});
  return opening;
}
