import assert from "node:assert/strict";

const manifest={
  format:"foodradar-relational-v1",
  batch:"20261127071423",
  path:"DB/versions/20261127071423/catalog.db",size:8192
};
const calls=[];
globalThis.fetch=async(url,opts)=>{
  calls.push({url,opts});
  if(String(url).endsWith("DB/current.json"))
    return {ok:true,status:200,json:async()=>manifest};
  return {ok:true,status:206,headers:{
    get:key=>key==="content-range"?"bytes 0-1023/8192":null
  }};
};
const queries=[];
const worker={db:{exec:async sql=>{
  queries.push(sql);
  if(sql.includes("WHERE key='batch'"))
    return [{columns:["value"],values:[['"20261127071423"']]}];
  if(sql.includes("FROM metadata"))
    return [{columns:["key","value"],values:[["batch",'"20261127071423"']]}];
  if(sql.includes("store_geo"))
    return [{columns:["uuid","latitude","longitude"],values:[["A",25,121.5]]}];
  if(sql.includes("price_observations"))
    return [{columns:["batch_id"],values:[["20261127071423"]]}];
  return [{columns:["product_name"],values:[["套餐"]]}];
}}};
const mod=await import("../web/hf-sqlite-client.js");
const connected=await mod.connectHFSQLite({
  manifestUrl:"https://huggingface.co/datasets/hub-google/Foodradar-TW/resolve/main/DB/current.json",
  createDbWorker:async()=>worker,
  workerUrl:"/sqlite.worker.js",
  wasmUrl:"/sql-wasm.wasm"
});
assert(connected.url.endsWith("/DB/versions/20261127071423/catalog.db"));
assert.equal(calls.length,2);
assert.equal((await mod.sqlMetadata()).batch,"20261127071423");
assert.equal((await mod.sqlProducts({keyword:"O'Reilly"})).rows.length,1);
assert(queries.at(-1).includes("O''Reilly"));
assert.equal((await mod.sqlNearbyStores({latitude:25,longitude:121.5})).length,1);
assert.equal((await mod.sqlHistory("A","P")).length,1);
console.log("PASS: HF SQLite version, Range, SQL quoting, geo and history");
