/** Full legacy homepage query contract, backed ONLY by public HF SQLite. */
const {chromium}=require("playwright");
(async()=>{
  const browser=await chromium.launch({headless:true,args:["--no-sandbox"]});
  const page=await browser.newPage({viewport:{width:1280,height:800}});
  const errors=[],oldRequests=[];
  page.on("pageerror",err=>errors.push(String(err)));
  page.on("request",request=>{
    if(/\/data\/(manifest\.json|blobs\/)/.test(request.url()))oldRequests.push(request.url());
  });
  try{
    const root=(process.env.FR_TEST_BASE||"http://127.0.0.1:8765/").replace(/\/?$/,"/");
    await page.goto(root+"index.html",{waitUntil:"domcontentloaded",timeout:30000});
    const result=await page.evaluate(async()=>{
      const client=await import("./static-catalog.js");
      const meta=await client.metadata();
      const stores=await client.searchStoresPage({newOnly:false,page:1});
      const keyword=await client.searchProductsPage({kind:"global",keyword:"雞排",page:1});
      const discounts=await client.searchProductsPage({kind:"discounts",page:1});
      const promotions=await client.searchProductsPage({kind:"promos",page:1});
      const info=window.__FR_SQLITE_RELEASE__||null;
      let current=null,history=null;
      if(keyword.rows.length){
        const r=keyword.rows[0];
        current=await client.packedCurrentProduct(r.store_uuid,r.product_uuid);
        history=await client.packedHistory(r.store_uuid,r.product_uuid);
      }
      return {meta,stores:stores.rows.length,keywords:keyword.rows.length,
        discounts:discounts.rows.length,promotions:promotions.rows.length,
        current:Boolean(current),history:Array.isArray(history),info};
    });
    console.log("FR_SQLITE_FULL_SITE_CONTRACT="+JSON.stringify(result));
    if(!result.info||!result.stores||!result.keywords||!result.current||
       !result.history||result.meta.latest_batch!==result.info.batch)
      throw Error("Single SQLite did not satisfy original page API");
    if(oldRequests.length)throw Error("Website still fetched a third product dataset");
    if(errors.length)throw Error("Uncaught browser errors: "+JSON.stringify(errors.slice(0,4)));
    await page.screenshot({path:"fr-sqlite-full-site.png",fullPage:true});
  }catch(e){
    console.error("FR_SQLITE_MAIN_SITE_FAILED",e.message,JSON.stringify({oldRequests,errors}));
    await page.screenshot({path:"fr-sqlite-full-site-failure.png",fullPage:true}).catch(()=>{});
    process.exitCode=1;
  }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1});
