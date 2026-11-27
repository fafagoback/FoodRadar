/** Browser test must use the real public HF SQLite from a local test origin. */
const {chromium}=require("playwright");
(async()=>{
  const browser=await chromium.launch({headless:true,args:["--no-sandbox"]});
  const page=await browser.newPage({viewport:{width:1280,height:880}});
  const logs=[],errors=[];
  page.on("console",m=>logs.push(m.type()+": "+m.text()));
  page.on("pageerror",e=>errors.push(String(e)));
  page.on("requestfailed",r=>errors.push(r.url()+": "+r.failure()?.errorText));
  try{
    await page.goto("http://127.0.0.1:8765/relational-v8-preview.html",
      {waitUntil:"domcontentloaded",timeout:30000});
    await page.waitForFunction(()=>{
      const text=document.getElementById("state")?.textContent||"";
      return text.includes("查詢耗時")||text.includes("沒有符合條件")||text.includes("查詢失敗");
    },null,{timeout:120000});
    let status=await page.locator("#state").innerText();
    const coldDuration=await page.locator("#state").innerText();
    const coldRows=await page.locator("#rows tr").count();
    console.log("FR_SQLITE_COLD_SEARCH=",JSON.stringify({status,coldRows}));
    if(/失敗|Unavailable|無法/.test(status))throw Error("Initial SQL query failed: "+status);
    await page.locator("#keyword").fill("雞排");
    await page.locator("#search").click();
    await page.waitForFunction(()=>{
      const t=document.getElementById("state")?.textContent||"";
      return (t.includes("查詢耗時")||t.includes("查詢失敗")||t.includes("沒有符合條件"))&&!t.includes("正在");
    },null,{timeout:90000});
    status=await page.locator("#state").innerText();
    const rows=await page.locator("#rows tr").count();
    if(status.includes("失敗"))throw Error(status);
    if(rows===0)throw Error("Expected real keyword search matches: "+status);
    console.log("FR_HF_SQLITE_REAL_BROWSER=",JSON.stringify({status,rows,coldDuration,errors:errors.slice(0,5),phaseLogs:logs.filter(x=>x.includes("FR_SQL_"))}));
    await page.screenshot({path:"fr-hf-sqlite-browser.png",fullPage:true});
    if(errors.length)console.log("BROWSER_REQUEST_DIAGNOSTICS=",errors.slice(0,12));
  }catch(e){
    await page.screenshot({path:"fr-hf-sqlite-failure.png",fullPage:true}).catch(()=>{});
    console.error("BROWSER_SQLITE_TEST_FAILED",e.message,JSON.stringify({errors,logs:logs.slice(-30)}));
    process.exitCode=1;
  }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1});
