const cors={'Access-Control-Allow-Origin':'https://fafagoback.github.io','Access-Control-Allow-Methods':'GET, OPTIONS','Access-Control-Allow-Headers':'Content-Type'};
const reply=(body,status=200)=>new Response(JSON.stringify(body),{status,headers:{...cors,'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'}});
async function sql(env,statement,args=[]){
  if(!env.TURSO_DATABASE_URL||!env.TURSO_READONLY_TOKEN)throw Error('not configured');
  const url=String(env.TURSO_DATABASE_URL).replace(/^libsql:\/\//,'https://').replace(/\/+$/,'')+'/v2/pipeline';
  const response=await fetch(url,{method:'POST',headers:{'Authorization':'Bearer '+env.TURSO_READONLY_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({requests:[{type:'execute',stmt:{sql:statement,args:args.map(v=>({type:'integer',value:String(v)}))}},{type:'close'}]})});
  if(!response.ok)throw Error('Turso unavailable');
  const body=await response.json(), first=body.results?.[0];
  if(first?.type==='error'||!first?.response?.result)throw Error('Turso query failed');
  const result=first.response.result,keys=result.cols.map(c=>c.name);
  return result.rows.map(row=>Object.fromEntries(row.map((cell,i)=>[keys[i],cell.value])));
}
export default {async fetch(request,env){
  if(request.method==='OPTIONS')return new Response(null,{status:204,headers:cors});
  if(request.method!=='GET')return reply({error:'method_not_allowed'},405);
  const u=new URL(request.url);
  try{
    if(u.pathname==='/api/health'){
      const rows=await sql(env,"SELECT key,value FROM metadata WHERE key='latest_batch'");
      return reply({ok:true,database:'connected',latest_batch:rows[0]?.value||null});
    }
    if(u.pathname==='/api/stores'){
      const limit=Math.max(1,Math.min(50,parseInt(u.searchParams.get('limit')||'50',10)||50));
      const cursor=Math.max(0,parseInt(u.searchParams.get('cursor')||'0',10)||0);
      if(!Number.isSafeInteger(cursor))return reply({error:'invalid_cursor'},400);
      const rows=await sql(env,"SELECT store_id,store_uuid,name,city,locality,rating,review_count FROM store_directory WHERE status='active' AND is_open=1 AND store_id>? ORDER BY store_id LIMIT ?",[cursor,limit+1]);
      const items=rows.slice(0,limit),has_next=rows.length>limit;
      return reply({items,has_next,next_cursor:has_next?items[items.length-1].store_id:null});
    }
    return reply({error:'not_found'},404);
  }catch{return reply({ok:false,error:'database_unavailable'},503);}
}};