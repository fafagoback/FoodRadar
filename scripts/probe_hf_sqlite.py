"""Probe remote SQLite Range and CORS, not a browser performance benchmark."""
import argparse
import json
from urllib.request import Request,urlopen

def probe(url,origin):
    req=Request(url,headers={
        "Origin":origin,"Range":"bytes=0-1023","Cache-Control":"no-cache"})
    with urlopen(req,timeout=30) as response:
        headers=response.headers
        status=response.status
        data=response.read(1025)
        cors=headers.get("Access-Control-Allow-Origin")
        exposed=headers.get("Access-Control-Expose-Headers","").lower()
        result={
            "http_status":status,
            "final_host_url":response.url.split("?")[0],
            "content_range":headers.get("Content-Range"),
            "access_control_allow_origin":cors,
            "access_control_expose_headers":exposed,
            "bytes_read":len(data)
        }
        result["range_ok"]=status==206 and len(data)<=1024 and bool(result["content_range"])
        result["cors_ok"]=cors in (origin,"*")
        result["expose_ok"]="content-range" in exposed or exposed=="*"
        result["candidate_ok"]=result["range_ok"] and result["cors_ok"] and result["expose_ok"]
        return result

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("url")
    p.add_argument("--origin",default="https://fafagoback.github.io")
    a=p.parse_args()
    result=probe(a.url,a.origin)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if not result["candidate_ok"]:
        raise SystemExit(2)
