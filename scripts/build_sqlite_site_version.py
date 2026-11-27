"""Version the original homepage for a verified single-DB release; no cards on Pages."""
from __future__ import annotations
import argparse,json,os
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
ROOT=Path(__file__).resolve().parents[1]
def prepare(db_batch,web):
    if len(db_batch)!=14 or not db_batch.isdecimal():raise ValueError("Invalid immutable crawl batch")
    try:
        old=json.load(urlopen(Request("https://fafagoback.github.io/FoodRadar/version.json",
            headers={"Cache-Control":"no-cache"}),timeout=25))
    except HTTPError as e:
        if e.code!=404:raise
        old={}
    if str(old.get("batch",""))>db_batch:raise ValueError("Refuse to publish older DB batch")
    sha=os.environ.get("GITHUB_SHA","")
    if len(sha)!=40:raise ValueError("Release requires exact Git SHA")
    version=db_batch+"-sql-"+sha[:12]
    html=(web/"index.html").read_text(encoding="utf-8")
    if html.count("__FOODRADAR_RELEASE__")!=1:raise ValueError("Unexpected site version placeholder")
    (web/"index.html").write_text(html.replace("__FOODRADAR_RELEASE__",version),encoding="utf-8")
    (web/"version.json").write_text(json.dumps({
        "version":version,"batch":db_batch,"commit":sha,"data_revision":"sqlite-v9",
        "storage":"huggingface-single-sqlite"},ensure_ascii=False,indent=2),encoding="utf-8")
    print("FR_SQLITE_VERSION="+version)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--batch",required=True)
    p.add_argument("--web-dir",default=str(ROOT/"web"))
    x=p.parse_args()
    prepare(x.batch,Path(x.web_dir))
