"""Enable SQL-only site after its entire verified data corpus is inside HF SQLite."""
import argparse
import shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def configure(pointer,root):
    if pointer not in ("DB/preview.json","DB/current.json"):
        raise ValueError("Unsupported HF pointer")
    web=Path(root)
    cfg=web/"config.js"
    contents=cfg.read_text(encoding="utf-8")
    if "ENABLE_STATIC: true" not in contents:
        raise ValueError("Original site contract unexpectedly changed")
    if "ENABLE_SQLITE" in contents:
        import re
        contents=re.sub(r"ENABLE_SQLITE:\s*(true|false)","ENABLE_SQLITE: true",contents)
        contents=re.sub(r"HF_SQLITE_POINTER:\s*['\"][^'\"]+['\"]",
                        "HF_SQLITE_POINTER: '"+pointer+"'",contents)
    else:
        contents=contents.replace("ENABLE_STATIC: true",
             "ENABLE_STATIC: true, ENABLE_SQLITE: true, HF_SQLITE_POINTER: '"+pointer+"'",1)
    if not (web/"vendor"/"relational-vfs.mjs").is_file():
        raise RuntimeError("SQL worker bundle missing")
    cfg.write_text(contents,encoding="utf-8")
    # Remove the *deployed* third product copy. Only HF Raw + one HF SQLite remain.
    data=web/"data"
    if data.exists():
        if not (data/"manifest.json").exists():
            raise RuntimeError("Never delete unverified website data")
        shutil.rmtree(data)
    print("FR_SQLITE_SITE="+pointer+"; independent Pages data removed")

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--pointer",choices=("DB/preview.json","DB/current.json"),required=True)
    p.add_argument("--web-dir",default=str(ROOT/"web"))
    args=p.parse_args()
    configure(args.pointer,args.web_dir)
