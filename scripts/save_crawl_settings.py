"""Publish only the common starting value, after a successful RUN."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from adaptive_threads import average_best, valid_threads

def gh(*args):
    return subprocess.check_output(["gh", "api", *args], text=True)

def main():
    files = sorted(Path("data/parts").glob("foodradar-raw-shard-*/shard.json"))
    if len(files) != 15:
        raise SystemExit("需要 15 份工作機報告")
    manifests = [json.loads(p.read_text()) for p in files]
    if any(m.get("run_id") != os.environ["GITHUB_RUN_ID"] or
           m.get("source_sha") != os.environ["GITHUB_SHA"] for m in manifests):
        raise SystemExit("工作機報告不屬於本次執行")
    if any(s.get("success") is not True for m in manifests for s in m["stores"]):
        print("本輪仍有未完成店家，不更新設定")
        return
    best = average_best([m.get("thread_learning") or {} for m in manifests])
    if best is None:
        print("樣本不足，保留原始設定")
        return
    endpoint = f"repos/{os.environ['GITHUB_REPOSITORY']}/contents/crawl-settings.json"
    old = json.loads(gh(endpoint + "?ref=crawler-state"))
    prev = json.loads(base64.b64decode(old["content"]))["optimal_threads"]
    if not valid_threads(prev):
        raise ValueError("invalid stored optimal_threads")
    if best == prev:
        print(f"最佳執行緒仍為 {best}，不寫入 Repo")
        return
    content = base64.b64encode((json.dumps({"optimal_threads": best}, indent=2) + "\n").encode()).decode()
    gh("--method", "PUT", endpoint, "-f", "branch=crawler-state",
       "-f", "sha=" + old["sha"], "-f", "message=更新爬蟲最佳執行緒數",
       "-f", "content=" + content)
    print(f"最佳執行緒：{prev} → {best}")

if __name__ == "__main__":
    main()
