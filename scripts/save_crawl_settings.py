"""Publish the common starting value after all worker reports are validated."""
import json
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from adaptive_threads import maximum_best, valid_threads
from sb_task_pool import SupabaseQueue

def settings_queue():
    return SupabaseQueue(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"],
                         os.environ.get("GITHUB_RUN_ATTEMPT", "1"))

def read_setting(queue):
    value = queue._rpc("threads_read")["optimal_threads"]
    if not valid_threads(value):
        raise ValueError("invalid Supabase thread setting")
    return value

def main():
    files = sorted(Path("data/parts").glob("foodradar-raw-shard-*/shard.json"))
    count = int(os.environ.get("FOODRADAR_BATCH_COUNT", "15"))
    if len(files) != count:
        raise SystemExit(f"需要 {count} 份工作機報告")
    manifests = [json.loads(p.read_text()) for p in files]
    if {m.get("shard_index") for m in manifests} != set(range(count)):
        raise SystemExit("工作機批次編號缺少或重複")
    if any(m.get("run_id") != os.environ["GITHUB_RUN_ID"] or
           m.get("source_sha") != os.environ["GITHUB_SHA"] for m in manifests):
        raise SystemExit("工作機報告不屬於本次執行")
    # Worker identity and assembly are validated before publishing learning.
    # HF upload / website publication is intentionally independent.
    reports = [m.get("thread_learning") or {} for m in manifests]
    # The controller only assigns optimal_threads after a valid busy window.
    print("有效最佳值：" + json.dumps([r.get("optimal_threads") for r in reports]))
    best = maximum_best(reports)
    if best is None:
        print("樣本不足，保留原始設定")
        return
    queue = settings_queue()
    prev = read_setting(queue)
    if best == prev:
        print(f"Supabase 最佳執行緒仍為 {best}")
        return
    saved = queue._rpc("threads_write", optimal_threads=best)["optimal_threads"]
    if saved != best or read_setting(queue) != best:
        raise RuntimeError("Supabase thread setting verification failed")
    print(f"Supabase 最佳執行緒：{prev} → {best}")

if __name__ == "__main__":
    if sys.argv[1:] == ["--read"]:
        print(read_setting(settings_queue()))
    elif not sys.argv[1:]:
        main()
    else:
        raise SystemExit("usage: save_crawl_settings.py [--read]")
