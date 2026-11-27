"""獨立 Artifact 清理；只處理已完成的 main RUN，保護最新發布及待部署資料。"""
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CRAWL = ".github/workflows/crawl.yml"
PAGES = ".github/workflows/pages.yml"
RAW = re.compile(r"(?:foodradar-shared-feed(?:-[0-9a-f]{16})?|foodradar-raw-shard-(?:[0-9a-f]{16}-)?\d+|fr-task-[0-9a-f]{16}-[0-9a-f-]{36}-[0-9a-f]{32}|fr-worker-[0-9a-f]{16}-\d+(?:-w\d{2})?|fr-(?:plan|section|page|small)-[0-9a-f]{16}-[0-9a-f-]+)")
CRAWL_REPORTS = {"foodradar-data-validation", "foodradar-run-metrics"}
PAGE_REPORTS = {"fr-single-db-audit"}
RAW_SAVED = "交叉核對店家覆蓋率與原始 HTTP SHA-256，統一上傳 HF Raw"
PUBLISHED = "線上再驗收原完整網站與歷史紀錄"
lock = Lock()
blocked_until = 0.0

def api(path, method="GET"):
    global blocked_until
    base = "https://api.github.com/repos/" + os.environ["GITHUB_REPOSITORY"]
    headers = {"Authorization": "Bearer " + os.environ["GH_TOKEN"],
               "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}
    for attempt in range(5):
        with lock:
            delay = max(0, blocked_until - time.monotonic())
        if delay:
            time.sleep(delay)
        try:
            with urlopen(Request(base + path, headers=headers, method=method), timeout=30) as response:
                body = response.read()
                return json.loads(body) if body else None
        except HTTPError as exc:
            if exc.code == 404:
                return None
            limited = exc.code == 429 or (exc.code == 403 and
                (exc.headers.get("Retry-After") or exc.headers.get("X-RateLimit-Remaining") == "0" or
                 b"rate limit" in exc.read().lower()))
            if not limited and exc.code not in (500, 502, 503, 504):
                raise
            if attempt == 4:
                raise
            retry = exc.headers.get("Retry-After")
            reset = exc.headers.get("X-RateLimit-Reset")
            delay = max(2 ** attempt, int(retry or 0))
            if limited:
                delay = max(delay, 60 if not retry else 0)
                if exc.headers.get("X-RateLimit-Remaining") == "0" and reset:
                    delay = max(delay, int(reset) - time.time() + 1)
                if delay > 180:
                    raise RuntimeError("GitHub API 額度暫時不足；保留剩餘資料包")
                with lock:
                    blocked_until = max(blocked_until, time.monotonic() + delay)
            print("API 暫時受限，退避", delay, "秒", flush=True)
            time.sleep(delay)
        except (URLError, TimeoutError):
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)

def paginated(path, key):
    page = 1
    while True:
        sep = "&" if "?" in path else "?"
        result = api(f"{path}{sep}per_page=100&page={page}")
        if result is None:
            raise RuntimeError("無法取得完整清單：" + path)
        values = result[key]
        yield from values
        if len(values) < 100:
            return
        page += 1

def owner(artifact):
    return (artifact.get("workflow_run") or {}).get("id")

def workflow(run):
    return run.get("path", "").split("@")[0]

def completed_main(run):
    return bool(run and run.get("head_branch") == "main" and run.get("status") == "completed")

def jobs_for(run_id):
    return list(paginated(f"/actions/runs/{run_id}/jobs", "jobs"))

def step_ok(jobs, name):
    return any(step.get("name") == name and step.get("conclusion") == "success"
               for job in jobs for step in (job.get("steps") or []))

def active_deploy():
    # A queued/waiting run also blocks deletion of source/Pages artifacts.
    for workflow in ("pages.yml",):
        for status in ("queued", "in_progress", "waiting", "pending", "requested"):
            data = api(f"/actions/workflows/{workflow}/runs?branch=main&status={status}&per_page=1")
            if data is None or any(str(x["id"])!=os.environ.get("GITHUB_RUN_ID")
                                   for x in data.get("workflow_runs", [])):
                return True
    return False

def active_crawl():
    for status in ("queued", "in_progress", "waiting", "pending", "requested"):
        data = api(f"/actions/workflows/crawl.yml/runs?branch=main&status={status}&per_page=1")
        if data is None or any(str(x["id"])!=os.environ.get("GITHUB_RUN_ID")
                               for x in data.get("workflow_runs", [])):
            return True
    return False

def plan(artifacts, runs, get_jobs, busy):
    """只有已確認發布的版本能作為舊來源包清理基準。"""
    by_run = {}
    for a in artifacts:
        by_run.setdefault(owner(a), []).append(a)
    sources = [r for rid, r in runs.items() if completed_main(r)
               and workflow(r) == CRAWL and r.get("conclusion") == "success"
               and any(a["name"] == "fr-db-source" and a.get("size_in_bytes", 0) > 0
                       for a in by_run.get(rid, []))]
    pages = sorted((r for rid, r in runs.items() if completed_main(r) and workflow(r) in (PAGES, CRAWL)
                    and any(re.fullmatch(r"fr-hf-sqlite-\d+", a["name"])
                            and a.get("size_in_bytes", 0) > 0 for a in by_run.get(rid, []))),
                   key=lambda r: r["created_at"], reverse=True)
    published = None
    cutoff = None
    if not busy:
        for r in pages:
            if step_ok(get_jobs(r["id"]), PUBLISHED):
                published = r
                break
        if published:
            # Historical dispatch inputs are not returned by the Runs API.
            # Protect every possible matching source and all newer sources.
            possible = [r for r in sources if r["head_sha"] == published["head_sha"]
                        and r["created_at"] <= published["created_at"]]
            if possible:
                cutoff = min(r["run_number"] for r in possible)
    targets = {}
    for a in artifacts:
        rid, name = owner(a), a["name"]
        r = runs.get(rid)
        if not completed_main(r):
            continue
        kind = None
        if workflow(r) == CRAWL:
            if RAW.fullmatch(name) and r.get("conclusion") == "cancelled":
                # Successful FR Raw artifacts feed the asynchronous DB workflow;
                # keep their 3-day GitHub retention instead of deleting them
                # before the 15 DB workers have downloaded them.
                kind = "已取消爬蟲暫存包"
            elif cutoff is not None and r["run_number"] < cutoff:
                if name == "foodradar-static-source":
                    kind = "舊網站來源包"
                elif name in CRAWL_REPORTS:
                    kind = "舊報告包"
        elif workflow(r) == PAGES and published and r["created_at"] < published["created_at"]:
            if re.fullmatch(r"fr-hf-sqlite-\d+", name):
                kind = "舊網站部署包"
            elif name in PAGE_REPORTS:
                kind = "舊報告包"
        if kind:
            targets.setdefault(rid, []).append((a, kind))
    return targets, cutoff, published

def main():
    started = time.monotonic()
    dry = os.environ.get("FR_CLEANUP_DRY_RUN", "false").lower() == "true"
    artifacts = [a for a in paginated("/actions/artifacts", "artifacts") if not a["expired"]]
    # No full historical run scan: fetch each relevant run once.
    ids = sorted({owner(a) for a in artifacts if owner(a) is not None})
    runs = {rid: api(f"/actions/runs/{rid}") for rid in ids}
    runs = {rid: r for rid, r in runs.items() if r is not None}
    cache = {}
    def get_jobs(rid):
        if rid not in cache:
            cache[rid] = jobs_for(rid)
        return cache[rid]
    busy = active_deploy() or active_crawl()
    targets, cutoff, published = plan(artifacts, runs, get_jobs, busy)
    deleted, failures, skipped = [], [], []
    counts = {}
    def remove(pair):
        a, kind = pair
        try:
            api(f"/actions/artifacts/{a['id']}", "DELETE")
            return a, kind, None
        except Exception as exc:
            return a, kind, str(exc)
    if not dry:
        with ThreadPoolExecutor(max_workers=4) as pool:
            for rid, group in targets.items():
                # No cleanup during a new run: safest even for cancelled raw fragments.
                if active_deploy() or active_crawl():
                    skipped.extend(a["id"] for a, _ in group)
                    continue
                current = api(f"/actions/runs/{rid}")
                if not completed_main(current) or current["run_attempt"] != runs[rid]["run_attempt"]:
                    skipped.extend(a["id"] for a, _ in group)
                    continue
                for a, kind, error in pool.map(remove, group):
                    if error:
                        failures.append((a["id"], error))
                        print("刪除失敗", a["id"], error, flush=True)
                    else:
                        deleted.append(a)
                        counts[kind] = counts.get(kind, 0) + 1
                        print("已清除", a["id"], a["name"], a["size_in_bytes"], flush=True)
    remaining = [a for a in paginated("/actions/artifacts", "artifacts") if not a["expired"]]
    remaining_ids = {a["id"] for a in remaining}
    # A protected artifact may expire naturally; never classify it as deleted by this script.
    for a in deleted:
        if a["id"] in remaining_ids:
            failures.append((a["id"], "清理後仍存在"))
    mb = lambda values: sum(a["size_in_bytes"] for a in values) / 1048576
    candidates = [a for group in targets.values() for a, _ in group]
    lines = ["## FoodRadar 03｜儲存空間清理", "",
             "| 指標 | 實際結果 |", "|---|---|",
             f"| 模式 | {'僅盤點' if dry else '正式清理'} |",
             f"| 清理前 | {len(artifacts)} 個／{mb(artifacts):.2f} MB |",
             f"| 符合清理條件 | {len(candidates)} 個／{mb(candidates):.2f} MB |",
             f"| 已刪除 | {len(deleted)} 個／{mb(deleted):.2f} MB |",
             f"| 清理後 | {len(remaining)} 個／{mb(remaining):.2f} MB |",
             f"| 因其他任務或重跑而保留 | {len(skipped)} 個 |",
             f"| 刪除失敗 | {len(failures)} 個 |",
             f"| 最新確認發布 RUN | {published['id'] if published else '未確認／保守保留'} |",
             f"| 舊來源清理界線 | {cutoff if cutoff is not None else '未確認／保守保留'} |",
             f"| 執行時間 | {time.monotonic()-started:.1f} 秒 |"]
    lines.extend(f"| {kind} | {count} 個 |" for kind, count in sorted(counts.items()))
    report = "\n".join(lines) + "\n"
    print(report, flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write(report)
    if failures:
        raise SystemExit("部分 Artifact 未能清理；其餘資料已保留")

if __name__ == "__main__":
    main()
