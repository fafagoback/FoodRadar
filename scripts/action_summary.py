"""FoodRadar GitHub Actions 繁體中文執行摘要：只使用實際工作產物及工作狀態。"""
from __future__ import annotations
import json
import os
import re
import shutil
import sys
from pathlib import Path


def read(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None


def latest(pattern):
    files = sorted(Path(".").glob(pattern))
    return read(files[-1]) if files else None


def integer(value):
    try:
        return format(int(value), ",")
    except (ValueError, TypeError):
        return "未取得"


def ratio(ok, total):
    try:
        return f"{100 * int(ok) / int(total):.2f}%" if int(total) else "未取得"
    except (ValueError, TypeError, ZeroDivisionError):
        return "未取得"


def mb(value):
    try:
        return f"{int(value) / 1048576:,.2f} MB"
    except (ValueError, TypeError):
        return "未取得"


def result(value):
    return {"success": "成功", "failure": "失敗", "skipped": "未執行",
            "cancelled": "已取消", "in_progress": "進行中"}.get(value or "", "尚未確認")


def capture():
    target = Path("action-reports/metrics")
    target.mkdir(parents=True, exist_ok=True)
    for pattern, name in [
        ("data/*/shard-reconciliation.json", "shard-reconciliation.json"),
        ("data/*/static-public/current.json", "current.json"),
        ("pages-static-source/current.json", "current.json"),
        ("data/*/store-identity-report.json", "store-identity-report.json"),
        ("data/*/direct-db-report.json", "direct-db-report.json"),
    ]:
        matches = sorted(Path(".").glob(pattern))
        if matches:
            shutil.copyfile(matches[-1], target / name)
    print("已整理這次執行實際產生的指標檔案。")


def main(kind):
    if kind == "capture":
        capture()
        return
    rows = []
    messages = []
    def add(label, value):
        if value is None:
            value = "未取得"
        rows.append((label, str(value).replace("|", "／").replace("\n", " ")))
    def catalog(m, prefix):
        if not isinstance(m, dict):
            add(prefix + "資料統計", "未取得有效資料包")
            return
        add("資料批次", m.get("batch"))
        counts = m.get("counts") or {}
        add(prefix + "店家數", integer(counts.get("stores")))
        add(prefix + "商品數", integer(counts.get("products")))
        add(prefix + "資料大小", mb(m.get("bytes")))
        summary = m.get("summary") or {}
        if "newProducts" in summary:
            add("近七日新增商品數", integer(summary["newProducts"]))
        if "promotions" in summary:
            add("促銷商品數", integer(summary["promotions"]))
    title = {"ci": "品質測試", "crawl": "01｜資料爬取與更新",
             "pages": "02｜網站部署"}[kind]
    add("本次執行編號", os.environ.get("GITHUB_RUN_NUMBER"))
    add("執行紀錄", "[開啟此次執行](https://github.com/" +
        os.environ.get("GITHUB_REPOSITORY", "fafagoback/FoodRadar") +
        "/actions/runs/" + os.environ.get("GITHUB_RUN_ID", "") + ")")
    if kind == "ci":
        path = Path("action-reports/python-tests.log")
        content = path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
        matches = re.findall(r"Ran (\d+) tests? in", content)
        add("單元測試數", integer(matches[-1]) if matches else "未取得")
        add("單元測試結果", "通過" if matches and re.search(r"(?m)^OK\b", content) else
            ("未通過" if matches else "未取得"))
        add("前端程式測試", "通過" if Path("action-reports/frontend.ok").exists() else "未確認通過")
        add("工作流程設定檢查", "通過" if Path("action-reports/workflows.ok").exists() else "未確認通過")
        add("品質測試結果", result(os.environ.get("CI_RESULT")))
        m = read("action-reports/live-manifest.json")
        if m:
            catalog(m, "目前網站")
        add("網站部署", "本流程沒有部署網站")
    elif kind == "crawl":
        add("店家清單掃描", result(os.environ.get("DISCOVER_RESULT")))
        add("分工抓取", result(os.environ.get("SHARD_RESULT")))
        if os.environ.get("RECOVERY_PLAN_RESULT") == "success":
            add("需故障接手的批次", integer(os.environ.get("RECOVERY_COUNT")))
            if os.environ.get("RECOVERY_COUNT") != "0":
                add("故障接手結果", result(os.environ.get("RECOVERY_RESULT")))
        add("資料彙整與發布", result(os.environ.get("INGEST_RESULT")))
        m = read("action-reports/metrics/shard-reconciliation.json")
        if m:
            total = m.get("captured_stores")
            good = m.get("verified_stores")
            add("本次發現店家數", integer(m.get("feed_union")))
            add("本次抓取店家數", integer(total))
            add("成功抓取店家數", integer(good))
            add("抓取失敗店家數", integer(m.get("failed_stores")))
            if m.get("complete") is True:
                add("本輪店家完整性", "全部驗證通過")
            elif m.get("partial_replayable") is True:
                add("本輪店家完整性", "部分店家未通過：隔離失敗店家，保留既有可信資料")
                messages.append("本輪只允許更新已完整驗證的店家；未通過的店家不採用本次商品或價格，不算作成功抓取。")
            else:
                add("本輪店家完整性", "未通過發布條件")
            for failure in (m.get("failed_store_details") or [])[:20]:
                if isinstance(failure, dict):
                    add("未驗證店家 " + str(failure.get("uuid", "")), failure.get("error"))
            add("店家抓取成功率", ratio(good, total))
            add("原始網路回應數", integer(m.get("raw_responses")))
            add("實際請求次數", integer(m.get("sum_api_requests")))
            if "worker_artifact_uploads" in m:
                add("爬蟲工作機成果上傳次數", integer(m["worker_artifact_uploads"]))
            for row in m.get("worker_timing", []):
                add("工作機 %02d" % row["worker"],
                    "店家 %s；API %s；上傳 %s；爬取 %s 秒；領取任務 %s 秒；無工作等待 %s 秒" %
                    (row["stores"], row["requests"], row["uploads"], row.get("worker_seconds","—"),
                     row.get("claim_seconds","—"), row.get("idle_queue_seconds","—")))
            add("流量限制異常次數", integer(m.get("sum_429_events")))
            add("漏抓店家數", integer(m.get("owner_discovery_gap")))
            add("爬蟲工作機數", integer(m.get("runners")))
            add("同時工作機上限", integer(m.get("max_parallel_runners", 15)))
            add("有效執行緒學習樣本", integer(m.get("learning_samples")))
            add("本次最佳執行緒平均", integer(m.get("learned_initial_threads")))
        else:
            messages.append("本次沒有完整分工抓取統計；重新發布或前段失敗時，不以歷史數量冒充本次抓取成果。")
        m = read("action-reports/metrics/current.json")
        catalog(m, "產出資料")
        direct=read("action-reports/metrics/direct-db-report.json")
        if direct:
            add("精簡資料庫總容量",mb(direct.get("compact_bytes")))
            add("單店暫存記憶體資料庫峰值",mb(direct.get("max_shop_scratch_bytes")))
            add("資料庫與網站完整處理秒數",direct.get("seconds"))
        succeeded = os.environ.get("INGEST_RESULT") == "success" and isinstance(m, dict)
        add("資料更新結果", "已完成驗證並產出新的可部署資料包" if succeeded else
            "未完成可確認的資料更新")
        add("網站部署結果", "尚待網站部署" if succeeded else "本次無法宣稱網站部署成功")
        messages.append("資料爬取與網站部署是兩個不同流程；網站成功上線與否，請以「FoodRadar 02｜網站部署」的最終摘要為準。")
    elif kind == "pages":
        deploy = os.environ.get("DEPLOY_RESULT")
        add("資料更新確認", "使用既有已驗證資料" if os.environ.get("GATE_RESULT") == "skipped"
            else result(os.environ.get("GATE_RESULT")))
        add("網站發布作業", result(deploy))
        m = read("action-reports/metrics/manifest.json")
        catalog(m, "網站")
        version = read("action-reports/metrics/version.json")
        if isinstance(version, dict):
            add("部署的資料批次", version.get("batch"))
            add("網站版本", version.get("version"))
        complete = deploy == "success"
        add("最終部署結論", "**網站已成功部署**" if complete else
            "**網站未成功部署**")
        add("網站網址", "[開啟 FoodRadar](https://fafagoback.github.io/FoodRadar/)")
        messages.append("網站發布及線上版號核對成功，即判定部署成功。")
    lines = ["## FoodRadar " + title, "", "| 指標 | 實際結果 |", "|---|---|"]
    lines.extend("| " + key + " | " + value + " |" for key, value in rows)
    if messages:
        lines.extend(["", "### 補充說明"])
        lines.extend("- " + message for message in messages)
    data = "\n".join(lines) + "\n"
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as stream:
            stream.write(data)
    print(data)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("capture", "ci", "crawl", "pages"):
        raise SystemExit("請指定執行報告類型")
    main(sys.argv[1])
