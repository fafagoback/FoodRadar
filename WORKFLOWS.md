# FoodRadar GitHub Actions 生產流程

正式只保留三條 GitHub Actions Workflow。價格及商品原始資料仍以 Hugging Face 為來源，Turso 是已驗證的網站查詢資料庫，GitHub Pages 提供前端靜態網站。

| Workflow | 檔案 | 觸發時機 | 主要任務 |
|---|---|---|---|
| FoodRadar 01｜品質測試 | `.github/workflows/ci.yml` | PR、main 相關程式碼 Push、手動 | Python/JS 回歸、價格契約、GitHub Actions 語法檢查；**不寫 HF/Turso** |
| FoodRadar 02｜爬蟲與資料庫 | `.github/workflows/crawl.yml` | 臺灣 06:30／18:30、自動補抓、手動 | 原生 API → HF → SQLite V2 → 七日價格驗證 → Turso 交易式發布 → 唯讀驗收 |
| FoodRadar 03｜網站部署 | `.github/workflows/pages.yml` | web 程式更新、02 **成功完成**、手動 | 讀取 Turso 已發布批次 → 產生 version.json → 部署 Pages → 驗證公開版號、瀏覽器實機檢查 |

## 正常路徑

```text
Pull Request → 01 品質測試（禁止部署未驗證的改動）
                            ├─ 提交程式到 main，改動 web/** → 03 網站部署
每日 06:30／18:30 → 02 爬蟲 → HF 原始 JSON
                          → V2 可信歷史/SQLite
                          → Turso（正式驗證成功才算完成）
                          → 03 網站部署 → 正式版號及 Chromium 驗收
每日 07:30／19:30 → 02 檢查是否漏跑 → 若未漏跑則不抓取
```

03 的 `workflow_run` 只接受 **02 且成功、來源為 main** 的完成事件；失敗或取消的 02 不能觸發網站發布。03 也可以單獨更新網站，不依賴 Python 爬蟲工作目錄的 `data/latest.txt`：`scripts/prepare_pages.py` 必須使用 **Turso 的已發布 `latest_batch` metadata** 產生版號。

## 手動復原

從 02 選 `Run workflow`：

- `crawl`：立即抓一輪並發布；不可自動依靠改 commit 訊息觸發正式爬蟲。
- `republish`：不重新呼叫 Uber API；從最新已存 HF Raw 重新處理並向 Turso 發布。
- `full-rebuild`：重新驗證、重播 HF 歷史並重建 Turso。必須在 `confirm_reset` 輸入 `REBUILD_V2`，否則在資料庫寫入前失敗。保留既有完整重播 checksum／驗證證明門檻。

03 可獨立 `Run workflow` 重新部署網站；**不會重新抓取或清空資料庫**。

## 保障、維運及失敗行為

- V2 只允許 `NONE`、`SAME_SKU_BXGY`、`DIRECT_DISCOUNT` 的已驗證價格；排除 BOGA 及跨商品優惠；任何未驗證七日降價不能發布。
- 02 的資料庫驗證、Turso 寫入、Turso 唯讀驗收依序執行；失敗時整個 Workflow 失敗，不會觸發 03。
- 排程補抓只在 Turso 檢查確認漏跑時繼續。02 共用一個 publisher concurrency group，禁止並行修改資料庫。
- 03 有獨立部署 concurrency，新的網站版本可以取代舊版部署，**不會取消正在執行的爬蟲／資料庫發布**。
- 02 每次寫入前再跑 Python 回歸測試；01 在 PR/Push 獨立提早測試。01 必須通過才能認定變更已驗收，若 GitHub 未設定分支保護仍須遵守人工合併管制。
- 03 保留前端 JavaScript 測試及實際瀏覽器驗收。若資料庫寫入成功但網頁部署失敗，**只重跑 03**，不重建 HF/Turso。
- 刪除一次性 Costco、萊爾富、豆漿等特殊商品稽核 Job；如需專案專項稽核，應以一次性腳本或測試用例進行，不以正式工作流程執行。
- Cloudflare Worker 並非現行 GitHub Pages 網站的必要元件，因此不設部署 Workflow。

### 注意

`workflow_run` 只能在 GitHub 預設分支找到 02、03 對應工作流程後觸發；首次分工提交後，03 的網站改動 Push 會獨立部署，以驗證整套新版連線。下個排程的 02 成功後會再驗證跨 Workflow 自動部署。
