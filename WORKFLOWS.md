# FoodRadar 正式資料與部署流程

正式 GitHub Actions 只保留兩條 Workflow，避免資料未完成就重複部署。

| 名稱 | 檔案 | 觸發時機 | 成功條件 |
|---|---|---|---|
| FoodRadar 01｜資料爬取與更新 | `.github/workflows/crawl.yml` | 每天臺灣時間 06:30、18:30；相關核心程式碼更新；必要時人工從該 Workflow 啟動 | Python/JavaScript 回歸測試 → 原生 Uber API 原始資料完整性 → SQLite V2 → 七日降價與反覆價格判斷 → 靜態索引 → `foodradar-static-source` Artifact |
| FoodRadar 02｜網站部署 | `.github/workflows/pages.yml` | **只有 01 成功完成後**，透過 `workflow_run` 自動啟動 | 僅下載同一次 01 的 Artifact → 建置搜尋與地理索引 → 部署 GitHub Pages → 核對正式網頁版本 → Chromium 電腦及手機實測 |

## 資料權威與發布

- Uber Feed、Store、Catalog 回應保存到 HF `Raw/`，原始資料不可覆寫；HF 僅作歷史原始資料保存與重播。
- SQLite `product_price_observations` 是原生 V2 已驗證的實質單價來源。
- `src/verified_weekly.py` 是**唯一正式七日降價發布入口**；`src/price_oscillation.py` 統一處理 A→B→A 反覆價格。
- `scripts/audit_deal_pipeline.py` 為發布前獨立稽核，使用串流資料避免一次載入全國商品。
- `scripts/prototype_pack_db.py`、`scripts/build_static_catalog.py`、`scripts/prepare_pages.py` 建立網站的唯讀靜態搜尋資料。
- 網站只使用 GitHub Pages 靜態 Artifact，不使用 Turso 或 Cloudflare Worker 執行查詢。

## 發布與失敗保護

1. 01 先執行回歸測試，失敗即停止，不發布新資料。
2. 爬蟲與原始資料驗證、七日計價、索引打包全部成功後，才產出來源 Artifact。
3. 02 僅在 01 成功且來源 commit 仍為 main 最新 commit 時部署；不得使用其他批次的舊資料包。
4. 02 網站部署完成後執行真實 Chromium 網頁驗收，失敗時不得宣稱結案。
5. 新程式碼在 GitHub 以單次提交觸發一次正式工作流程，避免反覆推送造成 Actions 失敗洗版。
6. 每項商品仍保留原始價格；發生 A→B→A 只暫停其七日降價聲明，直到爭議高價不再影響七日參考價。

## 舊相容程式

`src/json_to_db.py`、`src/export_static_snapshots.py`、
`src/alert_engine.py` 仍有舊版本測試依賴；它們**不是目前正式網站的資料發布流程**。
未完成逐一替代與測試前，不直接刪除，以免破壞回歸驗收。

舊 `audit-results/` 屬歷史大型衍生報告，已從 Git 目前版本移除；新的報告以 GitHub Actions Summary 和短期 Artifact 提供。
