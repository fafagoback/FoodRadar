# FoodRadar

正式資料只有兩層：HF 每日完整原始快照，以及 FoodRadar 獨立的 Turso 網頁查詢資料庫。
所有正式抓取、建置、發布由 GitHub Actions 執行；runner 的 SQLite/JSON 是即用即棄中間產物，不依賴使用者電腦。

- 每日台灣時間 06:30 排程，或手動執行 `.github/workflows/crawl.yml`。
- HF：`hub-google/UberEat/FoodRadar/snapshots/<YYYYMMDDhhmmss>/` 保存完整店家 `stores.json`、菜單 JSON 的 `taiwan_menus_<batch>.tar.gz`、完成 manifest；每日完整快照保留，latest 指向最新成功原始批次。Parquet 是由 Raw 衍生、可重建的歷史分析快取。
- Turso：`foodradar-packed-v1`，與 UberEat DB 完全分開。沿用 MessagePack + Zstandard store bundles、search buckets、可查詢店家 directory、事件與版本 metadata。
- 網頁：GitHub Pages，唯讀查詢 Turso；支援特價、近七日新店、全部店家搜尋、全商品搜尋／篩選／分頁、新品、促銷、價格變動歷史。第一批為基準，既有店家不冒充新店。
- 更新：固定店家／商品 ID 與搜尋引用，先讀遠端 checksum；只 upsert 新增／變動的 bundles、directory、索引桶及必要 metadata，刪除已不需要的 chunk。每日不走整庫上傳或 reset；單次差異最多 250 MiB，超出就停止。
- 不把每日 last_seen 和內部價格觀察窗口複製進每個網頁 bundle，避免無變化資料每天重寫；網頁顯示發布批次的觀察時間，完整每日事實以 HF Raw 為準。
- 第一個台灣日期為基準日；當天所有批次的店家／商品都不列為新店或新品。之後依首次出現的台灣日期判定新增；老店新品還必須晚於店家首次出現日期，避免同日補抓冒充新品。
- 所有差異在一個交易內套用，逐表核對後 commit；失敗 rollback，重跑相同版本為零寫入。HF Raw 與 Turso 各自標記版本，Raw 成功但 Turso 失敗可重試已存 Raw。
- GitHub secrets：`HF_TOKEN`、`FOODRADAR_TURSO_DATABASE_URL`、`FOODRADAR_TURSO_WRITE_TOKEN`、`FOODRADAR_TURSO_READONLY_TOKEN`。前端 config 只由 Actions 注入資料庫專屬 read-only token。
- 初次建立／重試：手動執行工作流程並勾選 `publish_existing`，從 HF 重建，不重新抓取。平台建庫工具 `scripts/provision_foodradar.py` 只建立 FoodRadar DB，不修改 UberEat。

以下命令供開發驗證；正式流程使用 Actions。

固定配送座標 `25.123246847935267, 121.52951826298867`，逐頁抓完 Uber Eats Feed 回傳的全部店家與菜單。沒有距離篩選，`max_pages: 0` 表示翻到沒有下一頁。店家會隨時間及配送可用性變動。

前端沿用 UberEat 儀表板、四個情報頁籤、全庫搜尋、分頁及價格走勢。菜單轉換、Schema 驗證、JSON → SQLite、Current/Events、packed DB、快照封存及 Parquet 欄位沿用原專案程式。查詢範圍和歷史來源限定這個座標，HF 路徑使用獨立 `FoodRadar/` 前綴。

```powershell
py -m pip install -r requirements.txt
py -m unittest discover -s tests -v
py src/crawl.py
# 設定 HF_TOKEN 後上傳完整批次
py src/crawl.py --upload
py -m http.server 8000 --directory web
```

批次使用原專案 14 碼 YYYYMMDDhhmmss。data/<batch>/menus 為原格式 Schema.org Restaurant JSON；archive/taiwan_menus_<batch>.tar.gz 包含 manifest.json 與 Json/<batch>_<SHA256>.json。封存前檢查店家集合、批次和 Schema；packed DB 驗證完整解壓、checksum 和筆數。

runner 的 ubereats.db 使用原 ETL 時序資料表；serving.db 使用原 Current/Events schema；packed-serving.db 使用原 MessagePack + Zstandard schema。site 是建置中間產物；Pages 發布前移除完整商品／歷史 JSON，只保留小型統計與空備援。正式網頁查詢 Turso，全庫不截斷。

HF 使用單一 commit 上傳壓縮快照、店家清單、完成 manifest、Parquet 和 latest 指標。抓取或驗證失敗不發布新資料。Actions 每日排程與手動執行，使用 repository secrets 與 GitHub Pages。

網站查詢此座標的 FoodRadar Turso DB，不連原全台 UberEat DB。第一批是歷史基準，後續批次才有跨期價差與新品比較。
