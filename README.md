# FoodRadar

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

ubereats.db 使用原 ETL 時序資料表；serving.db 使用原 Current/Events schema；packed-serving.db 使用原 MessagePack + Zstandard schema。site 保存原格式 Parquet、情報 JSON 和價格歷史。資料庫及完整菜單留在資料目錄，網頁載入前端用 JSON，全庫不截斷。

HF 使用單一 commit 上傳壓縮快照、完成 manifest、Parquet 和 latest 指標。抓取或驗證失敗不發布新資料。Actions 手動執行，使用 repository secret HF_TOKEN 與 GitHub Pages。

網站載入此座標的完整靜態資料，不連原全台 Turso 資料庫。第一批是歷史基準，後續批次才有跨期價差與新品比較。
