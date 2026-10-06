# FoodRadar LITE

參考 fafagoback/UberEat 的 Feed API、菜單 Schema.org 轉換與快照驗證，建立獨立輕量版。

固定配送查詢座標：`25.123246847935267, 121.52951826298867`。預設只保留店家實際位置距中心 **3 公里**內的資料。Uber Eats 回傳的是該配送位置可發現的店家，並非半徑內所有實體餐廳；不建立全台網格，不讀取舊全台資料庫。

## 使用

```powershell
python -m pip install -r requirements.txt
python src/crawl.py --check-config
python src/crawl.py
# 設定環境變數 HF_TOKEN 後，上傳完整批次
python src/crawl.py --upload
python -m http.server 8000 --directory web
```

`config.json` 可設定半徑（最大 10 公里）、並發（最大 5）、Feed 翻頁上限。達到翻頁上限而仍有下一頁、菜單失敗或空結果，流程失敗並保留先前已發布資料。座標缺失的店家先查菜單，仍無有效座標就排除。HF latest 只在完整批次上傳後更新。

- GitHub：`fafagoback/FoodRadar`
- HF dataset：`hub-google/UberEat`，獨立前綴 `FoodRadar/`
- 原始菜單、店家 JSON/CSV 與報告：`FoodRadar/snapshots/<batch>/`
- 最新完整快照：`FoodRadar/latest.json`
- 本機輸出：`data/<batch>/`（不進 Git）
- 輕量搜尋網站：`web/`，支援店名、餐點關鍵字與菜單價格。

Actions 只提供手動執行，單一工作節點。需要 repository secret `HF_TOKEN` 並啟用 GitHub Pages 的 GitHub Actions 來源。網站使用本批次靜態 JSON，不需要 Turso 或前端 token；不沿用原專案的資料庫與全台排程。HF 快照不自動清除，日後可另加保留期限。

驗證：`python -m unittest discover -s tests -v`。
