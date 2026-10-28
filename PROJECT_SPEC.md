# FoodRadar 專案規格與資料定義

> 本文件為 FoodRadar 現行實作的定義與架構說明（2026-10-28 核對，亦見根目錄 `PRD.md`）。本專案不使用根目錄 README.md。以下「新」代表 FoodRadar 的觀測定義，不代表現實世界的開幕或實際新品上市日期。

## 1. 專案目的與監控範圍

- 以**單一固定外送座標** (25.123246847935267, 121.52951826298867) 蒐集 Uber Eats 當時在該配送位置可見的店家及菜單。
- `config.json` 設定 `max_pages=0`：盡可能抓取 Feed 的全部分頁；不同時段可見店家可能不相同，**非全台店家全集**。
- 監控店家、菜色、價格異動、促銷、近期首次發現資訊，提供 GitHub Pages 網頁查詢。
- 以**台灣時間**記錄採集批次；預定每日 06:30 與 18:30 執行（GitHub Actions 排程可能延遲）。

## 2. 關鍵術語與公開介面定義

### 2.1 初始基準日（baseline）

- 第一個成功採集的台灣日期為基準日；這一天的所有批次均不列入「近期新店／新菜」。
- `baseline_processed_at` 紀錄基準起點；用日期比較排除同日補抓造成的假新品。
- 以下所有「最近七天」均以最新成功資料批次時間為錨點，不以使用者目前開網頁的即時時間為準。

### 2.2 新進店家（首頁及「新進店家速報」）

**現行正式定義：** 歷史資料中第一次觀測到的店家 UUID，且其 `stores.first_seen` 在最近 7 天、晚於基準日、當前 `status='active'` 且 `is_open=1`。

- 同 UUID 店家當天或日後再次被抓到，保留原 `first_seen`，不重算新店。
- 目前程式對店家有 `STORE_REAPPEARED` 事件，但**即使消失超過 60 天再出現，也不算新店**。這是現行約定；不要擅自把商品的 60 天規則套用到店家。
- `first_seen` 只在店家首次**有效營業中觀測**時建立；HF Raw 的非營業時段快照不視為首次發現。「第一次爬到」不等於新開幕，亦不保證與同名店／更換 UUID 的店家可跨 ID 去重。
- 新店的店家身份使用正規化 Uber Eats 店家 UUID。

### 2.3 老店新菜（首頁及「老店新菜推薦」）

**現行正式定義：** 屬於既有店家、現在有效的商品，且本輪上架時間在最近 7 天，並晚於基準日及該店首次出現的台灣日期；符合以下任一條件：
1. 這個 `(store_uuid, product_uuid)` 在 FoodRadar 歷史資料中首次被觀測，`first_seen` 與 `listing_started_at` 建立於該批次。
2. 歷史上出現過，但先被標記 `inactive`，再次出現時距前次 `last_seen` **至少 60 天**，更新 `listing_started_at`（`SEASONAL_RELIST`）。

**排除條件：**
- 新店首次觀測當日整份菜單不算「老店新菜」。
- 同商品短暫失蹤、未滿 60 天回來，不算新菜。
- 店家及商品需 `active`、`is_open=1`，商品有效價格大於 0。
- 商品識別鍵為 `(store_uuid, product_uuid)`；若平台變更商品 ID，即使名稱相同仍可能被視為新商品。

**60 天的技術細節：** 「下架」要先經過連續缺席門檻（預設 3 次符合條件的缺席觀測）才成為 `inactive`；60 天以歷史 `last_seen` 與本次重新觀測時間相減，不是以實際店家下架日計算。重新上架會改 `listing_started_at`，但永久 `first_seen` 不變。

### 2.4 七日低價、前次變價、促銷與全商品搜尋

**特價主清單唯一基準：** 同店同商品（`store_uuid`、`product_uuid`）的「最新成功批次有效實質單價」與該批次之前 **7×24 小時內最高有效實質單價**。時間範圍 `[T-7天, T)`，不含本次；超過七天的高價自動失效。計算只使用已驗證營業且可外送的價格觀測，不讀中位數、歷史最低價、前次價、原始菜單標價或無效時段舊報價。

- 降幅 = `max(0, (七日最高有效單價 - 最新有效單價) / 七日最高有效單價 × 100%)`；每件省下 = `max(0, 七日最高有效單價 - 最新有效單價)`。精算保留小數，介面以整數台幣顯示。
- 主清單預設 **降幅 ≥30% 且省下金額 ≥0 元**；即使近幾次價格都持平，只要七日內最高價仍較高，仍屬七日低價。沒有七日有效基準時不可認定降價，也不得以 0 筆暗示已檢查完整歷史。
- `products.seven_day_reference_price`、`seven_day_discount_pct`、`seven_day_discount_amount`、`seven_day_is_price_deal` 是首頁與特價主清單權威欄位。由 `PriceChange.seven_day_high` / `persisted_seven_day` 從有效歷史計算，網站只讀結果與過濾，不得自行重算。
- `reference_price`、`price_direction`、`is_price_drop`、`price_change_amount`、`price_change_percent`、`discount_pct` 屬**上次有效價 → 本次**的輔助變價資訊，**不得用來取代七日特價主清單**；例如第 4 次 100→50，第 5、6 次 50→50，後兩次的單次變價為持平，但七日特價仍是 50%。
- 實質單價依可支付標價與促銷件數計算；買一送一、第二件半價等換成每件成本。平台直接標示折扣的菜單價已為付款價，不重複打折。
- HF Raw 完整保存，包括非營業時段無法下單的菜單報價；僅當時經驗證為有效的歷史觀測可進入正式七日比較。無法驗證但有多次支持的歷史菜單價，只能放在**另列的參考報價**，不能偽裝成可下單的降價。
- 同時要求 `store/product active`、`is_open=1`、`last_seen==latest_processed_at`；舊批次曾有促銷，不代表目前有效。
- 平台促銷（促銷標籤或優惠組合）與實際七日降價是不同概念；「全庫商品搜尋」只列目前有效商品。歷史覆蓋不足需顯示有效採集日、具可比對價格的商品數。

### 2.5 可重現的七日特價範例

| 七日前價格序列（有效觀測） | 最新實質單價 | 前七天最高價 | 主清單 |
| --- | ---: | ---: | --- |
| 100、50、50、50 | 50 | 100 | 降幅 50%；列入 ≥30% |
| 100、100、100 | 80 | 100 | 降幅 20%；不列入 ≥30% |
| 50、50、50 | 50 | 50 | 降幅 0%；不列入 |
| 100（超過七天）、50 | 50 | 50 | 降幅 0%；過期高價排除 |
| 無有效歷史或僅非營業菜單 | 50 | 無 | 不列入，顯示基準不足 |

## 3. 資料生命週期與防重複

1. `src/crawl.py` 掃描此配送座標的店家，將同次 feed 依正規化店家 UUID 去重，抓取逐店菜單；任何店家抓取失敗即拒絕發布不完整快照。HF Raw 完整封存所有成功抓取的店家與菜單（含非營業時段）。
2. `src/pipeline.py:restore_history` 優先從 Hugging Face 永久 normalized state 還原先前 `serving.db`（驗證 SHA-256、SQLite integrity 與批次一致性），再補入新於 checkpoint 的已完成 Raw 快照。
3. `src/price_observation.py` 統一校驗採集當下是否營業：必須 `isOpen=True`、不能有關店訊息、營業時段不能矛盾。缺少營業時間表時須有明確營業中文字訊號。未通過者只留 HF Raw，**不得新增／更新正式 stores、products、events、first_seen 或歷史價格**；僅用暫存 seen marker 防止假下架。
4. `src/serving_state.py` 只對驗證通過的資料，以店家 UUID、商品複合 UUID 查詢含 inactive 的歷史資料。存在則更新狀態但不重設 `first_seen`；不存在才插入並產生 `STORE_NEW` 或 `NEW` 事件。
4. 店家缺席會累積 `missing_streak`，達預設門檻成 `inactive`；回歸產生 `STORE_REAPPEARED`，不重設首次日期。
5. 商品在可見且已驗證營業的店家菜單內缺席累積 `missing_streak`，達門檻成 `inactive`；滿 60 天再觀測時標記季節性回歸並更新本輪上架日期。
6. `scripts/prototype_pack_db.py` 從歷史 state 建立查詢用 Packed DB：新店保留 `first_seen`；新商品以 `listing_started_at` 的七日視窗、基準日和店家首次日期產生 `f:new` 索引。
7. 靜態 JSON 及衍生 Parquet 採用同一營業判斷，防止另一條 ETL 路徑重新寫入髒資料。
8. Turso 使用只寫入必要差異的發布程序，驗證後才提交；原始快照與僅含有效營業觀測的永久 state 保存在 HF。既有髒資料如需重播 HF Raw 重建，**必須先確認歷史 Raw 完整**；若候選店家或商品較既有持久資料減少超過 30%（基準筆數至少 100），發布器會拒絕覆蓋，以免因 Raw 不完整大規模刪除原本有效的資料。

## 4. 正式系統架構

```text
GitHub Actions (.github/workflows/crawl.yml)
  ├─ Python 單座標 Uber Eats feed + 所有可見店菜單
  ├─ HF 歷史 state 還原與 Raw 驗證
  ├─ normalized serving.db (stores / products / events / crawl_batches)
  ├─ 產生 Packed DB（目錄、壓縮店家 bundles、商品搜尋 buckets）
  ├─ HF dataset: hub-google/UberEat/FoodRadar/
  │   ├─ snapshots/<batch>/ 原始菜單封存與 manifest
  │   ├─ state/<batch>/serving-state.db.gz
  │   ├─ state/latest.json
  │   └─ Parquet 分析衍生資料
  ├─ 差異更新獨立 FoodRadar Turso 資料庫
  └─ 發布 GitHub Pages (web/)
           └─ 使用唯讀 token 查詢 Turso Packed DB
```

**持久化來源：** HF 完整 Raw + 永久 normalized state；**網站查詢來源：** FoodRadar 專用 Turso；**執行環境：** GitHub Actions runner 臨時 SQLite/JSON 檔案，並非正式持久資料來源。網頁不會把原始完整 JSON、Parquet 當作公開靜態資料集。

主要入口：
- `config.json`：配送座標、工作者、最大頁數、HF 目標。
- `src/crawl.py`：單座標採集與上傳入口。
- `src/menu_core.py`：店家菜單與店家 UUID 正規化。
- `src/serving_state.py`：歷史 Current/Events、首次出現、失蹤／回歸判斷。
- `src/pipeline.py`：歷史還原、構建與 HF 發布。
- `scripts/prototype_pack_db.py`：建置 Turso 資料投影與七日新菜索引。
- `scripts/publish_turso_delta.py`：安全差異更新。
- `web/packed-turso.js`：網站資料查詢、新店／新菜條件。
- `web/app.js`：頁籤、篩選與首頁統計；首頁新店／新菜筆數取自查詢結果長度。
- `.github/workflows/crawl.yml`：測試、採集、發布工作流程。

## 5. 網頁計算與顯示核對（2026-10-19）

| 網頁項目 | 直接來源 | 核對結論 |
| --- | --- | --- |
| 新進店家列表 | `web/app.js:fetchNewStores` → `searchStores(newOnly=true)` → `store_directory` | 依 `first_seen` 七日視窗、基準日期以及 active/open 條件，符合 §2.2 |
| 老店新菜列表 | `fetchNewProducts` → `searchPacked(newOnly=true)` → `f:new` posting | 依 `listing_started_at`、七天、晚於店家首見及基準日；可含滿 60 天重新上架，符合 §2.3 |
| 首頁新店數 | `updateStatsUI` 使用 `APP_STATE.filteredStores.length` | 預設近期新店範圍正確；列表在「全部店家」模式或其他篩選時，數量可能變成篩選後筆數 |
| 首頁新菜數 | `updateStatsUI` 使用 `APP_STATE.filteredProducts.length` | 初次載入時與近期新菜列表同源；使用者互動後標頭未必隨後續搜尋同步更新 |

**核對範圍：** 2026-10-27 最新成功 Actions 對 HF 七日 RAW 進行 17 批稽核：最新批有效商品 4,654 項，964 項有前七天有效價格可比較，正式降幅 ≥30% 為 0；另有 2 項舊菜單報價相較現價降幅 ≥30% 的候選，但舊報價不等於已驗證可下單價格。最新網頁所稱監控總量不等於本批有效可比價格商品數。稽核批次隨新採集更新。

## 6. 發布、驗證與風險

- 流程：`python -m unittest discover -s tests -v` → 完整採集 → HF commit → Packed DB 與 Turso 差異驗證 → GitHub Pages。
- 工作流程排程採 UTC cron `30 10,22 * * *`，對應台灣 18:30、06:30；GitHub 可延後啟動。
- GitHub secrets：`HF_TOKEN`、`FOODRADAR_TURSO_DATABASE_URL`、`FOODRADAR_TURSO_WRITE_TOKEN`、`FOODRADAR_TURSO_READONLY_TOKEN`。請勿把寫入 token 放入網頁。
- `publish_existing` 可由現有 HF Raw 重建／重試發布而不重新爬取。
- 注意：平台 UUID 變化、採集盲區、營業時間及漏抓可能影響「首次被觀測」之意義；不應將 `first_seen` 解釋成現實世界的開幕日期。
