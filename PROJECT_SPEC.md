# FoodRadar 專案規格與資料定義

> 本文件為 FoodRadar 現行實作的定義與架構說明（2026-10-28 核對，亦見根目錄 `PRD.md`）。本專案不使用根目錄 README.md。以下「新」代表 FoodRadar 的觀測定義，不代表現實世界的開幕或實際新品上市日期。

## 2026-10-30 原始資料永久規格（優先於下方舊版流程說明）

**唯一的新採集原始資料來源（獨立 Dataset Repository）：** `hub-google/FoodRadar`，其內 `Raw/<YYYYMMDDHHMMSS>/capture.tar.gz`。舊 `hub-google/UberEat` 之任何檔案均不再作為 DB 來源。

- 爬蟲直接以二進位保存 `getFeedV1` 每頁、每次 HTTP 回應，以及 `getStoreV1` 每間店、每次 HTTP 回應的 `response.content`，不重新序列化 JSON、不轉換幣值、不改欄位、不刪促銷內容、不做商品正規化。Tar/Gzip 只做無損封存，解壓後原始回應的 SHA-256 必須一致。
- `manifest.json` 是獨立的採集索引與 SHA-256 清單，不插入原始回應。失敗與停業回應也必須保留。歷史 Raw 快照不可覆寫；失敗批次可以封存但不可發布到 DB。
- 只有在原始回應完整上傳 HF、重新下載並逐檔核對成功後，`scripts/prepare_hf_release.py` / `src/pipeline.py:restore_history` 才能**從 HF 讀回 Raw**，進行店家識別、商品解析、單位換算、促銷、營業狀態、七日比較及 Turso 發布。
- `hub-google/UberEat` 的任何舊檔、checkpoint、Parquet 都不會讀取，也不會寫入新 Dataset。**新的獨立 Repository `hub-google/FoodRadar` 僅使用 `Raw/` 存放原始回應，DB 只由此讀取**。
- 爬蟲操作原始回應只為判斷請求成功、下一頁與要抓哪些店，這些操作不得修改或取代 HF 保存的原始回應。DB 轉換的失敗不允許回寫或刪除原始快照。

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
- `first_seen` 只在店家首次**有效營業時段菜單觀測**時建立；即使目前無法外送，仍可首次發現菜單。HF Raw 的非營業時段快照不視為首次發現。「第一次爬到」不等於新開幕，亦不保證與同名店／更換 UUID 的店家可跨 ID 去重。
- 新店的店家身份使用正規化 Uber Eats 店家 UUID。

### 2.3 老店新菜（首頁及「老店新菜推薦」）

**現行正式定義：** 屬於既有店家、現在有效的商品，且本輪上架時間在最近 7 天，並晚於基準日及該店首次出現的台灣日期；符合以下任一條件：
1. 這個 `(store_uuid, product_uuid)` 在 FoodRadar 歷史資料中首次被觀測，`first_seen` 與 `listing_started_at` 建立於該批次。
2. 歷史上出現過，但先被標記 `inactive`，再次出現時距前次 `last_seen` **至少 60 天**，更新 `listing_started_at`（`SEASONAL_RELIST`）。

**排除條件：**
- 新店首次觀測當日整份菜單不算「老店新菜」。
- 同商品短暫失蹤、未滿 60 天回來，不算新菜。
- 店家及商品需 `active`，商品需有最近取得的菜單報價；下單／配送狀態不應讓既有菜單消失。
- 商品識別鍵為 `(store_uuid, product_uuid)`；若平台變更商品 ID，即使名稱相同仍可能被視為新商品。

**60 天的技術細節：** 「下架」要先經過連續缺席門檻（預設 3 次符合條件的缺席觀測）才成為 `inactive`；60 天以歷史 `last_seen` 與本次重新觀測時間相減，不是以實際店家下架日計算。重新上架會改 `listing_started_at`，但永久 `first_seen` 不變。

### 2.4 七日低價、前次變價、促銷與全商品搜尋

**特價主清單唯一基準：** 同店同商品（`store_uuid`、`product_uuid`）的「最新成功批次有效實質單價」與該批次之前 **7×24 小時內最高有效實質單價**。時間範圍 `[T-7天, T)`，不含本次；超過七天的高價自動失效。計算只使用已驗證實際營業時段的有效菜單單價（可暫不可外送，但不得宣稱當時已確認可下單），不讀中位數、歷史最低價、前次價、原始菜單標價或無效時段舊報價。

- 降幅 = `max(0, (七日最高有效單價 - 最新有效單價) / 七日最高有效單價 × 100%)`；每件省下 = `max(0, 七日最高有效單價 - 最新有效單價)`。精算保留小數，介面以整數台幣顯示。
- 主清單預設 **降幅 ≥30% 且省下金額 ≥0 元**；即使近幾次價格都持平，只要七日內最高價仍較高，仍屬七日低價。沒有七日有效基準時不可認定降價，也不得以 0 筆暗示已檢查完整歷史。
- `products.seven_day_reference_price`、`seven_day_discount_pct`、`seven_day_discount_amount`、`seven_day_is_price_deal` 是首頁與特價主清單權威欄位。由 `PriceChange.seven_day_high` / `persisted_seven_day` 從有效歷史計算，網站只讀結果與過濾，不得自行重算。
- `reference_price`、`price_direction`、`is_price_drop`、`price_change_amount`、`price_change_percent`、`discount_pct` 屬**上次有效價 → 本次**的輔助變價資訊，**不得用來取代七日特價主清單**；例如第 4 次 100→50，第 5、6 次 50→50，後兩次的單次變價為持平，但七日特價仍是 50%。
- 實質單價依可支付標價與促銷件數計算；買一送一、第二件半價等換成每件成本。平台直接標示折扣的菜單價已為付款價，不重複打折。
- HF Raw 完整保存，包括非營業時段無法下單的菜單報價；僅當時實際營業時段取得的有效菜單報價可進入正式七日比較。非營業時段菜單僅供商品目錄參考；配送不可用的營業時段報價能供菜單價比較，但不能偽裝成可下單優惠。
- **七日菜單降價**要求 `store/product active`、當批經實際營業時段驗證且 `last_seen==latest_processed_at`；另用 `delivery_available` 區分當批是否確認可外送，舊批促銷不可宣稱現在可用。
- 平台促銷（促銷標籤或優惠組合）與實際七日降價是不同概念；「全庫商品搜尋」列出 `active` 的**最後已知菜單商品**，即使店家目前打烊或暫不能外送，也不得因此刪除搜尋索引；介面要標註「最近菜單價・非即時」或「菜單參考價・暫不可外送」。歷史覆蓋不足需顯示有效採集日、具可比對價格的商品數。

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
2. `src/pipeline.py:restore_history` 每次只重播 `hub-google/FoodRadar` Repository 的 `Raw/` 原始批次。**絕不從舊 `hub-google/UberEat` 或 normalized state/checkpoint 還原**。
3. `src/price_observation.py` **分開**驗證：`menu_catalog_valid()` 允許營業或非營業時段的有價菜單進入一般商品資料庫；`menu_price_observation_valid()` 僅將營業時段報價納入正式比價，與是否可外送分離；`price_observation_valid()` 僅代表採集當批確認可外送。非營業菜單可更新最近目錄報價，但不新增七日比價樣本。
4. `src/serving_state.py` 對有效菜單報價（無論是否可外送），以店家 UUID、商品複合 UUID 查詢含 inactive 的歷史資料。存在則更新狀態但不重設 `first_seen`；不存在才插入並產生 `STORE_NEW` 或 `NEW` 事件。
4. 店家缺席會累積 `missing_streak`，達預設門檻成 `inactive`；回歸產生 `STORE_REAPPEARED`，不重設首次日期。
5. 商品在可見且已驗證營業的店家菜單內缺席累積 `missing_streak`，達門檻成 `inactive`；滿 60 天再觀測時標記季節性回歸並更新本輪上架日期。
6. `scripts/prototype_pack_db.py` 從歷史 state 建立查詢用 Packed DB：新店保留 `first_seen`；新商品以 `listing_started_at` 的七日視窗、基準日和店家首次日期產生 `f:new` 索引。
7. 靜態 JSON 及衍生 Parquet 採用同一營業判斷，防止另一條 ETL 路徑重新寫入髒資料。
8. Turso 使用只寫入必要差異的發布程序，驗證後才提交；原始快照、最後已知菜單報價及獨立的可外送價格驗證狀態保存在 HF。來源切換或清庫時，使用已驗證的獨立 Raw 封存及其校驗證明進行交易式完整重建，禁止再灌入舊髒資料。

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

**2026-10-28 配送／菜單分離修正：** 商品目錄的存在與搜尋不以「當前可下單」為必要條件。`menu_quote_*` 保存最近菜單標價與優惠單價，`delivery_available` 是單獨資訊；正式七日菜單降價以營業時段的有效實質單價判斷；配送驗證為獨立標示。完整 HF RAW 重播可將舊時因 `Delivery unavailable` 而被隱藏的菜單恢復。

### 2026-10-28 正式 RAW 菜單比價修復

價格比較與配送狀態分離。每批取得的有價菜單報價，不因 `Delivery unavailable` 或當次打烊從**商品目錄**消失；`products.menu_quote_history` 只保存七日內同店同商品在**實際營業時段**的每批實質單價，`repair_effective_prices.py` 使用「最新菜單價 vs 前七天最高菜單實質單價」填入正式 `seven_day_*` 欄位，`prototype_pack_db.py` 建立正式降價索引。配送是否可下單只影響獨立標示，不能冒充已確認優惠。網站走勢圖直接讀取保存的菜單單價，並由 `audit_deal_pipeline.py` 獨立核對已存計算結果。需從所有已驗證完整性的 HF RAW 重播，不能只換搜尋條件。


### 2026-10-28 HF RAW 清庫重建指令（不保留舊 Turso 資料）
- 同一份 HF RAW 保持不變，重新解釋同店同 SKU 的原始售價與促銷分類。
- **菜單搜尋**保留早上未營業時的商品資訊與最近取得的標價；**七日價格比較**僅採用實際營業時段的菜單實質單價。即使 `isOpen=true`，若營業時段表或 `Opens 9:30 AM` 顯示尚未營業，該筆不得新增比價歷史。
- `Delivery unavailable` 不代表未營業，不會單獨排除營業時段的歷史菜單報價。
- 同 SKU 同時屬於一般分類及買一送一分類時，必須優先採用有明確促銷證據的版本；不可任意取 API 的第一個分類。
- 回歸案例：魚池貳壹台北士林店「楊桃甘露」RAW 標價全為 $55；營業時段出現買一送一，實質單價 $27.50；尚未開門的 $55 報價不可成為七日最高實質單價的基準，否則產生假 50% 降幅。
- 對 Turso 執行完整的 `--reset-all`：必須先完成 HF 所有完整快照 SHA256/店家數驗證、暫存資料庫重建、單價稽核，才在單一交易內清除 FoodRadar 全部五個正式資料表，再插入新完整資料。完成後逐表核對內容；驗證失敗時回滾。不備份舊 Turso。


### 2026-10-28 HF RAW 每筆歷史標價與促銷完整保存
- 價格走勢不再僅保存每件實質單價。每筆 `menu_quote_history` 用 `[本地採集時間, 實質單價, RAW 標價, 促銷類型, 優惠取得數量]` 保存歷史來源；同一店、同一 SKU、同一批次只保存一筆，優先採用有明確優惠分類的資訊。
- 延續七日價格比較規則：仍以第二欄的實質單價計算，不改降幅定義。舊版只有兩欄的歷史仍可讀取但不偽造促銷分類或標價。
- 前端必須依原始五欄顯示歷史標價、優惠種類和每件實質單價；若歷史資料無法查到標價／優惠，就標明尚未保存，不能假裝是無促銷。
- 證據案例：世界豆漿大王民族店冰豆漿 RAW 10/21～10/24 標價 58、促銷分類無，10/25 起同店同 SKU 同時出現在一般區與 `Buy 1, get 1 free`，標價仍 58，每件實質單價 29。需完整重播全部 HF RAW 更新 Turso，不能只改網頁顯示。


### 2026-10-28 全專案程式盤點修正（重建核對）
- 有價菜單目錄與真正營業狀態分離：`snapshot_seen_stores` 只記菜單可用於商品目錄；`stores/products.is_open` 表示採集時按營業時段確認營業。不能用前者冒充可比較報價。
- 同 SKU 在一般與促銷分類有不同 `offers.price` 時，促銷優惠必須使用其來源分類的原始售價與件數；不得把一般分類原價與促銷份數混合。
- 同批次 HF RAW 重建可產生完全不同的價格資料；`data_revision` 由完整來源表雜湊而非批次時間構成，網頁快取及歷史走勢入口均需在載入時檢查新版資料，避免舊 Bundle 污染。
- 明細中的可外送狀態顯示為「最近採集時」，不是即時承諾；首頁不重複顯示兩個相同的七日比較數字，累積產品數明示包含已下架商品。
- GitHub Actions 主要採集每日 06:30／18:30（台灣）；07:30／19:30 使用同 workflow 只在 Turso 查不到本時段成功採集時補抓，避免固定重複採集。GitHub 排程啟動可能延遲，不可宣稱精確保證。
- HF RAW 稽核的「價格可比較率」只計算實際營業時段合格的 RAW 單價；`Delivery unavailable` 不自動排除，非營業快照不納入。
- 上述處理會改變歷史營業與同 SKU 價格基準。**正式發布需重新驗證、重播所有已驗證 HF RAW，重建 Turso 查詢索引與價格欄位**，不能沿用舊 checkpoint；任何差異稽核失敗必須擋下發布。

### 2026-10-28 正式採集端到端重驗
- 完成 52 批 HF RAW 修復重播且 Turso 全表比對通過後，執行一次新採集，以驗證本日網路採集、HF 封存、有效價格計算、Turso 更新及網站發布流程。不得因展示目的而偽造降價商品。
