# FoodRadar 產品需求文件（PRD）
**版本：** 2026-11-09｜**狀態：** 完整分類 API 與精簡網站欄位整合；正式發布需逐批驗收  
**專案：** `fafagoback/FoodRadar`｜**唯一當前 Raw Dataset：** `hub-google/FoodRadar/Raw/`

## 2026-11-09 每日三軌查核及全店驗收契約（最新補充）

- **不得以單一品牌作為驗收範圍**：哈根達斯只是漏抓案例。每天對同一固定配送點下，最新成功 HF Raw 批次包含的**每一間店、每一款原生 UUID 商品、每一個分頁及原生價格**進行對帳；不得宣稱知道 Uber Eats 沒有向 API 公開的隱藏商品。
- **採集主流程（A）**：既有 GitHub Actions `getFeedV1 → getStoreV1 → getCatalogPresentationV2`，按 `hasMore/offset` 遍歷完整回應、SHA-256 無損封存 HF Raw。每店商品按原生 UUID 去重，不可拿含重複分類的出現次數冒充不同商品數。
- **第二種真正獨立的網頁畫面查核（B，2026-11-09 已在 GitHub Actions 實跑）**：使用 Playwright **WebKit（Safari 核心）**實際開啟 Uber Eats 商店，讀取瀏覽器畫面上 `data-testid="store-item-<UUID>"` 商品卡、商品圖片 alt 商品名稱、可見標價；點擊每個 `role=tab` 分類，持續捲動收集出現的商品。**禁止讀取 `__REACT_QUERY_STATE__`、禁止以另一份 `getStoreV1` API JSON 充當網頁資料，也不能使用 HF/Turso 當 B 輸入。** Chromium 先前受 403 阻擋，實測 WebKit 及 Firefox 均可在 GitHub 開啟好市多、全聯、優市三家店 HTTP 200 且讀到真實商品卡。真正同一次 A/B 逐 UUID 比對（只走前兩個分類）：好市多 API 123／UI 167／UI 獨有 66；全聯 API 515／UI 255／UI 獨有 60；優市 API 1032／UI 374／UI 獨有 57。共 **183 款 UI 獨有 SKU**；另有 1 款價格原生 cents 差 1（須確認浮點顯示四捨五入）。第二種確實找到第一種未收錄的商品，但只看兩個分類尚非全店掃描，不能宣稱 100% 涵蓋；每日正式 B 預設試圖遍歷所有分類並維持不完整標記，不得將比對自動修正為 A 的值。GitHub 實測紀錄：`42589395192`。
- **全資料層稽核（額外、不可冒充第二來源）**：`scripts/audit_full_catalog.py` 從 HF `Raw/latest.json` 下載原始封存，檢驗封存 SHA-256、所有 HTTP body、店家與頁數、每一個 SKU 有無從原始回應遺失於 ETL、價格換算一致性，再核對正式 Turso `latest_batch`。這是資料**傳遞完整性**，不是獨立驗證 Uber 當天的全部品項。
- **排程**：台灣時間每日 04:00 先執行 `.github/workflows/daily-dual-audit.yml` 產出 API 與網頁 DOM 兩軌差異，以及全批次資料層報告；06:15 使用既有「排隊怪獸」LINE OA `hub-google/LINE/.github/workflows/foodradar-daily-line-report.yml` 推送，再於 06:30 進入正式爬蟲（GitHub Actions 可能延遲，通知須明示報告日期）。LINE 僅能推送給明確設定的 `REPORT_LINE_USER_ID` **或**透過「排隊怪獸」平台 OA 傳送「訂閱FoodRadar」明確同意訂閱的用戶（以簽章驗證的 LINE webhook 存在 RLS 私有資料表，HMAC 認證取回訂閱收件人）；使用者傳送「取消FoodRadar訂閱」停止。當沒有收件人時必須失敗並提示，**不得廣播所有 OA 顧客**。
- **LINE OA 安全與驗收**：訂閱資料僅包含 LINE User ID、訂閱開關和時間，`public.foodradar_line_subscriptions` 已啟用 RLS 並撤銷 anon/authenticated 的資料表權限。每日工作流程只使用 GitHub Secrets 內既有 OA Token，對 LINE Bridge 的訂閱名單讀取必須經過當日 HMAC 驗證；未訂閱前訊息不可冒充已送達，收件人 ID 不得寫入 GitHub 公開報告或日誌。2026-11-09 發送實測：簽章訂閱名單讀取成功，但尚無已訂閱者，因此訊息沒有送出；等待使用者傳送一次「訂閱FoodRadar」完成收件註冊。
- **日報內容**：每間樣本店 A/B/C 實際商品數、HTTP 錯誤與差異；全批次店家數、原生唯一 SKU 數、ETL 成功 SKU 數、漏轉筆數、價格不符數、是否與 Turso 同一批次。上次成功正式批次與本次失敗稽核須區分，不能用前一版報告宣稱當日成功。
- **驗收範圍與限制**：2026-11-09 `20261109133009` 正式 Raw 共有 577 間店、817 份原始 body；全店最新批次不同原生 SKU 33,278，轉換後相同 33,278，漏轉 0、價格不符 0，Turso 同批。這代表已取得資料的完整傳遞，非全平台或截圖方式 100% 獨立核實。
- **第二種方法的 GitHub 程式**：`scripts/live_webkit_dom.py` 使用 GitHub Actions 上的 Playwright WebKit 瀏覽器，執行商品卡讀取、實際分類切換及捲動；`scripts/daily_triple_audit.py` 把 B 的可見商品 UUID／價格與 A 正式 API SKU 做差異比較，分別列出 A 獨有、B 獨有與價格不一致；`scripts/live_ui_api_comparison.py` 是獨立同時段實測。舊版 `storefront_html_reader.py` 只讀內嵌 API 快取，**不得作為第二來源驗證或每日日報依據**。
- **報告保存**：完整結果每日寫入 `audit-results/YYYY-MM-DD/report.json`、`audit-results/latest.json`、`audit-results/full-catalog-latest.json`；原始 API 回應及 DOM 比對輸出留 Actions artifacts，不再產生截圖或 OCR 結果。報告提交須支援同時間其他程式更新造成的 non-fast-forward 重試；即使第二種被擋也必須保留異常報告並讓查核工作為失敗狀態。

## 2026-11-09 最新契約（優先於下方歷史條款）

1. **業務邏輯不改**：維持平台促銷、同品買 X 送 Y 實質單價、七日內最高有效實質單價比較、實質降幅 ≥30%、新品與新店的既有定義；平台刪除線價絕不當成七日歷史最高價。
2. **僅保留三支 GitHub Actions 已實測 API**：`getFeedV1` 發現指定配送座標店家；`getStoreV1` 取得店家狀態及 `aisles.catalogSectionUUID`；`getCatalogPresentationV2` 一次查詢多分類並沿 `pagingInfo.offset` 迭代至 `hasMore=false`，不得只抓第一頁。跨分類同 SKU 按 `store_uuid+product_uuid` 去重。
3. **HF 僅原始回應**：每份 HTTP body 以原始位元組加到無損壓縮檔，不修改、不正規化、不覆寫；保存失敗回應及所有重試，外層重試與內層分頁的路徑必須唯一。只有完整所有店家及分頁、上傳並重新下載 SHA-256 驗證均通過的批次才能更新 `Raw/latest.json` 並進入 DB。
4. **Turso 只存網頁需要的衍生欄位**：`products.catalog_web`（精簡 JSON）與 `web_products.card` 包含商品圖片網址、平台刪除線價、原生活動 ID/類型、另有滿額活動文字、商品售罄/可售狀態、所有分類 UUID。**不得儲存另一份完整 API JSON 到 Turso**；原始證據只在 HF Raw。
5. **三套價格嚴格分離**：`price` 是平台現在標價；`display_original_price` 是平台刪除線參考價；`seven_day_reference_price` 是 FoodRadar 真正觀測到的過去七日最高有效實質單價。買送與單品折價由 `verified_prices.py` 根據原生數值計算。滿額折抵只有文案與活動 ID，**不攤提到單品價格**。
6. **可售狀態獨立**：`is_sold_out=true` 或 `is_available=false` 的商品仍保留商品索引，但不能被宣稱為當前可購買優惠。打烊/無配送不代表下架；不完整爬蟲不得增加 missing streak。
7. **分類與去重**：多分類 UUID 存於單 SKU 的 `section_uuids`，不是多份商品；`titleBadge`、`quickAddConfig`、`displayType`、`spanCount` 等純前台排版欄位僅留 HF Raw。
8. **完整驗收**：GitHub Actions Python+JS 測試、HF Raw SHA-256 驗證、DB 重播、Turso 索引核對及瀏覽器查詢必須分別通過；不能把「程式已提交」或「單店模擬成功」報成正式資料更新完成。

### 新增必要欄位對照

| 原生證據 | DB 投影／用途 |
| --- | --- |
| `uuid`, `title`, `price` | 商品主鍵、名稱、目前菜單價（沿用既有欄位） |
| `imageUrl` | `catalog_web.image_url`／網站商品縮圖 |
| `itemThumbnailElements` 有唯一現價及刪除線舊價 | `catalog_web.display_original_price_cents`，僅作平台標示原價 |
| `catalogItemAnalyticsData.promoType` | `catalog_web.native_promo_type`，與實質折扣分開 |
| `promoInfo.promotionUUID` | `catalog_web.promotion_uuid` |
| `itemLevelPromotion.multiSKUFlatPromotion` | `catalog_web.cart_campaign_uuid`、`cart_promo_label`，不折算單品 |
| `isAvailable`, `isSoldOut` | `catalog_web.is_available`、`is_sold_out` |
| `sectionUuid`, `subsectionUuid`, `catalogSectionUUID` | 同 SKU 去重後多分類 UUID |
| `pagingInfo.offset`, `hasMore` | HF manifest 完整性、取得後續分頁，不放商品主檔 |
| `purchaseInfo`、`quickAddConfig`、`productInfo` 等 | 保留 HF Raw；除網頁確實需要外不進 DB |

--- 

### 2026-11-09 原生分類 API 空頁兼容（已實測）

- GitHub 真實 HF Raw 批次 `20261109105106` 發現 4/523 間店的 `getCatalogPresentationV2` 回應為 **HTTP 200、`data.catalog=null`**，同時具有合法 `data.pagingInfo.hasMore`。其中三間是 `hasMore=false` 的末頁，一間是 `hasMore=true` 且 `offset` 持續前進的中途空頁；**這不是 HTTP 失敗或找不到店家**。
- 只有在回應明確具有 `catalog` 欄位且 `pagingInfo.hasMore` 為布林值時，才將 `catalog=null` 視為「本頁 0 筆」，仍要原封不動存 HF Raw；`hasMore=true` 時下一個 `offset` 必須嚴格增加，`hasMore=false` 才算結束。缺欄位、錯誤型別、offset 停滯、分頁超出上限仍禁止完整發布。
- 實際重測前述四間店皆成功：分類頁數 4/4/2/15、商品出現紀錄 62/80/41/198；同時保留 121 項 Python 回歸測試，包含空中途頁、空末頁與惡意循環分頁。
- **HF 完整性仍是發布必要條件**：若其他店的 API 真正失敗，整批仍保留 `complete=false` 原始資料，不產生新 DB 最新批次；重播历史時略過失敗 Raw，但不得刪除原始證據。

## 1. 產品目標與範圍
監測 Uber Eats 在固定配送點 `25.123246847935267, 121.52951826298867` 可見店家、商品、實質單價、平台促銷、首次發現及價格歷史。這不是全台即時所有店家；以完整成功爬取的一個單座標批次為當前有效資料。

## 2. 採集、保存與驗證
- 排程每日台北時間 06:30、18:30（GitHub Actions 可能延遲）；人工可重跑。每批完整且驗證後才更新最新指標。
- **目前唯一 HF 保存格式**：`hub-google/FoodRadar/Raw/<batch>/capture.tar.gz` 內無損保存 `getFeedV1`、`getStoreV1`、`getCatalogPresentationV2` 的每份原始 HTTP `response.content`（含失敗重試）；`manifest.json` 和 `Raw/latest.json` 只存批次索引、原始 body SHA-256、檔名與完整性狀態。**HF 不上傳轉換後菜單、商品表、價格表、Parquet 或 Turso DB。** 舊版 `hub-google/UberEat` 的 `Json/`、`ApiRaw/`、state 描述只代表已停用歷史，不能當作現行資料來源。
- **菜單存在與配送可用分離。** `menu_catalog_valid()` 接受有標價菜單（即使打烊或不能外送），保留商品供搜尋；`menu_price_observation_valid()` 另外依實際營業時段決定報價能否成為七日比較樣本，不因單純 `Delivery unavailable` 排除；`price_observation_valid()` 更嚴格，只判斷採集時是否確認可外送。三種狀態不可混用。
- 同批同 SKU 跨菜單分類先合併優惠證據，避免把重複分類當不同商品或錯失買一送一。
- ID：店家 UUID；商品鍵 `store_uuid + product_uuid`。資料庫保留最後已知的菜單報價（即使目前打烊／暫不可外送），並獨立保存經驗證可外送的價格事件；不以舊菜單報價推測可支付價格。

## 3. 唯一正式七日特價規則（最高優先）
1. 以**最新成功批次的營業時段有效菜單觀測 T** 決定可比較商品（店家與商品均 active、實際營業中，且兩者最近觀測等於最新批次）。所有最後已知菜單商品仍可搜尋，但不一定能參與七日比較。
2. 找同店同商品、**早於 T 且距 T 不超過 7×24 小時**的歷史有效**實質單件成本**；範圍 `[T-7天,T)`。以此集合的 **MAX** 為參考價（不是中位數、最低價、平均、上一筆價）。
3. 降幅 `max(0, (七日最高價 - 最新實質價) / 七日最高價 × 100%)`；每件省下 `max(0, 七日最高價 - 最新實質價)`。計算保留小數，UI 金額顯示整數台幣。
4. `七日最高價>最新實質價` 即屬七日降價；主清單預設門檻 **降幅 ≥30%** 且 **省下 ≥$0**，支援其他門檻篩選。不看商家是否有標註促銷。
5. **連續低價仍有效**：6 天前 $100，之後每天 $50，今天 $50 → 比較 $100，仍顯示下降 50%。高價自然超過七天後即移除；7 天內完全沒有有效舊價則顯示無法比較，不能假裝已驗證零降價。
6. 首頁卡片、特價分頁、查詢索引、靜態備援與後端稽核都必須使用相同規則；舊單次變價 `reference_price/discount_pct/is_price_drop` **不可替代** `seven_day_*` 主清單欄位。

**技術歸屬：** `src/price_change.py:PriceChange.seven_day_high` 決定基準；`PriceChange.compare` / `qualifies` 做金額百分比運算；`persisted_seven_day` 建立 `seven_day_reference_price`、`seven_day_discount_pct`、`seven_day_discount_amount`、`seven_day_is_price_deal`；`scripts/repair_effective_prices.py` 依已驗證事件重算並持久化；`web/packed-turso.js` 僅讀資料庫欄位，不自創規則。

## 4. 實質單價
- 單件：可支付菜單售價；無促銷多入：總付款價÷件數。
- 買 B 送 F：總需付 B 件價錢÷(B+F)；買一送一按每件半價；第二件半價按兩件總價÷2；加一元多一件按兩件總付款÷2。
- 已折抵的直接折扣菜單價不再乘折扣率一次，避免重複折扣。
- 歷史參考價與目前價都必須是相同口徑的有效實質單價，不能「以前標價」比「現在優惠單價」。

## 5. 其他頁籤與事件
- **新店：** 同 UUID 首次有效出現於 7 天內、晚於初始基準日且目前有效；重現不算新店。
- **老店新菜：** 既有店的首次有效新品，或已 inactive 至少 60 天後再次上架；需晚於初始基準日與店家首見日期，且在 7 天內。
- **平台促銷：** BOGO、多入活動及明確優惠標籤，獨立於價格比較；不能以活動標籤單獨判定降價。
- **全庫商品：** 列出 `active` 商品的**最後已知菜單報價**；店家打烊或暫時無法外送不刪除商品，網站標示「最近菜單價・非即時」或「菜單參考價・暫不可外送」。不可把舊報價冒充即時可下單價。
- **價格詳情：** 用 `reference_price/price_direction/price_change_amount` 說明前次變化；折扣主卡用 `seven_day_*`；圖表每個時間點只顯示一筆營業時段有效實質單價，同時保留 RAW 標價與促銷分類。
- **歷史菜單參考：** `archive_*` 只能另列「曾有此菜單標價但未驗證可下單」，與正式七日低價隔離。

## 6. 首頁資料品質透明度
顯示可搜尋菜單商品總數、最新批已驗證可外送商品數、擁有七日有效基準的商品數、七天內有效採集日期數、平台促銷商品數。沒有基準時標註「歷史不足」而不是默認沒有特價。店家總數/商品庫總數不等於當批有效可比較數。

## 7. 發布架構
GitHub Actions `.github/workflows/crawl.yml` → Python 採集/有效營業判斷 → HF RAW/正規化狀態 → 七日最高有效價計算與稽核 → Packed DB 及 Turso 差異發布 → GitHub Pages。前端使用受限唯讀 token，寫入 token 只能在 Actions secrets。發布新定義時要重算衍生價格和查詢索引，不能沿用舊快照的折扣百分比。

## 8. 驗收條件
| 測試情境 | 預期 |
| --- | --- |
| 6 天前 100，近 5 次 50，最新 50 | 仍降 50%，入 ≥30% 主清單 |
| 前七天最高 100、最新 80 | 下降 20%，不入 ≥30% |
| 前七天只有 50，最新 50 | 0%，不入主清單 |
| 100 發生在 8 天前，其餘有效價 50，最新 50 | 不使用過期 100；0% |
| 100 僅來自非營業時段 RAW、營業時段有效價均 50 | 不列正式降價；舊菜單價可保留商品目錄參考 |
| 同名商品不同店家 ID | 不互相比價 |
| 買一送一 $100 → 普通單件 $60 | 實質價 $50→$60，是上漲，不是假特價 |
| 最新批商品未在實際營業時段取得價格 | 仍保留一般菜單搜尋，但不列入本批七日降價；營業時段有菜單但不能外送者可比較單價，須標示非下單保證 |
| 七日有效歷史完全缺失 | 參考價 NULL；顯示不可比較 |

CI 驗證包含 `tests/test_price_comparison.py`、`tests/test_seven_day_deals.py`、`scripts/audit_deal_pipeline.py`、`scripts/audit_hf_price_history.py`。任何 published `seven_day_*` 與重新計算結果不一致，應阻擋發布。

## 9. 2026-10-27 現場資料事實與限制
最新成功 HF RAW 7 日稽核共 17 批、原始店家觀測 9,146 筆，其中有效店家觀測 1,597；當批 4,654 項有效商品，只有 964 項有七日有效歷史可比較，正式降幅 ≥30% 為 0。另 2 項歷史菜單報價對比達 30% 以上，但不符合「當時可下單」驗證，所以**不得混入正式特價**。畫面顯示的商品庫存量不代表都有足夠價格歷史；資料缺口要透明揭露而非取消價格驗證。這是 2026-10-27 批次稽核事實，後續數字隨新批次更新。

## 菜單顯示／營業／外送的獨立狀態（2026-10-28 修正）

- 一般商品索引使用最後一次有效取得的菜單與報價（`menu_quote_price`、`menu_quote_effective_price`、`menu_quote_promo_type`、`menu_quote_quantity`），不要求商家最新批營業、也不要求當下可以配送。無法外送只是一種配送狀態，不是刪除商品的理由。
- 如同店當批菜單仍存在、當時依時段應營業，但 `Delivery unavailable`，也保留該批菜單，將 `delivery_available=0`；購物介面顯示「菜單參考價・暫不可外送」。
- 沒有在最新批確認的商品仍可搜尋，但顯示「最近菜單價・非即時」，不得宣稱目前可下單。
- 正式七日菜單價差維持同店同 SKU，最新**營業時段有效實質單價**相較前七日最高**營業時段有效實質單價**；歷史可含 `Delivery unavailable`，但不能當成已確認可下單。另保留 `delivery_available` 供標示。
- 需從 HF 原始菜單重新整理／重建最後已知菜單欄位，僅更換前端文字並不能修復先前被排除的產品。

## 2026-10-28 正式七日菜單比價口徑（已更正）

- **資料入庫率**：RAW 取得的正數菜單商品報價，按 `store_uuid + product_uuid` 去重後成功留在產品資料庫的比例。不能外送、已打烊皆不得視為 RAW 菜單抓取失敗。
- **價格可比較率**：最新一批有報價的同店同 SKU，最近七日（不含當批）至少有一筆**營業時段有效** RAW 菜單實質單價者的比例；不因單純 `Delivery unavailable` 排除，但未營業時段不納入比價。
- **正式七日菜單價差**：`products.menu_quote_history` 保存最多七日內，每批同店同 SKU 一筆菜單實質單價（買一送一等先換算每件成本）；`repair_effective_prices.py` 用最新菜單單價對過去七日最高菜單單價計算 `seven_day_*`，並直接寫入 Turso 正式索引。
- `delivery_available` 及嚴格 `price_observation_valid` 仍各自保存作為**能否外送下單的資訊**，不能再阻擋菜單價歷史比較。當下未確認外送者，網站必須標「菜單報價降幅、非下單保證」，不能宣稱已確認優惠。
- 店家明確營業且菜單真的缺少某商品時，正常累計下架觀測；打烊不刪歷史商品。
- 不能將不同店家、不同 SKU 相互比較，不能只比菜單原價而忽略實質件數，不能將同一 SKU 同批跨分類重複計算；7 日內沒有先前報價者不宣稱降價。
- `scripts/audit_deal_pipeline.py` 從產品 `menu_quote_history` 獨立重算，核對已發布的 `seven_day_*` 欄位，不一致必須擋下發布。


## 原生促銷來源及未知分類（2026-10-29）

- 網站商品卡片不展示 Uber 原始菜單分類文字（例如 Featured items、Offers、一般商品）；分類欄位仍用於查詢及稽核。真正可辨識的促銷標籤仍展示。
- 優先使用商品級明確的原生結構化優惠類型（僅白名單確認的 BOGO、買 X 送 Y、第二件半價），其次才由菜單分類、商品名稱、描述文字推斷；同一 UUID 多分類去重時，結構化證據優先。
- 未知促銷種類或僅有 Offers 等籠統標題時，**不擅自用半價計算**；新批次在 report 的 `promotion_source_audit` 留下分類、原始 API 相關欄位路徑及來源數量供後續規則擴充。
- `ApiRaw/` 原始 API 回應與 `Json/` 派生菜單一同封存於批次 tar.gz，必須完整、檢核數量，兩者不得混入同一份價格重播輸入。完整原生 JSON 於新採集啟用後才會存在，不能把舊 Schema.org 快照冒稱 getStoreV1 原始回應。
- 因 Uber Web API 可能改版，首次實際採集後須以 `promotion_source_audit.observed_native_promotion_field_paths` 檢查可用欄位，再擴充已證實的映射；不能假設所有優惠皆提供商品級固定欄位。
