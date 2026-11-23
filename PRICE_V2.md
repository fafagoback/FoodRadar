# FoodRadar V2：價格證據及折扣契約

## 2026-11-09 原生完整分類 API 補充（優先於舊版敘述）

- `getStoreV1` 僅作店家和分類發現，完整商品改由 `getCatalogPresentationV2` 全分類分頁回應提供，兩者不要各自建立商品/價格紀錄；每個不同 `store_uuid + product_uuid + batch_id` 只記一次報價。
- `MULTI_SKU_FLAT` 可同時附著在**已經有單品刪除線折價**的商品上。若原生 `itemThumbnailElements` 唯一顯示「現在售價 = item.price」與「較高且帶 STRIKE_THROUGH 的平台原價」，且額外結構只有 `itemLevelPromotion.type=multiSKUFlatPromotion` 及 `multiSKUFlatPromotion`，則只將**該單品折價**分類為 `DIRECT_DISCOUNT`；將滿額活動 ID/文案另外記錄，不把滿額折抵算入單品價。其他混合/衝突條件仍標示 `UNVERIFIED`。
- **區分三種原價**：`display_original_price_cents` 是平台明示刪除線原價；`product_price_observations.original_total_cents` 是買送/直接折價公式的**整組基準總價**；`seven_day_reference_price` 是 FoodRadar 觀測的七日最高有效**每件實質單價**。三者不可覆蓋、也不能互相推測。
- 商品是否售罄使用原生 `isSoldOut`、`isAvailable` 觀測做獨立標示；不影響歷史菜單存在判定，但售罄/不可售不能計入「當前可下單促銷」。
- HF `hub-google/FoodRadar/Raw/<batch>/capture.tar.gz` 只封存原始 HTTP body；所有衍生價格與商品展示欄位只在 Turso 投影。舊 `ApiRaw/`、`Json/` 是已停用歷史格式。

## 計價範圍
只支援下列優惠：
1. 同品買 X 送 Y：`item.catalogItemAnalyticsData.promoType == "BOGO"` 且
   `item.itemPromotion.type == "buyXGetYItemPromotion"`，購買數量
   `item.itemPromotion.buyXGetYItemPromotion.buyQuantity`、贈送數量
   `item.itemPromotion.buyXGetYItemPromotion.getQuantity` 兩者必須是正整數，
   不得存在額外互斥商品組合條件。原生 `item.price` 單位為「分」。
2. 單品直接降價：`item.catalogItemAnalyticsData.promoType` 是
   `DISCOUNTED_ITEM`、`CATEGORY_DISCOUNT`，或符合上方獨立刪除線驗證規則的 `MULTI_SKU_FLAT`，原生 `item.price`
   是目前菜單售價，`item.itemThumbnailElements[].payload.labelPayload.label.richTextElements[]`
   的 `text.predefinedDecorations` 必須明確包含 `STRIKE_THROUGH`
   才能取得原價；其餘非刪除線價格標籤須與目前價格完全一致。

其他包括 **買 A 送 B (BOGA)、跨 SKU、純滿額折扣、任選多件組合、免費贈品**，
不得僅靠活動文案建立單品優惠報價；但符合上方獨立且明確的單品刪除線折價時，仍可認證該單品折價，不能把額外購物車折抵攤進單品價格。
仍可保留商店／商品一般菜單資訊供搜尋，不可將忽略的優惠誤記為「沒有優惠」。

### 唯一價格公式

- 同品買送：
  - `original_total_cents = item.price * (buyQuantity + getQuantity)`
  - `payable_total_cents = item.price * buyQuantity`
  - `received_quantity = buyQuantity + getQuantity`
- 單品直接降價：
  - `original_total_cents = parsed_strikethrough_price_cents`
  - `payable_total_cents = item.price`
  - `received_quantity = 1`

統一由 `src/verified_prices.py` 使用精確 Decimal 計算：

```python
effective_unit_price = Decimal(payable_total_cents) / (Decimal(received_quantity) * 100)
discount_percentage = (Decimal(original_total_cents - payable_total_cents) * 100) / Decimal(original_total_cents)
```

不使用優惠文字、類別、badge 或推測數量重算折扣；計算後才在 UI 顯示百分比小數。
`VERIFIED_MENU` 只證明 Uber 原生菜單數值完整，**不表示實際結帳已確認**；
必須另外符合當次營業、可外送與資料時效門檻，才可作為目前可用的價格或七日比價。

### 七日歷史

只查 `product_price_observations`，同一商店同一商品、
營業且可外送的原生已驗證報價；使用過去七天「排除當次」最高精確實質單價為基準，
依 `(歷史最高單價-最新單價)/歷史最高單價*100%` 計算。
不同 SKU 或不等價的跨商品組合永不互比；缺歷史就不宣稱降價。
原始 HF `ApiRaw/` 保留且不可根據舊 `Json/` 的菜單分類重建促銷。

### 資料庫與發布

- SQLite `product_price_observations` 只接受 `NONE`、`SAME_SKU_BXGY`、
  `DIRECT_DISCOUNT`，保存來源 hash、整數分、實際數量及批次。
- `products` 僅以 `price_evidence_status`、`verified_discount_pct`
  及 V2 已驗證單價對外提供折扣。前端不得重新計算任何優惠。
- 新 HF 僅保存 `Raw/<batch>/capture.tar.gz` 及 `manifest.json`，每一份 Feed、Store、Catalog HTTP body 都有獨立檔名與 SHA-256；來源不完整、檔案覆寫或回應缺失一律禁止發布。
- 先完成完整 HF SHA256 校驗與 V2 重播，再建立 SQLite/Turso V2、
  跑資料品質與網站測試，最後才以帶有重建證明的交易切換正式 Turso。
  V1 衍生歷史不得直接延用。
- 折扣與歷史必須由同一個原生數值來源生成，不允許回復任何 V1
  `repair_effective_prices.py` 類別猜價、`web/data-contract.js` 文本猜價功能。

## 已核對的真實案例

- 萊爾富冰美式：`price=6000, buyQuantity=1, getQuantity=1`：
  菜單原價總額 $120、付 $60、得 2 件、平均 $30、折扣率 50%。
- 萊爾富黑糖奶茶：`item.price=7100`，刪除線原價 $79：
  商品 $71、折扣率 (79−71)/79=10.1265…%。
- BOGA 的 `getQuantity=0` 不是「沒贈品」的證據，此優惠一律排除。
- 2026-10-30 真實 HF 批次 `20261030100605` 成功配對
  490/490 份原始 API，讀得同品買送 611 筆、直接折價 354 筆，
  其他條件缺失者保持未驗證，不假算。
