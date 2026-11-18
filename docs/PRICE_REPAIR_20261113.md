# FoodRadar 2026/11/13 價格批次修復紀錄

## 範圍與來源事實
- 來源：HF Dataset `hub-google/FoodRadar`
- 使用者指定批次：`20261113110657`，代表台灣時間 2026/11/13 11:06 的採集批次。
- 指定店家：`Uber Eats 優市 北投店`。
- 截圖商品：`ARIEL抗菌洗衣精800g瓶裝-高效除蟎`，畫面出現 $89 → $229 → $89。
- **可直接核實的來源事實**：此批次的原始 HTTP SHA-256 校驗通過，原始資料包含指定店家。舊版解析器未能從該批次重現截圖中確切的 $229 商品報價，因此**不得將截圖尖峰說成已被 Raw 原文逐項證實**。使用者明確要求刪除整批，修復對象始終是這個確切 batch，而非猜測某個商品 ID。

## 精確刪除、備份與重算
GitHub workflow：`.github/workflows/repair-20261113.yml`。
1. 先執行完整回歸測試、校驗 HF manifest 與原始 HTTP 壓縮檔 SHA-256，核實指定店家出現在該批次。
2. 刪除前將原始 `manifest.json`、`capture.tar.gz` 與驗證報告上傳到 GitHub Actions Artifact，保留 30 天。
3. 以單一 HF commit 僅刪除 `Raw/20261113110657/manifest.json`、`Raw/20261113110657/capture.tar.gz`，並確認兩檔都不在 HF 目前版本。
4. 使用 `FOODRADAR_REBUILD_VALID_PRICE_HISTORY=1` 全量回放剩餘 HF Raw（不信任受污染的 SQLite cache），確定該 batch 不再出現在 `crawl_batches` 與 `product_price_observations`。
5. 通過本地價格與打包契約檢查後，以原有 `publish_turso_delta.py` 進行有交易保護的差異發布；最後以獨立唯讀連線核對正式 Turso。

## Turso 寫入限制
2026/11/18 的爬蟲執行紀錄 #411 明確回傳 `SQL write operations are forbidden (writes are blocked, do you need to upgrade your plan?)`。這是服務端寫入封鎖；不能透過重試相同 SQL、暫時隱藏前端圖點、假稱資料庫更新來繞過。

**只有 Turso 發布成功且唯讀核對成功，才可以宣稱正式站資料修復完成。** 若該工作流程因服務端寫入拒絕失敗，HF 刪除成果仍保留，Turso 以及使用中的網站可能暫時仍是較舊資料；workflow 可在寫入限制解除後重跑，HF 刪除具冪等性。

## 其他同期修正
- `src/catalog_complete.py`：支援從 `getStoreV1` 原生 sections 或 aisles 判讀實際子分類 UUID，缺乏原始證據仍禁止編造子分類或發布不完整菜單。
- `scripts/prototype_pack_db.py` + `web/packed-turso.js`：未變動、仍屬最新批次的搜尋卡片改從版本 metadata 取得採集時間，避免單純時間戳造成大量無意義的 Turso rows written；舊批次時間不會被改寫。
- GitHub `schedule` 有服務端延遲風險，不能以程式碼保證分秒不差地啟動。既定本地排程時間仍為每日 06:30、18:30（UTC+8）。
