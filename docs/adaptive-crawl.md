# FR GitHub 原生動態排程與自適應並行

- 掃描後以歷史耗時建立不重複的微批次，至少在工作足夠時開 15 個批次，約每 12 店一批，最多 256 個。GitHub Actions Matrix 的 max-parallel=15 管理共用待執行批次；一個工作槽完成立即由 GitHub 分派下一批。大型批次優先建立，批內大型店優先。沒有外部任務池或頻繁 Git API 寫入。
- GitHub-hosted runner 完成 job 後被回收；下一批由新 runner 啟動，並非常駐 15 台可互相接手執行中的 Python。大型店仍在本機並行獨立分類（最多 3 個），未實作跨工作機單店分頁移交。
- 每個工作機在該批次 RUN 內調整整店執行緒（1–12），保留現有正常 API 間隔，429 遵守 Retry-After、立即降速、在原頁重試。持續限流超過重試或 job 時限會失敗，不把不完整資料發成完整。
- 所有批次共用單一 Repo 初始值。每個有效批次回報最佳穩定值，彙整機取有效工作機報告算術平均、四捨五入，僅不同時寫入 crawler-state/crawl-settings.json。清除 Actions 不影響設定。
- 由原始清單、唯一所有權、每份原始 HTTP SHA-256 和所有店家狀態共同驗收，嚴格完整才更新資料庫及部署。保留原始 HF 格式。初次矩陣有 job 失敗時，同一 RUN 自動列出本次未成功上傳成果的批次並由第二組矩陣補抓；已保存成果的批次絕不重新分派。崩潰前未保存的部分成果仍可能需要重新抓取。
- 不使用 Supabase、Cloudflare 或新的付費服務。未改動資料庫儲存演算法。

- 整店抓取使用執行緒本機 HTTPS 連線池，每次店家建立新的 Session、Cookie jar；獨立促銷覆核不共用 Session 或連線池。
- Artifact 名稱使用 discovery SHA-256 前綴隔離；彙整必須核對當次完整 discovery SHA-256、所有權、RUN 及來源 commit。
