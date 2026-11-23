"""Create the standalone FoodRadar dataset without relying on old UberEat.

The old repository may be deleted or inaccessible. We deliberately never read
or import it; GitHub Actions captures a fresh raw batch into this dataset.
"""
import json
import os

from huggingface_hub import CommitOperationAdd, HfApi

REPO = "hub-google/FoodRadar"
CARD = """---
language:
- zh
pretty_name: FoodRadar 原始 API 回應
---

# FoodRadar 原始資料庫

這是 `hub-google/FoodRadar` 獨立 Hugging Face Dataset Repository，
與舊 `hub-google/UberEat` 無關。

`Raw/<YYYYMMDDHHMMSS>/capture.tar.gz` 保存原封不動的
Uber Eats API HTTP 回應內容，tar.gz 只是無損壓縮。
`Raw/<YYYYMMDDHHMMSS>/manifest.json` 記錄原始檔 SHA-256，
`Raw/latest.json` 指向最近的完整封存批次。

抓取階段不改任何欄位、價格、促銷或商品資料。
FoodRadar 會在從本 Dataset 讀取後才轉換成 Turso 使用的資料。
"""

def main():
    token=os.environ["HF_TOKEN"]
    api=HfApi(token=token)
    api.create_repo(repo_id=REPO, repo_type="dataset",
                    private=False, exist_ok=True)
    files=set(api.list_repo_files(repo_id=REPO, repo_type="dataset"))
    if "README.md" not in files:
        api.create_commit(
            repo_id=REPO,repo_type="dataset",
            operations=[CommitOperationAdd(path_in_repo="README.md",
                                           path_or_fileobj=CARD.encode("utf-8"))],
            commit_message="Initialize independent FoodRadar raw Dataset")
    info=api.repo_info(repo_id=REPO,repo_type="dataset")
    if info.id != REPO:
        raise ValueError("Created Hugging Face dataset does not match intended namespace")
    print("FOODRADAR_DATASET_CREATED="+json.dumps({
        "repo_id":info.id,
        "private":info.private,
        "root":"Raw/",
        "legacy_repository_read":False,
    },ensure_ascii=False),flush=True)

if __name__=="__main__":
    main()
