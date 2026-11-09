"""Inspect an immutable incomplete HF Raw capture without changing HF or Turso."""
import json
import os
import re
import tarfile
from huggingface_hub import hf_hub_download

BATCH = "20261109105106"
REPO = "hub-google/FoodRadar"


def load(filename):
    path = hf_hub_download(REPO, filename, repo_type="dataset", token=os.environ["HF_TOKEN"])
    return path


def main():
    with open(load(f"Raw/{BATCH}/manifest.json"), encoding="utf-8") as file:
        meta = json.load(file)
    failures = [s for s in meta.get("stores", []) if not s.get("success")]
    records = []
    with tarfile.open(load(meta["archive_path"]), "r:gz") as archive:
        names = archive.getnames()
        for store in failures:
            sid = store["store_uuid"]
            paths = sorted(n for n in names if n.startswith(f"catalog/{sid}/"))
            status_counts = {}
            examples = []
            for path in paths:
                match = re.search(r"status-(\d+)\.body$", path)
                code = match.group(1) if match else "unknown"
                status_counts[code] = status_counts.get(code, 0) + 1
                try:
                    response = json.load(archive.extractfile(path))
                    data = response.get("data") or {}
                    entry = {
                        "file": path.rsplit("/", 1)[-1],
                        "status": code,
                        "outer_status": response.get("status"),
                        "data_keys": list(data)[:12],
                        "error_code": str(data.get("code") or "")[:60],
                        "error_message": str(data.get("message") or response.get("message") or "")[:160],
                        "paging_info": data.get("pagingInfo")
                    }
                except Exception as exc:
                    entry = {"file": path.rsplit("/", 1)[-1], "status": code,
                             "decode_error": str(exc)[:100]}
                if len(examples) < 28:
                    examples.append(entry)
            records.append({
                "store_uuid": sid, "error": store.get("error"),
                "native_http_status_counts": status_counts,
                "catalog_response_count": len(paths),
                "sample_responses": examples[-22:]
            })
    print("HF_FAILED_RAW_RESPONSE_DIAGNOSTICS=" +
          json.dumps({"batch": BATCH, "failures": records}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
