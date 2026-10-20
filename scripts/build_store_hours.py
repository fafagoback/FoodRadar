"""Build an independent opening-hours lookup from the latest immutable HF raw archive.

This is presentation/analysis metadata only. No serving DB tables or raw snapshots
are modified. A missing hours specification means we cannot independently check
opening hours; the original explicit isOpen flag must still be checked.
"""
import json
import os
import sys
import tarfile
from pathlib import Path
from huggingface_hub import hf_hub_download

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from serving_state import store_identity


def main():
    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    repo_id = cfg["hf_repo_id"]
    latest = f'{cfg["hf_folder"].strip("/")}/latest.json'
    token = os.environ.get("HF_TOKEN")
    manifest = json.loads(Path(hf_hub_download(repo_id=repo_id, filename=latest, repo_type="dataset", token=token)).read_text(encoding="utf-8"))
    if not manifest.get("complete") or not manifest.get("archive_path"):
        raise RuntimeError("No verified raw archive available for opening-hours lookup")
    path = hf_hub_download(repo_id=repo_id, filename=manifest["archive_path"], repo_type="dataset", token=token)
    lookup = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            if not member.isfile() or not member.name.startswith("Json/") or not member.name.endswith(".json"):
                continue
            stream = archive.extractfile(member)
            if stream is None:
                continue
            doc = json.load(stream)
            specs = doc.get("openingHoursSpecification")
            if not isinstance(specs, list) or not specs:
                continue
            sid, _ = store_identity(doc)
            slots = []
            for spec in specs:
                if not isinstance(spec, dict):
                    continue
                days = spec.get("dayOfWeek") or []
                if isinstance(days, str):
                    days = [days]
                opening, closing = spec.get("opens"), spec.get("closes")
                if not (isinstance(days, list) and opening and closing):
                    continue
                slots.append({"days": days, "opens": opening, "closes": closing})
            if slots:
                lookup[sid] = slots

    # Failing here is safer than deploying a silent missing-lookup fallback.
    target_id = "34f9b599-c1f4-5e89-9434-84a433989755"
    if target_id not in lookup:
        raise RuntimeError("Louisa Tianmu opening hours unavailable in latest raw snapshot; cannot validate target case")
    output = ROOT / "web" / "data" / "store-hours.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"batch": manifest["batch_id"], "hours": lookup}, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"OPENING_HOURS_LOOKUP stores={len(lookup)} Louisa={json.dumps(lookup[target_id], ensure_ascii=False)}")


if __name__ == "__main__":
    main()
