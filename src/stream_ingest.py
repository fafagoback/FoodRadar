"""Production ingestion: read one worker ZIP at a time, never expand its Raw."""
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import zipfile

import requests

from crawl_queue import artifact_name
from distributed_crawl import ROOT, TW, load_shared_discovery, merge_shards
from dynamic_crawl import shared_identity, verify_queue, runtime_queue, validate_task_document, worker_reports
from raw_io import RawWriter, digest_file, disk_report, safe_name


class GitHubSource:
    def __init__(self, repo, token):
        self.base = "https://api.github.com/repos/" + repo
        self.http = requests.Session()
        self.http.headers.update({"Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})

    def get(self, path, **params):
        with self.http.get(self.base + path, params=params, timeout=(20, 120)) as response:
            response.raise_for_status()
            return response.json()

    def pages(self, path, key):
        result = []
        for page in range(1, 1001):
            data = self.get(path, per_page=100, page=page)[key]
            result.extend(data)
            if len(data) < 100:
                return result
        raise RuntimeError("GitHub pagination bound exceeded")

    @contextmanager
    def zip(self, artifact, directory):
        path = Path(directory) / (str(artifact["id"]) + ".zip")
        for attempt in range(3):
            try:
                with self.http.get(self.base + "/actions/artifacts/" + str(artifact["id"]) + "/zip",
                                   stream=True, timeout=(30, 180)) as response:
                    response.raise_for_status()
                    with path.open("wb") as out:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                out.write(chunk)
                expected = artifact.get("digest")
                if expected and expected != "sha256:" + digest_file(path):
                    raise ValueError("GitHub artifact ZIP SHA256 mismatch")
                break
            except (requests.RequestException, OSError):
                path.unlink(missing_ok=True)
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        try:
            with zipfile.ZipFile(path) as archive:
                entries = [i for i in archive.infolist() if not i.is_dir()]
                names = [safe_name(i.filename) for i in entries]
                if len(names) != len(set(names)):
                    raise ValueError("Duplicate ZIP member")
                print("FOODRADAR_INPUT_ZIP=" + json.dumps({
                    "name": artifact["name"], "id": artifact["id"],
                    "zip_bytes": path.stat().st_size, "raw_bytes": sum(i.file_size for i in entries),
                    "files": len(entries)}), flush=True)
                yield archive, set(names)
        finally:
            path.unlink(missing_ok=True)


@contextmanager
def source_identity(run_id, sha, attempt):
    values = {"GITHUB_RUN_ID": str(run_id), "GITHUB_SHA": sha,
              "GITHUB_RUN_ATTEMPT": str(attempt)}
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def recovery_evidence(gh, run_id):
    """Read-only recovery of a completed whole-store run with all workers finalized."""
    run = gh.get("/actions/runs/" + run_id)
    if run.get("status") != "completed" or run.get("path") != ".github/workflows/crawl.yml":
        raise ValueError("Recovery source is not a completed crawl workflow")
    jobs = gh.pages("/actions/runs/" + run_id + "/jobs", "jobs")
    workers = [j for j in jobs if re.fullmatch(r"爬蟲工作機 \d+", j["name"])]
    if ({int(j["name"].split()[-1]) for j in workers} != set(range(1, 16))
            or len(workers) != 15 or any(j["conclusion"] != "success" for j in workers)
            or any(not any(s["name"] == "一次確認本台全部店家已持久保存"
                           and s["conclusion"] == "success" for s in j["steps"]) for j in workers)):
        raise ValueError("Recovery source lacks all 15 durable finalized workers")
    plan = [j for j in jobs if j["name"] == "檢查是否需要故障接手"]
    recovery = [j for j in jobs if j["name"].startswith("接手 01")]
    ingest = [j for j in jobs if j["name"].startswith("第十六台")]
    if (len(plan) != 1 or plan[0]["conclusion"] != "success"
            or not recovery or any(j["conclusion"] != "skipped" for j in recovery)
            or len(ingest) != 1 or ingest[0]["conclusion"] != "failure"):
        raise ValueError("Recovery requires original zero-recovery plan and failed ingest")
    # Never assign an old observation the date of the repair.
    batch = datetime.fromisoformat(ingest[0]["started_at"].replace("Z", "+00:00")).astimezone(TW).strftime("%Y%m%d%H%M%S")
    return run["head_sha"], run["run_attempt"], batch


def read_worker(archive, names, state, artifact, expected, writer, reports_dir):
    manifest = json.loads(archive.read("batch.json"))
    summary = json.loads(archive.read("summary.json"))
    name = artifact["name"]
    match = re.fullmatch(r"fr-worker-([a-f0-9]{16})-(\d+)-w(\d{2})", name)
    if match is None:
        raise ValueError("Invalid worker artifact name")
    epoch, worker, wave = match[1], int(match[2]), int(match[3])
    if epoch != state["epoch"] or not 0 <= worker < 15:
        raise ValueError("Worker epoch or index mismatch")
    for key, value in {"run_id": state["run_id"], "source_sha": state["source_sha"],
            "epoch": epoch, "worker": worker, "wave": wave, "name": name}.items():
        if manifest.get(key) != value:
            raise ValueError("Worker manifest mismatch: " + key)
    for key, value in {"run_id": state["run_id"], "source_sha": state["source_sha"],
            "epoch": epoch, "shard_index": worker, "wave": wave}.items():
        if summary.get(key) != value:
            raise ValueError("Worker summary mismatch: " + key)
    records = manifest.get("records")
    if (not isinstance(records, dict) or manifest.get("sections")
            or summary.get("completed") != len(records) or summary.get("sections_completed", 0)):
        raise ValueError("Worker roster or summary is not whole-store capture")
    if expected is not None and set(records) != set(expected):
        raise ValueError("Artifact differs from finalized task pool roster")
    # Group ZIP members once, not an O(number of stores * files) scan.
    folders = defaultdict(set)
    for path in names - {"batch.json", "summary.json"}:
        pieces = path.split("/", 2)
        if len(pieces) != 3 or pieces[0] != "tasks":
            raise ValueError("Unexpected worker ZIP member: " + path)
        folders[pieces[1]].add(pieces[2])
    expected_folders = set()
    stores, checks = [], {}
    for sid, token in records.items():
        if not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", sid) or not re.fullmatch(r"[a-f0-9]{32}", token):
            raise ValueError("Invalid store or lease identity")
        lease = {"worker": worker, "token": token}
        if expected is not None:
            row = expected[sid]
            if (row["worker"] != worker or row["token"] != token
                    or row.get("owner") != manifest.get("owner")
                    or int(row["artifact_id"]) != int(artifact["id"])):
                raise ValueError("Artifact lease differs from finalized task pool")
        folder = artifact_name(state, sid, lease)
        expected_folders.add(folder)
        prefix = "tasks/" + folder + "/"
        doc = json.loads(archive.read(prefix + "task.json"))
        claimed = doc["responses"]
        actual_names = folders[folder]
        if actual_names != {"task.json"} | {"raw/" + key for key in claimed}:
            raise ValueError("Task ZIP member list mismatch")
        validate_task_document(doc, state, sid, lease, claimed)
        if doc["store"].get("catalog_deferred") or doc["store"].get("catalog_deferred_children"):
            raise ValueError("Deferred catalogs are not whole-store results")
        # Store metadata first, followed by its catalog pages. Physical order
        # follows manifest order, enabling local indexed reads during replay.
        ordered = sorted(claimed, key=lambda key: (key != doc["store"].get("response_path"), key))
        for relative in ordered:
            if relative in checks:
                raise ValueError("Duplicate raw body within worker")
            body = archive.read(prefix + "raw/" + relative)
            writer.add(relative, body, claimed[relative])
            checks[relative] = claimed[relative]
        stores.append(doc["store"])
    if set(folders) != expected_folders:
        raise ValueError("Unexpected or missing task folders")
    folder = reports_dir / name
    folder.mkdir(parents=True, exist_ok=False)
    (folder / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    return worker, stores, checks


def run(source_run_id=None):
    config = json.loads((ROOT / "config.json").read_text())
    current_run = os.environ["GITHUB_RUN_ID"]
    source_run = str(source_run_id or current_run)
    reuse = source_run != current_run
    if not source_run.isdigit():
        raise ValueError("Invalid source run ID")
    gh = GitHubSource(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"])
    if reuse:
        sha, attempt, batch = recovery_evidence(gh, source_run)
    else:
        sha, attempt, batch = os.environ["GITHUB_SHA"], int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")), None
    stage = ROOT / "data/raw-stage"
    stage.mkdir(parents=True, exist_ok=False)
    reports_dir = ROOT / "data/worker-summaries"
    parts = ROOT / "data/parts"
    discovery = ROOT / "data/discovery"
    discovery.mkdir(parents=True, exist_ok=False)
    artifacts = gh.pages("/actions/runs/" + source_run + "/artifacts", "artifacts")
    feeds = [a for a in artifacts if a["name"].startswith("foodradar-shared-feed-") and not a["expired"]]
    if len(feeds) != 1:
        raise ValueError("Missing or ambiguous immutable discovery artifact")
    with gh.zip(feeds[0], stage) as (z, names):
        for name in names:
            if name != "discovery.json" and not (name.startswith("raw/feed/") and name.endswith(".body")):
                raise ValueError("Unexpected feed ZIP member: " + name)
            dest = discovery / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            with z.open(name) as src, dest.open("xb") as out:
                shutil.copyfileobj(src, out, length=1024 * 1024)
    discovery_file = discovery / "discovery.json"
    with source_identity(source_run, sha, attempt):
        shared, digest, ids = shared_identity(discovery_file)
        load_shared_discovery(discovery_file, config, discovery, -1)
        if reuse:
            state = {"run_id": source_run, "source_sha": sha, "epoch": digest[:16]}
            accepted = None
        else:
            state = runtime_queue().read()
            verify_queue(state, shared, digest, ids)
            if state["pending"] or state["leased"] or state.get("local_done") or state.get("catalog_groups") or set(state["completed"]) != set(ids):
                raise ValueError("Task pool not fully finalized as whole stores")
            accepted = defaultdict(dict)
            for sid, lease in state["completed"].items():
                accepted[lease["batch_name"]][sid] = lease
    if feeds[0]["name"] != "foodradar-shared-feed-" + state["epoch"]:
        raise ValueError("Discovery artifact epoch mismatch")
    workers = [a for a in artifacts if a["name"].startswith("fr-worker-" + state["epoch"] + "-") and not a["expired"]]
    if len({a["name"] for a in workers}) != len(workers):
        raise ValueError("Ambiguous worker artifact names")
    if reuse:
        if {a["name"] for a in workers} != {f"fr-worker-{state['epoch']}-{i}-w00" for i in range(15)}:
            raise ValueError("Recovery source must contain exactly 15 wave-zero worker artifacts")
    else:
        workers = [a for a in workers if a["name"] in accepted]
        if {a["name"] for a in workers} != set(accepted):
            raise ValueError("Finalized worker artifact is missing")
    workers.sort(key=lambda a: tuple(map(int, re.search(r"-(\d+)-w(\d+)$", a["name"]).groups())))
    owners, by_worker, checks_by_worker = {}, defaultdict(list), defaultdict(dict)
    archive_path = stage / "capture.tar.gz"
    disk_report("before_stream", stage, source_run=source_run, reuse=reuse)
    with RawWriter(archive_path) as writer:
        for name, checksum in shared["raw_checksums"].items():
            writer.add(name, (discovery / "raw" / name).read_bytes(), checksum)
            checks_by_worker[0][name] = checksum
        shutil.rmtree(discovery / "raw")
        for a in workers:
            with gh.zip(a, stage) as (z, names):
                worker, stores, checks = read_worker(z, names, state, a,
                    None if reuse else accepted[a["name"]], writer, reports_dir)
            for store in stores:
                sid = store["store_uuid"]
                if sid in owners:
                    raise ValueError("Store was captured in more than one accepted artifact")
                owners[sid] = worker
            by_worker[worker].extend(stores)
            checks_by_worker[worker].update(checks)
            disk_report("worker_complete", stage, worker=worker + 1,
                        verified_stores=len(owners), raw_bytes=writer.raw_bytes,
                        archive_bytes=archive_path.stat().st_size)
        if set(owners) != set(ids):
            raise ValueError("Worker artifacts do not cover exactly the original discovery roster")
        response_checks = writer.checks
        raw_bytes = writer.raw_bytes
    disk_report("archive_complete", stage, raw_bytes=raw_bytes,
                archive_bytes=archive_path.stat().st_size, stores=len(owners))
    archive_bytes = archive_path.stat().st_size
    for index in range(15):
        summary = worker_reports(reports_dir, state, index)
        folder = parts / f"foodradar-raw-shard-{index}"
        folder.mkdir(parents=True, exist_ok=False)
        # Only metadata is written; Raw is already inside the single archive.
        stores = by_worker[index]
        meta = {"format": "foodradar-shard-v1", "run_id": source_run,
            "source_sha": sha, "shard_index": index, "shard_count": 15,
            "public_ip": summary.get("public_ip"), "feed_pages": shared["feed_pages"],
            "shared_discovery_digest": digest, "shared_owners": owners,
            "discovered_store_uuids": ids, "assigned_store_uuids": sorted(s["store_uuid"] for s in stores),
            "stores": stores, "thread_learning": summary.get("thread_learning", {}),
            "api": summary.get("api", {}), "responses": checks_by_worker[index],
            "worker_waves": summary.get("wave_count", 0), "worker_ips": summary.get("worker_ips", []),
            "artifact_uploads": summary.get("artifact_uploads", 0), "timing": summary.get("timing", {})}
        (folder / "shard.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    merge_shards(config, parts, count=15, discovery_file=discovery_file, upload_raw=True,
                 prepared_archive=archive_path, verified_checks=checks_by_worker,
                 batch_override=batch)
    print("FOODRADAR_STREAM_INGEST_COMPLETE=" + json.dumps({
        "source_run": source_run, "reused": reuse, "stores": len(owners),
        "responses": len(response_checks), "raw_bytes": raw_bytes,
        "archive_bytes": archive_bytes}), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source-run-id")
    args = p.parse_args()
    run(args.source_run_id)
