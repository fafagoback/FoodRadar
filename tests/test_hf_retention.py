"""Regression tests for rolling HF Raw expiry; never access real HF."""
import hashlib
import json
import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import hf_retention as r
from state_checkpoint import manifest_digest

OLD = "20260927065959"
EDGE = "20260927070000"
NEW = "20261126063000"
NOW = datetime(2026, 11, 26, 7, tzinfo=r.TW)


def raw(batch):
    prefix = f"Raw/{batch}/"
    archive = (batch + " archive").encode()
    meta = {"batch_id": batch, "complete": True,
            "format": "foodradar-immutable-http-body-v1",
            "archive_path": prefix + "raw.tar.zst",
            "archive_sha256": hashlib.sha256(archive).hexdigest()}
    return {prefix + "manifest.json": r.pack(meta), prefix + "raw.tar.zst": archive}


class FakeHF:
    def __init__(self, files):
        self.files = dict(files)
        self.rev = 1
        self.commits = 3
        self.squashes = 0
        self.writes = 0

    def repo_info(self, *args, **kwargs):
        return SimpleNamespace(sha=str(self.rev))

    def list_repo_files(self, *args, **kwargs):
        return list(self.files)

    def list_repo_commits(self, *args, **kwargs):
        return list(range(self.commits))

    def list_repo_refs(self, *args, **kwargs):
        return SimpleNamespace(branches=[SimpleNamespace(name="main")], tags=[], pull_requests=[])

    def get_paths_info(self, repo, paths, **kwargs):
        return [SimpleNamespace(path=p, lfs=SimpleNamespace(
            sha256=hashlib.sha256(self.files[p]).hexdigest())) for p in paths]

    def create_commit(self, repo, *, operations, parent_commit, **kwargs):
        assert repo == r.REPO and parent_commit == str(self.rev)
        from huggingface_hub import CommitOperationDelete
        for op in operations:
            if isinstance(op, CommitOperationDelete):
                del self.files[op.path_in_repo]
            else:
                self.files[op.path_in_repo] = op.path_or_fileobj
        self.rev += 1
        self.commits += 1
        self.writes += 1

    def super_squash_history(self, repo, *, branch, **kwargs):
        assert repo == r.REPO and branch == "main"
        self.rev += 1
        self.commits = 1
        self.squashes += 1


def fixture():
    files = {**raw(OLD), **raw(EDGE), **raw(NEW)}
    rows = [(p, json.loads(b), hashlib.sha256(b).hexdigest())
            for p, b in sorted(files.items()) if p.endswith("/manifest.json")]
    state = b"verified compact price state"
    meta = {"format": "foodradar-checkpoint-v4", "repo_id": r.REPO, "folder": "Raw",
            "manifest_count": len(rows), "manifest_digest": manifest_digest(rows),
            "verified_batches": len(rows), "batch_id": NEW,
            "retention_days": 60, "retention_cutoff": "2026-09-27T07:00:00+08:00",
            "compact_sha256": hashlib.sha256(state).hexdigest()}
    files[r.STATE + "manifest.json"] = r.pack(meta)
    files[r.STATE + "serving.compact.db"] = state
    files["Raw/latest.json"] = files[f"Raw/{NEW}/manifest.json"]
    files["Raw/latest-partial.json"] = files[f"Raw/{OLD}/manifest.json"]
    return files, rows


class RetentionTests(unittest.TestCase):
    def run_cleanup(self, api, dry=False):
        with patch.object(r, "config", return_value={}), \
             patch.object(r, "read", side_effect=lambda path, rev: api.files[path]), \
             patch.dict(os.environ, {}, clear=True):
            r.cleanup(api, dry=dry, now=NOW)

    def test_rolling_boundary(self):
        files, _ = fixture()
        self.assertEqual(r.expired_batches(files, NOW), {OLD})

    def test_dry_run_does_not_modify_hf(self):
        files, _ = fixture()
        api = FakeHF(files)
        self.run_cleanup(api, dry=True)
        self.assertEqual(api.files, files)
        self.assertEqual((api.writes, api.squashes), (0, 0))

    def test_delete_expired_and_squash_preserve_checkpoint_prefix(self):
        files, original_rows = fixture()
        api = FakeHF(files)
        self.run_cleanup(api)
        self.assertFalse(any(p.startswith(f"Raw/{OLD}/") for p in api.files))
        self.assertNotIn("Raw/latest-partial.json", api.files)
        self.assertEqual(api.files["Raw/latest.json"], files["Raw/latest.json"])
        for p in (*raw(EDGE), *raw(NEW), r.STATE + "serving.compact.db"):
            self.assertEqual(api.files[p], files[p])
        self.assertEqual((api.writes, api.squashes, api.commits), (1, 1, 1))
        with patch.object(r, "read", side_effect=lambda path, rev: api.files[path]):
            records = r.all_records(set(api.files), str(api.rev))
            self.assertEqual(manifest_digest(records), manifest_digest(original_rows))
            self.assertEqual(r.check_prefix(json.loads(api.files[r.STATE + "manifest.json"]), records), 3)
        self.run_cleanup(api)
        self.assertEqual((api.writes, api.squashes), (1, 1))

    def test_no_verified_checkpoint_no_deletion(self):
        files, _ = fixture()
        del files[r.STATE + "manifest.json"]
        api = FakeHF(files)
        with self.assertRaisesRegex(ValueError, "No verified durable checkpoint"):
            self.run_cleanup(api)
        self.assertEqual((api.writes, api.squashes), (0, 0))

    def test_damaged_checkpoint_no_deletion(self):
        files, _ = fixture()
        files[r.STATE + "serving.compact.db"] = b"damaged"
        api = FakeHF(files)
        with self.assertRaisesRegex(ValueError, "SHA mismatch"):
            self.run_cleanup(api)
        self.assertEqual((api.writes, api.squashes), (0, 0))


if __name__ == "__main__":
    unittest.main()
