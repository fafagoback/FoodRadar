"""Offline stage-16 HF Raw assertions. No network and no UE API calls."""
from __future__ import annotations
import json,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
sys.path.insert(0,str(ROOT/"src"))
from db_stage_hf import verified_raw

class HF16Tests(unittest.TestCase):
    def verify(self, exists=True, remote_sha="a"*64):
        from huggingface_hub import HfApi
        batch="20261126072649"
        repo="test/repo"
        meta={"batch_id":batch,"archive_path":"Raw/"+batch+"/capture.tar.gz",
              "archive_sha256":"a"*64,"stores":[{"store_uuid":"s"}]}
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/"manifest.json"
            path.write_text(json.dumps(meta),encoding="utf-8")
            api=SimpleNamespace(file_exists=lambda *a,**kw:exists,
                get_paths_info=lambda *a,**kw:[SimpleNamespace(size=123,lfs={"sha256":remote_sha})])
            with patch("huggingface_hub.HfApi",return_value=api),\
                 patch("huggingface_hub.hf_hub_download",return_value=str(path)),\
                 patch("pipeline.raw_manifest_should_replay",return_value=True),\
                 patch.dict("os.environ",{"HF_TOKEN":"mock"}):
                return verified_raw({"hf_repo_id":repo,"hf_folder":"Raw"},batch)

    def test_manifest_and_remote_lfs_hash_are_required(self):
        self.assertEqual(self.verify()["bytes"],123)
        with self.assertRaisesRegex(RuntimeError,"SHA256"):
            self.verify(remote_sha="b"*64)

    def test_no_automatic_upload_when_raw_missing(self):
        self.assertIsNone(self.verify(exists=False))

if __name__=="__main__":
    unittest.main()
