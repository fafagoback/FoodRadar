"""Immutable HF Raw replay: incomplete captures cannot block complete history."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pipeline import raw_manifest_should_replay


def manifest(batch="20261109100016", complete=True):
    return {
        "format": "foodradar-immutable-http-body-v1",
        "batch_id": batch,
        "complete": complete,
        "archive_path": f"Raw/{batch}/capture.tar.gz",
    }


class HFIncompleteRawReplayTests(unittest.TestCase):
    def test_explicit_failed_capture_is_archived_but_skipped(self):
        self.assertFalse(raw_manifest_should_replay(
            manifest(complete=False), "Raw/20261109100016/manifest.json", "Raw"))

    def test_complete_native_capture_remains_publishable(self):
        self.assertTrue(raw_manifest_should_replay(
            manifest("20261109104731", True), "Raw/20261109104731/manifest.json", "Raw"))

    def test_missing_completeness_and_bad_source_are_fatal(self):
        for changed in [{"complete": None}, {"format": "schema-derived"},
                        {"archive_path": "legacy/Json/menu.json"}, {"batch_id": "bad"}]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                raw_manifest_should_replay(
                    {**manifest(), **changed}, "Raw/20261109100016/manifest.json", "Raw")


if __name__ == "__main__":
    unittest.main()
