import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from raw_io import RawWriter, DocumentSpool, seekable_archive


class StreamingRawIO(unittest.TestCase):
    def test_original_bytes_and_seekable_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "capture.tar.gz"
            values = {"stores/a.body": b'{"a":1}', "catalog/a.body": b'{"a":2}'}
            with RawWriter(p) as writer:
                for name, data in values.items():
                    writer.add(name, data, hashlib.sha256(data).hexdigest())
                self.assertEqual(writer.raw_bytes, sum(map(len, values.values())))
            self.assertEqual(list(Path(tmp).iterdir()), [p])
            with seekable_archive(p) as archive:
                self.assertEqual({m.name for m in archive.getmembers()}, set(values))
                for name in reversed(list(values)):
                    self.assertEqual(archive.extractfile(name).read(), values[name])

    def test_duplicate_and_corruption_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "capture.tar.gz"
            with self.assertRaises(ValueError):
                with RawWriter(p) as writer:
                    writer.add("stores/a.body", b"a", "0" * 64)
            self.assertFalse(p.exists())
            with self.assertRaises(ValueError):
                with RawWriter(p) as writer:
                    writer.add("stores/a.body", b"a", hashlib.sha256(b"a").hexdigest())
                    writer.add("stores/a.body", b"a", hashlib.sha256(b"a").hexdigest())
            self.assertFalse(p.exists())

    def test_spool_exact_sequence_and_replacement(self):
        with DocumentSpool() as docs:
            docs.append({"store_uuid": "a", "menu": [1, 2], "price": "5.25"})
            docs.append({"store_uuid": "b", "menu": [3]})
            docs[0] = {"store_uuid": "a", "menu": [9]}
            self.assertEqual(docs.store_ids, ["a", "b"])
            self.assertEqual(list(docs), [{"store_uuid": "a", "menu": [9]},
                                          {"store_uuid": "b", "menu": [3]}])
            self.assertEqual(len(docs), 2)

if __name__ == "__main__":
    unittest.main()
