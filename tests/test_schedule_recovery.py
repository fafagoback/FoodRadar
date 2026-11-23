import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from should_crawl_backup import needs_recovery, most_recent_window, current_published_time

TW = timezone(timedelta(hours=8))


class BackupScheduleTests(unittest.TestCase):
    def test_morning_0700_does_not_repeat_successful_0630_crawl(self):
        now = datetime(2026,10,28,7,30,tzinfo=TW)
        self.assertEqual(most_recent_window(now), datetime(2026,10,28,6,30,tzinfo=TW))
        self.assertFalse(needs_recovery('2026-10-28T06:32:00+08:00', now))
        self.assertTrue(needs_recovery('2026-10-27T23:58:41+08:00', now))

    def test_evening_1930_checks_evening_not_morning(self):
        now = datetime(2026,10,28,19,30,tzinfo=TW)
        self.assertEqual(most_recent_window(now), datetime(2026,10,28,18,30,tzinfo=TW))
        self.assertTrue(needs_recovery('2026-10-28T07:00:00+08:00', now))
        self.assertFalse(needs_recovery('2026-10-28T18:35:00+08:00', now))

    def test_invalid_or_missing_remote_time_triggers_backup(self):
        now = datetime(2026,10,28,19,30,tzinfo=TW)
        for value in ('', None, 'bad', '2026-10-28T19:00:00'):
            self.assertTrue(needs_recovery(value, now))

    def test_published_time_uses_turso_client_that_normalizes_libsql(self):
        import types
        import sys
        instances = []
        class FakeRemote:
            def __init__(self, url, token):
                instances.append(self)
                self.url, self.token, self.closed = url, token, False

            def query(self, sql):
                self.sql = sql
                return [{"value": '"2026-10-28T18:35:00+08:00"'}]

            def close(self):
                self.closed = True

        with patch.dict(sys.modules, {"publish_turso_delta": types.SimpleNamespace(Remote=FakeRemote)}):
            with patch.dict("os.environ", {
                "TURSO_DATABASE_URL": "libsql://foodradar.turso.io",
                "TURSO_READONLY_TOKEN": "read-only",
            }):
                result = current_published_time()
        self.assertEqual(result, "2026-10-28T18:35:00+08:00")
        self.assertEqual(instances[0].url, "libsql://foodradar.turso.io")
        self.assertEqual(instances[0].sql,
                         "SELECT value FROM metadata WHERE key='latest_processed_at'")
        self.assertTrue(instances[0].closed)

    def test_missing_published_metadata_triggers_recovery(self):
        import types
        import sys
        class FakeRemote:
            def __init__(self, *_): pass
            def query(self, *_): return []
            def close(self): pass
        with patch.dict(sys.modules, {"publish_turso_delta": types.SimpleNamespace(Remote=FakeRemote)}):
            with patch.dict("os.environ", {
                "TURSO_DATABASE_URL": "libsql://foodradar.turso.io",
                "TURSO_READONLY_TOKEN": "read-only",
            }):
                published = current_published_time()
        self.assertEqual(published, "")
        self.assertTrue(needs_recovery(
            published, datetime(2026,10,28,19,30,tzinfo=TW)))

    def test_utc_metadata_is_converted_to_taiwan_time(self):
        now = datetime(2026,10,28,7,30,tzinfo=TW)
        self.assertFalse(needs_recovery('2026-10-27T22:31:00Z', now))


if __name__ == '__main__':
    unittest.main()
