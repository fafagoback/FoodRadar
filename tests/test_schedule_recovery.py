import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from should_crawl_backup import needs_recovery, most_recent_window

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

    def test_utc_metadata_is_converted_to_taiwan_time(self):
        now = datetime(2026,10,28,7,30,tzinfo=TW)
        self.assertFalse(needs_recovery('2026-10-27T22:31:00Z', now))


if __name__ == '__main__':
    unittest.main()
