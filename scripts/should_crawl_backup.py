"""Recovery guard for GitHub's delayed/cancelled scheduled FoodRadar crawls.

At 07:30/19:30 Asia/Taipei, run a backup crawl ONLY if Turso has no
successful dataset from the normal 06:30/18:30 collection window.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

TW = timezone(timedelta(hours=8))


def most_recent_window(now: datetime) -> datetime:
    local = now.astimezone(TW)
    evening = local.replace(hour=18, minute=30, second=0, microsecond=0)
    morning = local.replace(hour=6, minute=30, second=0, microsecond=0)
    if local >= evening:
        return evening
    if local >= morning:
        return morning
    return (morning - timedelta(days=1)).replace(hour=18, minute=30)


def needs_recovery(latest: str | None, now: datetime) -> bool:
    if not latest:
        return True
    try:
        observed = datetime.fromisoformat(str(latest).replace('Z', '+00:00'))
        if observed.tzinfo is None:
            return True
        return observed.astimezone(TW) < most_recent_window(now)
    except (TypeError, ValueError, OverflowError):
        return True


def current_published_time() -> str:
    """Query the same verified Turso HTTP pipeline client as the publisher.

    GitHub secret FOODRADAR_TURSO_DATABASE_URL normally starts with libsql://,
    which urllib cannot open directly; Remote normalizes it to HTTPS.
    """
    from publish_turso_delta import Remote
    url = os.environ["TURSO_DATABASE_URL"]
    token = os.environ["TURSO_READONLY_TOKEN"]
    remote = Remote(url, token)
    try:
        rows = remote.query(
            "SELECT value FROM metadata WHERE key='latest_processed_at'")
        if not rows:
            return ""
        if len(rows) != 1:
            raise ValueError("Turso metadata latest_processed_at is ambiguous")
        return json.loads(rows[0]["value"])
    finally:
        remote.close()


def main() -> None:
    now = datetime.now(TW)
    try:
        latest = current_published_time()
        needed = needs_recovery(latest, now)
    except Exception as exc:
        # Missing/corrupt remote freshness is NOT evidence of a successful run.
        print("FRESHNESS_CHECK_ERROR", type(exc).__name__, str(exc)[:200], file=sys.stderr)
        latest, needed = "unknown", True
    print(f"FOODRADAR_BACKUP_CRAWL needed={needed} latest={latest} "
          f"window={most_recent_window(now).isoformat()}", file=sys.stderr)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
        f.write('needed=' + ('true' if needed else 'false') + chr(10))


if __name__ == "__main__":
    main()
