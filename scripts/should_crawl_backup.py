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
    url = os.environ["TURSO_DATABASE_URL"].rstrip("/")
    token = os.environ["TURSO_READONLY_TOKEN"]
    query = json.dumps({"requests": [
        {"type": "execute", "stmt": {"sql": "SELECT value FROM metadata WHERE key='latest_processed_at'"}},
        {"type": "close"},
    ]}).encode()
    request = Request(url + "/v2/pipeline", data=query,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST")
    with urlopen(request, timeout=22) as response:
        body = json.load(response)
    result = body["results"][0]
    if result["type"] != "ok":
        raise RuntimeError(f"Turso freshness query failed: {result.get('error')}")
    rows = result["response"]["result"]["rows"]
    if not rows:
        return ""
    return json.loads(rows[0][0]["value"])


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
        f.write(f"needed={'true' if needed else 'false'}\\n".replace('\\n', '\n'))


if __name__ == "__main__":
    main()
