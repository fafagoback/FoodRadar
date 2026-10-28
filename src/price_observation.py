"""Evaluate whether a captured Uber Eats price was observed during store business hours.

The API's isOpen=True is insufficient: it can be true at 01:00 for a store
whose workingHoursTagline says "Opens 9:00 AM". Do not confuse listing status
with whether a *price observation* can be used as a price/discount baseline.
Replaying archived snapshots repairs historical false discounts.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

TW = timezone(timedelta(hours=8))
DAYS = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')


def observed_at(document: dict, batch_id: str) -> datetime:
    value = document.get('captured_at')
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return parsed.replace(tzinfo=TW).astimezone(TW) if parsed.tzinfo is None else parsed.astimezone(TW)
        except ValueError:
            pass
    return datetime.strptime(batch_id, '%Y%m%d%H%M%S').replace(tzinfo=TW)


def _minutes(value):
    try:
        parts = [int(n) for n in str(value).split(':')]
        if len(parts) not in (2, 3) or parts[0] < 0 or parts[0] > 24 or not 0 <= parts[1] <= 59:
            return None
        if parts[0] == 24 and (parts[1] or (len(parts) == 3 and parts[2])):
            return None
        return parts[0] * 60 + parts[1]
    except (TypeError, ValueError):
        return None


def _day_matches(spec, weekday: int) -> bool:
    days = spec.get('dayOfWeek') or []
    if not isinstance(days, list):
        days = [days]
    return any(str(x).rsplit('/', 1)[-1].lower() == DAYS[weekday] for x in days)


def _schedule_open(doc: dict, when: datetime):
    slots = doc.get('openingHoursSpecification') or []
    if not slots:
        return None
    minute = when.hour * 60 + when.minute
    weekday = when.weekday()
    recognized = False
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        start = _minutes(slot.get('opens'))
        end = _minutes(slot.get('closes'))
        if start is None or end is None:
            continue
        recognized = True
        if start < end:
            if _day_matches(slot, weekday) and start <= minute < end:
                return True
        elif start > end:
            # Day refers to the day on which the overnight shift begins.
            if (_day_matches(slot, weekday) and minute >= start) or (
                _day_matches(slot, (weekday - 1) % 7) and minute < end
            ):
                return True
        elif _day_matches(slot, weekday):
            return True  # e.g. 00:00-00:00 for a 24-hour store
    return False if recognized else None


def price_observation_valid(doc: dict, batch_id: str) -> bool:
    # Never infer that false/missing isOpen represents a verified payable price.
    if doc.get('isOpen') is not True or doc.get('menu_status') == 'inactive_account':
        return False
    # A closure banner supersedes an occasional stale isOpen=True flag.
    if str(doc.get('closedMessage') or '').strip():
        return False
    text = str(doc.get('workingHoursTagline') or '').strip().lower()
    if text.startswith(('opens ', 'opening ', 'closed', 'temporarily closed',
                        'currently closed', '未營業', '尚未營業', '休息中', '已打烊', '暫停營業')):
        return False
    hours = _schedule_open(doc, observed_at(doc, batch_id))
    if hours is False:
        return False
    if hours is True:
        return True
    # Absent or malformed schedule requires a positive OPEN tagline as well
    # as isOpen=True. Unknown observations remain only in HF Raw.
    return text.startswith(('open ', 'currently open', '24 hours', '24 小時',
                            '營業中', '營業至', '營業到'))


def menu_catalog_valid(doc: dict, batch_id: str) -> bool:
    """Was a real, priced menu captured? Delivery/closing hours are independent.

    The Uber getStoreV1 response can contain usable menu quotations even when
    delivery is unavailable or the restaurant has closed for the night.
    These quotes are valid for historical MENU comparison, not a guarantee
    that a customer can currently order at the displayed price.
    """
    if doc.get('menu_status') == 'inactive_account':
        return False
    # An explicitly open store with an empty menu still constitutes a real
    # observation (and can establish actual product removal).
    if price_observation_valid(doc, batch_id):
        return True
    sections = (doc.get('hasMenu') or {}).get('hasMenuSection') or []
    for section in sections:
        for item in section.get('hasMenuItem') or []:
            if not str(item.get('name') or '').strip():
                continue
            try:
                price = float((item.get('offers') or {}).get('price') or 0)
                if 0 < price < 1_000_000:
                    return True
            except (TypeError, ValueError, OverflowError):
                continue
    return False
