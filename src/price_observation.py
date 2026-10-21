"""Evaluate whether a captured Uber Eats price was observed during store business hours.

The API's isOpen=True is insufficient: it can be true at 01:00 for a store
whose workingHoursTagline says "Opens 9:00 AM". Do not confuse listing status
with whether a *price observation* can be used as a price/discount baseline.
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
    if doc.get('isOpen') is not True:
        return False
    text = str(doc.get('workingHoursTagline') or '').strip().lower()
    if text.startswith(('opens ', 'opens at ', 'closed', 'temporarily closed', 'currently closed')):
        return False
    hours = _schedule_open(doc, observed_at(doc, batch_id))
    if hours is False:
        return False
    # No schedule supplied in an older source: use the API's affirmative flag,
    # but reject an explicit "opens" / "closed" status above.
    return True
