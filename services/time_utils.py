from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config import APP_TIMEZONE

_TZ = ZoneInfo(APP_TIMEZONE)


def now_local() -> datetime:
    return datetime.now(_TZ)


def now_local_naive() -> datetime:
    # Для совместимости с текущими полями БД, где хранятся naive timestamps.
    return now_local().replace(tzinfo=None)


def normalize_time_str(value: str) -> str:
    s = (value or "").strip()
    if len(s) >= 8 and s.count(":") >= 2:
        return s[:5]
    return s


def shift_start_end_local_naive(shift_date: str, start_time: str, end_time: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(f"{shift_date} {normalize_time_str(start_time)}", "%Y-%m-%d %H:%M")
    end = datetime.strptime(f"{shift_date} {normalize_time_str(end_time)}", "%Y-%m-%d %H:%M")
    if end <= start:
        end += timedelta(days=1)
    return start, end
