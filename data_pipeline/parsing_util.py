from datetime import datetime
from typing import Optional


def float_or_none(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None

    try:
        return float(s)
    except ValueError:
        return None


def int_or_none(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None

    try:
        return int(s)
    except ValueError:
        return None


def parse_arso_datetime(s: str) -> datetime:
    """Parse datetime in format used in ARSO XML, for example "10.11.2023 16:40 CET"."""

    # Replace recognized timezone names with offsets.
    recognized_timezones = {
        "CET": "+0100",
        "CEST": "+0200",
        "UTC": "+0000"
    }
    for tz, offset in recognized_timezones.items():
        if s.endswith(tz):
            s = s[:-len(tz)] + offset
            break

    return datetime.strptime(s, "%d.%m.%Y %H:%M %z")
