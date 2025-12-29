"""
Created: Nov 4, 2025
By: Adam M.
Generalised: 2025-12-29
Objective: Tracking of last execution times and datetime utilities.
"""
import os
import yaml
import tempfile
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional
import re

def _parse_datetime_to_utc(value) -> datetime:
    """Parses a datetime value to UTC timezone-aware datetime."""
    if value is None:
        raise ValueError("No date value")
    
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
        
    s = str(value).strip()

    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError):
        pass

    # ISO pattern with optional microseconds and timezone
    iso_pattern = r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?([+-]\d{2}:\d{2}|Z)$"
    match = re.match(iso_pattern, s)
    if match:
        year, month, day, hour, minute, second, microsecond_str, tz_str = match.groups()
        
        microsecond = int(microsecond_str.ljust(6, '0')[:6]) if microsecond_str else 0

        if tz_str == 'Z' or tz_str == '+00:00':
            tz = timezone.utc
        elif tz_str:
            sign = 1 if tz_str[0] == '+' else -1
            tz_hours, tz_mins = map(int, tz_str[1:].split(':'))
            tz = timezone(timedelta(hours=sign * tz_hours, minutes=sign * tz_mins))
        else:
            tz = timezone.utc
            
        dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), microsecond, tz)
        return dt.astimezone(timezone.utc)

    fmts = [
        "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    raise ValueError(f"Unrecognised date format: {value}")


def read_last_run(path: str = "last_run.yaml", key: str = "Fabric") -> datetime:
    """Reads the last run timestamp for the given key."""
    if not os.path.exists(path):
        return datetime(1940, 1, 1, tzinfo=timezone.utc)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        v = data.get(key)
        if not v:
            return datetime(1940, 1, 1, tzinfo=timezone.utc)
        return _parse_datetime_to_utc(v)
    except Exception as e:
        print(f"WARNING: Failed to read last run date from {path}: {e}. Using default date 1940-01-01")
        traceback.print_exc()
        return datetime(1940, 1, 1, tzinfo=timezone.utc)


def write_last_run(path: str, key: str, dt: Optional[datetime] = None) -> None:
    """Updates the last run timestamp for the given key."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception:
            data = {}
            
    data[key] = dt.astimezone(timezone.utc).isoformat()
    dirn = os.path.dirname(path) or "."
    fd, tmppath = tempfile.mkstemp(dir=dirn, prefix=".last_run_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f)
        os.replace(tmppath, path)
    finally:
        if os.path.exists(tmppath):
            try:
                os.remove(tmppath)
            except Exception:
                pass
