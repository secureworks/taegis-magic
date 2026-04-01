"""Taegis QL relative timestamp parsing and time-range window generation.
Time range utility module for converting relative timestamps into chunking schedules for Taegis QL queries.
"""

import math
import re
from datetime import timedelta
from typing import List, Optional, Tuple

# Direct mapping for simple units
UNIT_MAP = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}

# Pattern to parse relative timestamps like '-30d', '-3mo', '-1y', '7d', etc.
_RELATIVE_TS_PATTERN = re.compile(
    r"^[+-]?(\d+)(s|m|h|d|w|mo|y)$", re.IGNORECASE
)


def parse_relative_timestamp(ts: str) -> timedelta:
    """Parse a Taegis QL relative timestamp into a timedelta.

    Supports: s, m, h, d, w, mo, y. According to : https://docs.taegis.secureworks.com/search/querylanguage/advanced_search/#time-ranges
    Parameters
    ----------
    ts : str
        Relative timestamp string, e.g. '-30d', '-3mo', '1y'.

    Returns
    -------
    timedelta
        Positive timedelta representing the duration.

    Raises
    ------
    ValueError
        If the timestamp string cannot be parsed.
    """
    ts = ts.strip()
    match = _RELATIVE_TS_PATTERN.match(ts)
    if not match:
        raise ValueError(
            f"Cannot parse relative timestamp: {ts!r}. "
            f"Expected format like '-30d', '-3mo', '-1y'."
        )

    value = int(match.group(1))
    unit = match.group(2).lower()

    if unit in UNIT_MAP:
        return timedelta(**{UNIT_MAP[unit]: value})
    elif unit == "mo":
        return timedelta(days=value * 30)
    elif unit == "y":
        return timedelta(days=value * 365)
    else:
        raise ValueError(f"Unknown unit: {unit!r} in timestamp {ts!r}.")


def sort_timestamps_descending(timestamps: List[str]) -> List[str]:
    """Sort timestamps by absolute duration, largest first.

    Parameters
    ----------
    timestamps : List[str]
        List of relative timestamp strings, e.g.['-7d', '-30d', '-1d', '-15d']

    Returns
    -------
    List[str]
        Sorted list with the largest duration first.
    """
    return sorted(timestamps, key=lambda ts: parse_relative_timestamp(ts), reverse=True)


def generate_chunk_windows(
    timestamps: List[str],
) -> List[List[Tuple[str, Optional[str]]]]:
    """Given a list of relative timestamps, produce chunking tiers.

    The timestamps are first sorted descending by duration. 
    - TIEr 0 is the full range window from the largest timestamp to NOW.
    - Tier 1 is a fallback tier of tier 0's range, chunked into windows of the second-largest timestamp.
    - Tier N is a fallback tier of tier 0's range, chunked into windows of the N-th largest timestamp.

    Parameters
    ----------
    timestamps : List[str]
        List of relative timestamp strings, e.g. ['-30d', '-15d', '-7d', '-1d']. Sorted descending by duration for chunking best effort.

    Returns
    -------
    List[List[Tuple[str, Optional[str]]]]
        A list of tiers.  Each tier is a list of (earliest_clause, latest_clause) tuples.

    Raises
    ------
    ValueError
        If fewer than 1 timestamp is provided.
    """
    if not timestamps:
        raise ValueError("At least one timestamp is required.")

    sorted_ts = sort_timestamps_descending(timestamps)

    # Parse all durations for arithmetic
    durations = [parse_relative_timestamp(ts) for ts in sorted_ts]

    full_range = durations[0]
    full_range_ts = sorted_ts[0]

    # Tier 0: single full-range window
    tiers: List[List[Tuple[str, Optional[str]]]] = [
        [(f"earliest={full_range_ts}", None)]
    ]

    # Tier 1..N: progressively smaller windows
    for tier_idx in range(1, len(sorted_ts)):
        window_size = durations[tier_idx]
        window_ts = sorted_ts[tier_idx]

        # Calculate how many windows for coverage
        num_windows = math.ceil(full_range / window_size)

        tier_windows: List[Tuple[str, Optional[str]]] = []
        for i in range(num_windows):

            earliest_offset = full_range - (window_size * i)
            latest_offset = full_range - (window_size * (i + 1))

            # Convert offsets back to relative timestamp strings
            earliest_days = earliest_offset.total_seconds()
            latest_days = latest_offset.total_seconds()

            earliest_clause = f"earliest=-{_offset_to_relative_str(earliest_offset)}"

            if latest_days <= 0:
                # This is the most recent window — LATEST defaults to now
                latest_clause = None
            else:
                latest_clause = f"latest=-{_offset_to_relative_str(latest_offset)}"

            tier_windows.append((earliest_clause, latest_clause))

        tiers.append(tier_windows)

    return tiers


def _offset_to_relative_str(offset: timedelta) -> str:
    """Convert a timedelta offset back to a compact relative timestamp string.

    Uses the largest exact unit that divides evenly, falling back to seconds.

    Parameters
    ----------
    offset : timedelta
        The time offset (positive).

    Returns
    -------
    str
        Compact string like '30d', '2w', '3600s', etc.
    """
    total_seconds = int(offset.total_seconds())

    if total_seconds <= 0:
        return "0s"

    # Try largest units first
    if total_seconds % (7 * 86400) == 0:
        weeks = total_seconds // (7 * 86400)
        return f"{weeks}w"
    if total_seconds % 86400 == 0:
        days = total_seconds // 86400
        return f"{days}d"
    if total_seconds % 3600 == 0:
        hours = total_seconds // 3600
        return f"{hours}h"
    if total_seconds % 60 == 0:
        minutes = total_seconds // 60
        return f"{minutes}m"

    return f"{total_seconds}s"
