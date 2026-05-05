"""Custom Jinja2 filters for Taegis QL earliest and latest time-range clauses.

Provides earliest_filter and latest_filter for use with
jinja2.Environment.filters.  Each filter accepts a list of relative
timestamp strings and returns a rendered earliest= / latest= clause.
"""

import logging
from typing import Dict, List, Optional, Tuple

from taegis_magic.core.time_range import (
    generate_chunk_windows,
    sort_timestamps_descending,
)

log = logging.getLogger(__name__)

# TaegisTimeRange — str subclass that carries chunk metadata
class TaegisTimeRange(str):
    """String subclass that also carries chunk metadata.

    When cast to str (which Jinja2 does), it produces a valid
    earliest=… or latest=… clause.

    Attributes
    ----------
    chunks : List[List[Tuple[str, Optional[str]]]]
        The full chunking schedule (list of tiers).
    timestamps : List[str]
        The original sorted timestamps.
    direction : str
        'earliest' or 'latest'.
    """

    chunks: List[List[Tuple[str, Optional[str]]]]
    timestamps: List[str]
    direction: str

    def __new__(
        cls,
        value: str,
        chunks: Optional[List[List[Tuple[str, Optional[str]]]]] = None,
        timestamps: Optional[List[str]] = None,
        direction: str = "earliest",
    ):
        instance = super().__new__(cls, value)
        instance.chunks = chunks or []
        instance.timestamps = timestamps or []
        instance.direction = direction
        return instance


# Module-level chunking registry
_CHUNKING_REGISTRY: Dict[str, List[List[Tuple[str, Optional[str]]]]] = {}


def get_chunking_schedule(
    rendered_cell: str,
) -> Optional[List[List[Tuple[str, Optional[str]]]]]:
    """Look up chunking metadata for a rendered cell string.

    Checks whether any registered earliest=… clause key appears in
    *rendered_cell* and returns the corresponding chunking schedule.

    Parameters
    ----------
    rendered_cell : str
        The fully rendered Taegis QL query string.

    Returns
    -------
    Optional[List[List[Tuple[str, Optional[str]]]]]
        The chunking schedule if found, else None.
    """
    if not rendered_cell:
        return None

    for key, schedule in _CHUNKING_REGISTRY.items():
        if key in rendered_cell:
            return schedule

    return None


def clear_chunking_registry() -> None:
    """Clear the chunking registry."""
    _CHUNKING_REGISTRY.clear()

# Jinja2 filter functions

def earliest_filter(timestamps: List[str]) -> TaegisTimeRange:
    """Jinja2 filter: {{ [] | earliest }}.

    1. Sorts *timestamps* descending by duration.
    2. Generates the chunking schedule via :func:`generate_chunk_windows`.
    3. Returns a :class:`TaegisTimeRange` whose string value is
       earliest=<largest> and whose .chunks carries the full schedule.
    4. Registers the schedule in :data:`_CHUNKING_REGISTRY`.

    Parameters
    ----------
    timestamps : List[str]
        Relative timestamp strings, e.g. ['-30d', '-15d', '-7d', '-1d'].

    Returns
    -------
    TaegisTimeRange
        String rendering to earliest=-30d (using the largest timestamp).
    """
    sorted_ts = sort_timestamps_descending(timestamps)
    chunks = generate_chunk_windows(sorted_ts)

    rendered_value = f"earliest={sorted_ts[0]}"

    result = TaegisTimeRange(
        value=rendered_value,
        chunks=chunks,
        timestamps=sorted_ts,
        direction="earliest",
    )

    # Register in module-level registry so execution layer can find it
    _CHUNKING_REGISTRY[rendered_value] = chunks
    log.debug(
        "Registered earliest chunking schedule: %s (%d tiers)",
        rendered_value,
        len(chunks),
    )

    return result


def latest_filter(timestamps: Optional[List[str]] = None) -> TaegisTimeRange:
    """Jinja2 filter: {{ [] | latest }}.

    - If *timestamps* is None or empty, returns TaegisTimeRange('')
      (defaults to now) with empty .chunks.
    - Otherwise returns TaegisTimeRange('latest=<smallest>') with
      .chunks populated.

    Parameters
    ----------
    timestamps : Optional[List[str]]
        Relative timestamp strings, or None.

    Returns
    -------
    TaegisTimeRange
        String rendering to latest=-7d (the smallest / most recent
        timestamp) or '' when defaulting to now.
    """
    if not timestamps:
        return TaegisTimeRange(
            value="",
            chunks=[],
            timestamps=[],
            direction="latest",
        )

    sorted_ts = sort_timestamps_descending(timestamps)
    chunks = generate_chunk_windows(sorted_ts)

    # The smallest (most recent) timestamp is the last after descending sort
    rendered_value = f"latest={sorted_ts[-1]}"

    result = TaegisTimeRange(
        value=rendered_value,
        chunks=chunks,
        timestamps=sorted_ts,
        direction="latest",
    )

    _CHUNKING_REGISTRY[rendered_value] = chunks
    log.debug(
        "Registered latest chunking schedule: %s (%d tiers)",
        rendered_value,
        len(chunks),
    )

    return result
