"""Chunked time-range search execution for Taegis QL queries.

Wraps the normal search flow so that, when a query uses the | earliest
or | latest Jinja2 filters, the query is automatically retried across
progressively smaller time-range windows if the full range fails.
"""

import logging
import re
import warnings
from datetime import timedelta
from typing import Any, Callable, List, Optional, Tuple

from gql.transport.exceptions import TransportQueryError

log = logging.getLogger(__name__)

# Regex patterns to locate EARLIEST= / LATEST= in a rendered query
EARLIEST_PATTERN = re.compile(r"EARLIEST\s*=\s*[^\s]+", re.IGNORECASE)
LATEST_PATTERN = re.compile(r"LATEST\s*=\s*[^\s]+", re.IGNORECASE)


# chunked search helpers
def _replace_time_range(
    query: str, earliest: str, latest: Optional[str]
) -> str:
    """Replace earliest=… and latest=… in *query* with new values.

    Parameters
    ----------
    query : str
        The rendered Taegis QL query containing an earliest=… clause
        and optionally a latest=… clause.
    earliest : str
        New earliest clause, e.g. 'earliest=-15d'.
    latest : Optional[str]
        New latest clause, e.g. 'latest=-7d'.
        If None, any existing latest=… in the query is **preserved**
        unchanged (the window extends to whatever the base query specifies,
        or "now" if no latest clause exists).

    Returns
    -------
    str
        The modified query string.
    """
    # Replace EARLIEST
    new_query = EARLIEST_PATTERN.sub(earliest, query, count=1)

    if latest is not None:
        if LATEST_PATTERN.search(new_query):
            # Replace existing LATEST
            new_query = LATEST_PATTERN.sub(latest, new_query, count=1)
        else:
            # Append LATEST on a new line after EARLIEST
            new_query = new_query.replace(earliest, f"{earliest}\n{latest}", 1)
    # else: latest is None → preserve any existing LATEST in the query as-is.
    # This respects user-written LATEST clauses (e.g. "LATEST = -7d") and
    # ensures chunk windows that extend "to now" don't accidentally strip
    # a LATEST that the user explicitly set in their query.

    return new_query


# Core chunked execution algorigthm
"""
    Algorithm
    ---------
    1. Try Tier 0 (full range). On success, return immediately.
    2. On failure -> keep any successful results, identify failed windows.
    3. Subdivide only the failed windows using the next tier's window size.
    4. Retry those subdivisions - successes are appended to the running total.
    5. Repeat until all tiers are exhausted or all windows succeed.
    6. Return aggregate_fn(all_results) or None.
"""

def execute_chunked_search(
    base_query: str,
    chunk_tiers: List[List[Tuple[str, Optional[str]]]],
    search_fn: Callable[[str], Any],
    aggregate_fn: Callable[[List[Any]], Any],
) -> Any:
    """Execute a search with progressive time-range chunking.

    Parameters
    ----------
    base_query : str
        The rendered Taegis QL query with earliest=… (and optionally
        latest=…).
    chunk_tiers : List[List[Tuple[str, Optional[str]]]]
        Tiers of (earliest_clause, latest_clause) pairs produced by
        :func:`~taegis_magic.core.time_range.generate_chunk_windows`.
    search_fn : Callable[[str], Any]
        Executes a single query string and returns a normalizer result.
        Must raise on failure (timeout, transport error, etc.).
    aggregate_fn : Callable[[List[Any]], Any]
        Combines multiple normalizer results into one.

    Returns
    -------
    Any
        Aggregated normalizer result, or None if every window failed.

    """
    all_results: List[Any] = []

    # Windows that still need to be retried.  Start with Tier 0.
    pending_windows: List[Tuple[str, Optional[str]]] = list(chunk_tiers[0])

    for tier_idx in range(len(chunk_tiers)):
        failed_windows: List[Tuple[str, Optional[str]]] = []
        log.info(
            "Chunked search: starting tier %d with %d window(s)",
            tier_idx,
            len(pending_windows),
        )

        for win_idx, (earliest, latest) in enumerate(pending_windows):
            modified_query = _replace_time_range(base_query, earliest, latest)
            latest_display = latest or "now"
            try:
                log.info(
                    "Chunked search tier %d window %d/%d: %s to %s",
                    tier_idx,
                    win_idx + 1,
                    len(pending_windows),
                    earliest,
                    latest_display,
                )
                result = search_fn(modified_query)
                all_results.append(result)
            except Exception as exc:
                msg_suffix = (
                    "Trying smaller windows in next tier..."
                    if tier_idx < len(chunk_tiers) - 1
                    else "No more tiers to retry."
                )
                warnings.warn(
                    f"Chunk failed ({earliest} to {latest_display}): {exc}. "
                    f"{msg_suffix}",
                    stacklevel=2,
                )
                failed_windows.append((earliest, latest))

        if not failed_windows:
            break

        if tier_idx == len(chunk_tiers) - 1:
            # Last tier. Exchausted all retries.
            warnings.warn(
                f"{len(failed_windows)} chunk(s) failed after all retry tiers "
                f"exhausted. Partial results returned.",
                stacklevel=2,
            )
            break

        # More tiers available.  Subdivide only the failed windows
        # using the next tier's granularity.
        next_tier_windows = chunk_tiers[tier_idx + 1]
        retry_windows = _subdivide_failed_windows(
            failed_windows, next_tier_windows,
        )
        warnings.warn(
            f"Tier {tier_idx} had {len(failed_windows)} failure(s) — "
            f"retrying those ranges as {len(retry_windows)} smaller "
            f"window(s) from tier {tier_idx + 1}...",
            stacklevel=2,
        )
        pending_windows = retry_windows

    return aggregate_fn(all_results) if all_results else None


def _subdivide_failed_windows(
    failed_windows: List[Tuple[str, Optional[str]]],
    next_tier_windows: List[Tuple[str, Optional[str]]],
) -> List[Tuple[str, Optional[str]]]:
    """Find the next-tier windows that cover the failed time ranges.

    For each failed (earliest, latest) pair, select every window from
    next_tier_windows whose range overlaps.  Two ranges overlap when
    neither ends before the other starts.

    The comparison is done on the absolute timedelta of each clause.
    latest=None is treated as offset 0 (now).

    Parameters
    ----------
    failed_windows : List[Tuple[str, Optional[str]]]
        Windows that failed at the current tier.
    next_tier_windows : List[Tuple[str, Optional[str]]]
        Complete set of windows from the next finer tier.

    Returns
    -------
    List[Tuple[str, Optional[str]]]
        De-duplicated list of next-tier windows that cover the failed ranges,
        preserving the order from *next_tier_windows*.
    """
    from taegis_magic.core.time_range import parse_relative_timestamp

    def _offset(clause: Optional[str]) -> timedelta:
        """Extract timedelta from 'earliest=-30d' or 'latest=-7d'."""
        if clause is None:
            return timedelta(0)  # "now"
        # Strip 'earliest=' or 'latest=' prefix
        value = clause.split("=", 1)[1]
        return parse_relative_timestamp(value)

    # Collect indices of next-tier windows that overlap any failed window.
    selected_indices: set = set()

    for f_earliest, f_latest in failed_windows:
        # Failed range: from f_earliest (large offset) to f_latest (small offset / now)
        f_start = _offset(f_earliest)   # e.g. timedelta(days=30)
        f_end = _offset(f_latest)       # e.g. timedelta(days=15) or 0

        for idx, (n_earliest, n_latest) in enumerate(next_tier_windows):
            n_start = _offset(n_earliest)   # e.g. timedelta(days=30)
            n_end = _offset(n_latest)       # e.g. timedelta(days=23)

            # Ranges overlap if neither is entirely before/after the other.
            # "Before" means a *larger* offset (further in the past).
            # Range A: [f_start .. f_end]  (f_start >= f_end)
            # Range B: [n_start .. n_end]  (n_start >= n_end)
            if n_end >= f_start or f_end >= n_start:
                continue  # no overlap
            selected_indices.add(idx)

    # Return in the original order from next_tier_windows.
    return [next_tier_windows[i] for i in sorted(selected_indices)]


# Aggregation helper
def aggregate_normalizer_results(results: List[Any]) -> Optional[Any]:
    """Merge multiple normalizer results into one.

    Per-chunk display metadata (query ID, result count, share link) is
    snapshotted before merging so that the display template can show
    accurate per-chunk rows even though raw_results on the combined
    normalizer is mutated in place.

    Parameters
    ----------
    results : List[Any]
        Normalizer results from individual chunk searches.

    Returns
    -------
    Optional[Any]
        The merged normalizer, or None if *results* is empty.
    """
    if not results:
        return None

    # Snapshot per-chunk display properties *before* mutating raw_results.
    chunk_snapshots: List[_ChunkSnapshot] = []
    if len(results) > 1:
        for r in results:
            snapshot = _ChunkSnapshot(
                region=getattr(r, "region", ""),
                tenant_id=getattr(r, "tenant_id", ""),
                service=getattr(r, "service", ""),
                status=getattr(r, "status", ""),
                results_returned=getattr(r, "results_returned", -1),
                query_identifier=getattr(r, "query_identifier", None),
            )
            # Trigger shareable_url creation while raw_results is intact.
            try:
                snapshot.shareable_url = r.shareable_url
            except Exception:
                snapshot.shareable_url = "Unable to create shareable link"
            chunk_snapshots.append(snapshot)

    # Now merge raw_results into the first normalizer.
    combined = results[0]
    for r in results[1:]:
        if hasattr(r, "raw_results") and hasattr(combined, "raw_results"):
            combined.raw_results.extend(r.raw_results)
        else:
            log.warning(
                "Cannot merge result of type %s — missing raw_results attribute.",
                type(r).__name__,
            )

    # Attach frozen snapshots for the display template.
    if chunk_snapshots:
        combined._chunk_results = chunk_snapshots

    return combined


class _ChunkSnapshot:
    """Frozen snapshot of a single chunk's display properties.

    Created *before* raw_results are merged so that each chunk's
    results_returned, query_identifier, and shareable_url
    reflect only that chunk's data.
    """

    __slots__ = (
        "region",
        "tenant_id",
        "service",
        "status",
        "results_returned",
        "query_identifier",
        "shareable_url",
    )

    def __init__(
        self,
        region: str,
        tenant_id: str,
        service: str,
        status: str,
        results_returned: int,
        query_identifier: Optional[str],
        shareable_url: str = "",
    ):
        self.region = region
        self.tenant_id = tenant_id
        self.service = service
        self.status = status
        self.results_returned = results_returned
        self.query_identifier = query_identifier
        self.shareable_url = shareable_url


# High-level wrapper for magics.py
def execute_chunked_search_from_magic(
    cell: str,
    command_args: list,
    chunking_schedule: List[List[Tuple[str, Optional[str]]]],
    app_fn: Callable,
) -> Optional[Any]:
    """Execute a chunked search from the magics command flow.

    For each time window in the chunking schedule:

    1. Modifies *cell* to replace earliest= / latest= with the
       window's values.
    2. Rebuilds *command_args* with the modified cell.
    3. Calls app_fn(modified_args, …) exactly as the existing flow does.
    4. Collects successful results.
    5. Warns on failures but continues.
    6. Merges results via aggregate_normalizer_results.

    Parameters
    ----------
    cell : str
        The rendered Taegis QL query (with earliest=…).
    command_args : list
        The CLI argument list (may contain --cell, which will be replaced).
    chunking_schedule : List[List[Tuple[str, Optional[str]]]]
        Chunking tiers from :func:`~taegis_magic.core.filters.get_chunking_schedule`.
    app_fn : Callable
        The Typer app callable (taegis_magic.cli.app).

    Returns
    -------
    Optional[Any]
        Aggregated normalizer result, or None if all windows failed.
    """

    def _search_fn(modified_cell: str) -> Any:
        """Run a single search via the CLI app."""
        # Build fresh args list with the modified cell
        args = list(command_args)

        # Remove any existing --cell argument pair
        clean_args: list = []
        skip_next = False
        for i, arg in enumerate(args):
            if skip_next:
                skip_next = False
                continue
            if arg == "--cell":
                skip_next = True  
                continue
            clean_args.append(arg)

        clean_args.extend(["--cell", modified_cell])

        result = app_fn(clean_args, prog_name="taegis", standalone_mode=False)
        if result is None:
            raise RuntimeError("Search returned None (possible empty result or error)")
        return result

    return execute_chunked_search(
        base_query=cell,
        chunk_tiers=chunking_schedule,
        search_fn=_search_fn,
        aggregate_fn=aggregate_normalizer_results,
    )
