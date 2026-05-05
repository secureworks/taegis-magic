"""Pandas functions for Proccess Lineage Event Dataframes (process only)"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
from taegis_magic.commands.process_trees import process_children, process_lineage

log = logging.getLogger(__name__)

REQUIRED_KEY_COLS = ["host_id", "process_correlation_id"]
OPTIONAL_KEY_COLS = ["resource_id"]


def _build_key_cols(df):
    """Return the key columns present in the DataFrame."""
    return REQUIRED_KEY_COLS + [c for c in OPTIONAL_KEY_COLS if c in df.columns]


def _row_to_key(row, key_cols):
    """Convert a row to a key tuple, substituting None for NaN values."""
    return tuple(None if pd.isna(row[c]) else row[c] for c in key_cols)


def _resolve_show_progress(show_progress: Optional[bool]) -> bool:
    """Determine whether to display a tqdm progress bar.

    When *show_progress* is ``None`` (auto), the bar is shown only when
    running inside a Jupyter notebook **and** tqdm is importable.
    """
    if show_progress is not None:
        return show_progress

    try:
        from IPython import get_ipython

        if get_ipython() is None:
            return False
    except Exception:
        return False

    try:
        import tqdm  # noqa: F401

        return True
    except ImportError:
        return False


def _fetch_concurrently(
    df, fetch_fn, *, region, tenant_id, max_workers,
    max_failure_rate: Optional[float] = 0.05,
    show_progress: Optional[bool] = None,
):
    """Fetch results for unique key tuples concurrently, returning a results map.

    Deduplicates API calls so identical (host_id, process_correlation_id[, resource_id])
    tuples only trigger a single network request.  resource_id is optional and
    may be absent from the DataFrame or contain NaN values.

    Parameters
    ----------
    max_failure_rate : float
        Maximum fraction of keys allowed to fail (0.0–1.0).  If the actual
        failure rate exceeds this threshold a ``RuntimeError`` is raised.
        Defaults to 0.05 (5 %).
    show_progress : bool, optional
        Display a tqdm progress bar.  When ``None`` (the default), a bar is
        shown automatically if running inside a Jupyter notebook and tqdm is
        available.
    """
    key_cols = _build_key_cols(df)

    unique_keys = (
        df[key_cols]
        .drop_duplicates()
        .dropna(subset=REQUIRED_KEY_COLS)
        .itertuples(index=False, name=None)
    )
    # Normalise NaN → None so tuples are hashable and consistent.
    unique_keys = [
        tuple(None if pd.isna(v) else v for v in key) for key in unique_keys
    ]
    # Deduplicate after NaN normalisation.
    unique_keys = list(dict.fromkeys(unique_keys))

    total_keys = len(unique_keys)
    log.info("Starting concurrent fetch for %d unique key(s)", total_keys)

    use_progress = _resolve_show_progress(show_progress)
    _tqdm = None
    if use_progress:
        from tqdm.auto import tqdm as _tqdm

    col_to_idx = {c: i for i, c in enumerate(key_cols)}

    results_map = {}
    failed_keys = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_fn,
                region=region,
                tenant_id=tenant_id,
                host_id=key[col_to_idx["host_id"]],
                process_correlation_id=key[col_to_idx["process_correlation_id"]],
                resource_id=key[col_to_idx["resource_id"]]
                if "resource_id" in col_to_idx
                else None,
            ): key
            for key in unique_keys
        }

        completed_iter = as_completed(futures)
        if use_progress and _tqdm is not None:
            completed_iter = _tqdm(
                completed_iter, total=total_keys, desc="Fetching process trees"
            )

        for future in completed_iter:
            key = futures[future]
            try:
                result = future.result()
                if hasattr(result, "results") and result.results is not None:
                    results_map[key] = result.results
                else:
                    results_map[key] = []
            except Exception:
                log.warning("Failed to fetch for key %s", key, exc_info=True)
                results_map[key] = []
                failed_keys.append(key)

    succeeded = total_keys - len(failed_keys)
    failure_rate = len(failed_keys) / total_keys if total_keys > 0 else 0.0
    log.info(
        "Concurrent fetch complete: %d/%d succeeded (%.1f%% success), %d failed",
        succeeded,
        total_keys,
        (1 - failure_rate) * 100,
        len(failed_keys),
    )

    if max_failure_rate is not None and failure_rate > max_failure_rate:
        raise RuntimeError(
            f"Fetch failure rate {failure_rate:.1%} exceeds maximum allowed "
            f"rate of {max_failure_rate:.1%} ({len(failed_keys)}/{total_keys} keys failed)"
        )

    return results_map


def _map_results(row, results_map, key_cols):
    """Map a row to its pre-fetched results by key tuple."""
    if any(pd.isna(row.get(c)) for c in REQUIRED_KEY_COLS):
        return []
    key = _row_to_key(row, key_cols)
    return results_map.get(key, [])


def lookup_lineage(
    df: pd.DataFrame,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
    max_workers: Optional[int] = 10,
    max_failure_rate: Optional[float] = 0.05,
    show_progress: Optional[bool] = None,
) -> pd.DataFrame:
    """
    For each row in the DataFrame, fetch process lineage using process_correlation_id, host_id, tenant_id, and resource_id.
    Adds a new column 'process_lineage' with the results.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame that contains Taegis process events with process_correlation_id, host_id, tenant_id, and resource_id.
    tenant_id: Optional[str]
        Taegis SDK Tenant ID that the asset lookup is for.  Defaults to None.
    region : Optional[str]
        Taegis SDK Region/Environment that the asset lookup is for.  Defaults to US1.
    max_workers : Optional[int]
        Maximum number of concurrent threads for API calls.  Defaults to 10.
    max_failure_rate : Optional[float]
        Maximum fraction of keys allowed to fail (0.0–1.0) before raising a
        RuntimeError.  Defaults to 0.05 (5 %).
    show_progress : Optional[bool]
        Display a tqdm progress bar.  ``None`` (default) auto-detects based
        on whether a Jupyter notebook environment and tqdm are available.

    Returns
    -------
    pd.DataFrame
        Returns a pandas DataFrame with additional asset information columns.

    Raises
    ------
    ValueError
        If the event is not a process event, or if process_correlation_id, host_id, and resource_id does not have columns in the dataframe a value error will be raised.
        If there the dataframe does not contain a valid tenant identifier.
    RuntimeError
        If the fraction of failed keys exceeds *max_failure_rate*.
    """

    df = df.copy()
    if df.empty:
        return df

    if not all(x in df.columns for x in REQUIRED_KEY_COLS):
        raise ValueError(
            "DataFrame must contain host_id and process_correlation_id columns for lineage lookup."
        )

    key_cols = _build_key_cols(df)

    results_map = _fetch_concurrently(
        df, process_lineage, region=region, tenant_id=tenant_id, max_workers=max_workers,
        max_failure_rate=max_failure_rate, show_progress=show_progress,
    )

    df["process_info.process_lineage"] = df.apply(
        _map_results, axis=1, results_map=results_map, key_cols=key_cols
    )
    df = df.explode("process_info.process_lineage").reset_index(drop=True)
    df["process_info.process_lineage"] = df["process_info.process_lineage"].apply(
        lambda x: {} if pd.isnull(x) else x
    )
    df["process_info.process_lineage.index"] = df["process_info.process_lineage"].apply(
        lambda x: (
            int(x.get("lineage_index", None))
            if x.get("lineage_index", None) is not None
            else None
        )
    )
    return df


def lookup_children(
    df: pd.DataFrame,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
    max_workers: Optional[int] = 10,
    max_failure_rate: Optional[float] = 0.05,
    show_progress: Optional[bool] = None,
) -> pd.DataFrame:
    """
    For each row in the DataFrame, fetch their children processes using process_correlation_id, host_id, tenant_id, and resource_id.
    Adds a new column 'process_children' with the results.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame that contains Taegis process events with process_correlation_id, host_id, tenant_id, and resource_id.
    tenant_id: Optional[str]
        Taegis SDK Tenant ID that the asset lookup is for.  Defaults to None.
    region : Optional[str]
        Taegis SDK Region/Environment that the asset lookup is for.  Defaults to US1.
    max_workers : Optional[int]
        Maximum number of concurrent threads for API calls.  Defaults to 10.
    max_failure_rate : Optional[float]
        Maximum fraction of keys allowed to fail (0.0–1.0) before raising a
        RuntimeError.  Defaults to 0.05 (5 %).
    show_progress : Optional[bool]
        Display a tqdm progress bar.  ``None`` (default) auto-detects based
        on whether a Jupyter notebook environment and tqdm are available.

    Returns
    -------
    pd.DataFrame
        Returns a pandas DataFrame with additional asset information columns.

    Raises
    ------
    ValueError
        If the event is not a process event, or if process_correlation_id, host_id, and resource_id does not have columns in the dataframe a value error will be raised.
    RuntimeError
        If the fraction of failed keys exceeds *max_failure_rate*.
    """

    df = df.copy()
    if df.empty:
        return df

    if not all(x in df.columns for x in REQUIRED_KEY_COLS):
        raise ValueError(
            "DataFrame must contain host_id and process_correlation_id columns for children lookup."
        )

    key_cols = _build_key_cols(df)

    results_map = _fetch_concurrently(
        df, process_children, region=region, tenant_id=tenant_id, max_workers=max_workers,
        max_failure_rate=max_failure_rate, show_progress=show_progress,
    )

    df["process_info.process_children"] = df.apply(
        _map_results, axis=1, results_map=results_map, key_cols=key_cols
    )
    df = df.explode("process_info.process_children").reset_index(drop=True)
    df["process_info.process_children"] = df["process_info.process_children"].apply(
        lambda x: {} if pd.isnull(x) else x
    )
    return df
