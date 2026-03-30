"""Pandas functions for Proccess Lineage Event Dataframes (process only)"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
from taegis_magic.commands.process_trees import process_children, process_lineage

log = logging.getLogger(__name__)

KEY_COLS = ["host_id", "process_correlation_id", "resource_id"]


def _fetch_concurrently(df, fetch_fn, *, region, tenant_id, max_workers):
    """Fetch results for unique key tuples concurrently, returning a results map.

    Deduplicates API calls so identical (host_id, process_correlation_id, resource_id)
    tuples only trigger a single network request.
    """
    unique_keys = (
        df[KEY_COLS]
        .drop_duplicates()
        .dropna(subset=KEY_COLS)
        .itertuples(index=False, name=None)
    )
    unique_keys = list(unique_keys)

    results_map = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_fn,
                region=region,
                tenant_id=tenant_id,
                host_id=key[0],
                process_correlation_id=key[1],
                resource_id=key[2],
            ): key
            for key in unique_keys
        }

        for future in as_completed(futures):
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

    return results_map


def _map_results(row, results_map):
    """Map a row to its pre-fetched results by key tuple."""
    if any(pd.isna(row.get(c)) for c in KEY_COLS):
        return []
    key = (row["host_id"], row["process_correlation_id"], row["resource_id"])
    return results_map.get(key, [])


def lookup_lineage(
    df: pd.DataFrame,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
    max_workers: Optional[int] = 10,
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

    Returns
    -------
    pd.DataFrame
        Returns a pandas DataFrame with additional asset information columns.

    Raises
    ------
    ValueError
        If the event is not a process event, or if process_correlation_id, host_id, and resource_id does not have columns in the dataframe a value error will be raised.
        If there the dataframe does not contain a valid tenant identifier.
    """

    df = df.copy()
    if df.empty:
        return df

    if not all(x in df.columns for x in KEY_COLS):
        raise ValueError(
            "DataFrame must contain host_id, process_correlation_id, and resource_id columns for lineage lookup."
        )

    results_map = _fetch_concurrently(
        df, process_lineage, region=region, tenant_id=tenant_id, max_workers=max_workers
    )

    df["process_info.process_lineage"] = df.apply(
        _map_results, axis=1, results_map=results_map
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

    Returns
    -------
    pd.DataFrame
        Returns a pandas DataFrame with additional asset information columns.

    Raises
    ------
    ValueError
        If the event is not a process event, or if process_correlation_id, host_id, and resource_id does not have columns in the dataframe a value error will be raised.
    """

    df = df.copy()
    if df.empty:
        return df

    if not all(x in df.columns for x in KEY_COLS):
        raise ValueError(
            "DataFrame must contain host_id, process_correlation_id, and resource_id columns for lineage lookup."
        )

    results_map = _fetch_concurrently(
        df, process_children, region=region, tenant_id=tenant_id, max_workers=max_workers
    )

    df["process_info.process_children"] = df.apply(
        _map_results, axis=1, results_map=results_map
    )
    df = df.explode("process_info.process_children").reset_index(drop=True)
    df["process_info.process_children"] = df["process_info.process_children"].apply(
        lambda x: {} if pd.isnull(x) else x
    )
    return df
