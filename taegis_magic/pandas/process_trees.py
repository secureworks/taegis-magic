"""Pandas functions for Proccess Lineage Event Dataframes (process only)"""

import logging
from dataclasses import asdict
from typing import List, Optional

import pandas as pd
from taegis_magic.commands.process_trees import process_children, process_lineage

log = logging.getLogger(__name__)


def lookup_lineage(
    df: pd.DataFrame, region: Optional[str] = None, tenant_id: Optional[str] = None
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

    required_cols = ["host_id", "process_correlation_id", "resource_id"]
    if not all(x in df.columns for x in required_cols):
        raise ValueError(
            "DataFrame must contain host_id, process_correlation_id, and resource_id columns for lineage lookup."
        )

    def get_lineage(row):
        if (
            pd.isna(row.get("host_id"))
            or pd.isna(row.get("process_correlation_id"))
            or pd.isna(row.get("resource_id"))
        ):
            return []

        else:
            result = process_lineage(
                region=region,
                tenant_id=tenant_id,
                host_id=row.get("host_id"),
                process_correlation_id=row.get("process_correlation_id"),
                resource_id=row.get("resource_id"),
            )

        # Check if there are results in the process_lineage call. If not, return empty.
        if hasattr(result, "results") and result.results is not None:
            return result.results
        else:
            return []

    df["process_info.process_lineage"] = df.apply(get_lineage, axis=1)
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
    df: pd.DataFrame, region: Optional[str] = None, tenant_id: Optional[str] = None
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

    required_cols = ["host_id", "process_correlation_id", "resource_id"]
    if not all(x in df.columns for x in required_cols):
        raise ValueError(
            "DataFrame must contain host_id, process_correlation_id, and resource_id columns for lineage lookup."
        )

    def get_children(row):
        if (
            pd.isna(row.get("host_id"))
            or pd.isna(row.get("process_correlation_id"))
            or pd.isna(row.get("resource_id"))
        ):
            return []

        else:
            result = process_children(
                region=region,
                tenant_id=tenant_id,
                host_id=row.get("host_id"),
                process_correlation_id=row.get("process_correlation_id"),
                resource_id=row.get("resource_id"),
            )

        # Check if there are results in the process_children call. If not, return empty.
        if hasattr(result, "results") and result.results is not None:
            return result.results
        else:
            return []

    df["process_info.process_children"] = df.apply(get_children, axis=1)
    df = df.explode("process_info.process_children").reset_index(drop=True)
    df["process_info.process_children"] = df["process_info.process_children"].apply(
        lambda x: {} if pd.isnull(x) else x
    )
    return df
