"""Taegis Tenants pandas functions."""

import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import pandas as pd
from taegis_magic.core.service import get_service
from taegis_magic.core.utils import get_tenant_id_column, to_dataframe
from taegis_sdk_python.services.tenants.types import TenantsQuery

log = logging.getLogger(__name__)


def inflate_environments(
    df: pd.DataFrame, columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Inflate environments to columns.

    Parameters
    ----------
    df : pd.DataFrame
        Tenants DateFrame

    Returns
    -------
    pd.DataFrame
        Inflated Tenants DateFrame

    Raises
    ------
    ValueError
        'environments' column not found in DataFrame
    """
    if df.empty:
        return df

    df = df.copy()

    if not columns:
        columns = ["tenant.environments", "environments"]

    environment_column = None
    for column in columns:
        if column in df.columns:
            environment_column = column
            break

    if not environment_column:
        raise ValueError(f"{columns} columns not found in DataFrame")

    def environments_to_dict(environments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Flatten environments array to dictionary key value pairs."""
        env_dict = {}
        for environment in environments:
            env_dict[environment.get("name", "error")] = environment.get(
                "enabled", False
            )
            env_dict[f"{environment.get('name', 'error')}.created_at"] = (
                environment.get("created_at", False)
            )
            env_dict[f"{environment.get('name', 'error')}.updated_at"] = (
                environment.get("updated_at", False)
            )
        return env_dict

    df = pd.concat(
        [
            df,
            df[environment_column]
            .apply(environments_to_dict)
            .apply(pd.Series)
            .fillna(False)
            .add_prefix(f"{environment_column}."),
        ],
        axis=1,
    )

    return df


def lookup_tenants(
    df: pd.DataFrame, region: str, tenant_id_column: Optional[str] = None
) -> pd.DataFrame:
    """Look up tenants and correlate to the input dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with tenant_id or tenant.id column
    region : str
        Taegis Region

    Returns
    -------
    pd.DataFrame
        Dataframe with correlated tenant.
        New columns will be prefixed with 'tenant.'
        Overlapping columns will be suffixed with '.lookup_tenants'

    Raises
    ------
    ValueError
        If the no valid tenant id column is found.
    """
    if df.empty:
        return df

    if not tenant_id_column:
        tenant_id_column = get_tenant_id_column(df)

    if not tenant_id_column in df.columns:
        raise ValueError(f"{tenant_id_column} not found in DataFrame columns.")

    if [
        column
        for column in df.columns
        if column.startswith("tenant.") and column != "tenant.id"
    ]:
        log.debug("Tenant columns already exist in DataFrame.")
        return df

    service = get_service(environment=region)

    max_results = 1000
    page_number = 1

    ids = df[tenant_id_column].unique().tolist()

    log.debug(f"Polling page: {page_number}")
    result = service.tenants.query.tenants(
        TenantsQuery(
            max_results=max_results,
            page_num=page_number,
            ids=ids,
        )
    )

    results = [result]

    while result.has_more:
        page_number += 1
        log.debug(f"Polling page: {page_number}")

        result = service.tenants.query.tenants(
            TenantsQuery(
                max_results=max_results,
                page_num=page_number,
                ids=ids,
            )
        )

        results.append(result)

    tenants_df = to_dataframe(
        [asdict(tenant) for result in results for tenant in result.results or []]
    ).add_prefix("tenant.")

    return pd.merge(
        df,
        tenants_df,
        left_on=tenant_id_column,
        right_on="tenant.id",
        how="left",
        suffixes=(None, ".lookup_tenants"),
    )
