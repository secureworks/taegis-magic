"""Pandas functions for Asset Lookups in Event and Alert DataFrames."""

import logging
import pandas as pd
from typing import List, Optional
from dataclasses import asdict

from taegis_sdk_python import GraphQLService
from taegis_sdk_python.services.assets2.types import (
    AssetV2,
    AssetFilter,
    AssetWhereInputV2,
)
from taegis_magic.pandas.utils import chunk_list, get_tenant_id
from taegis_magic.core.service import get_service
from taegis_magic.core.utils import to_dataframe

log = logging.getLogger(__name__)


def get_assets(
    service: GraphQLService,
    filter: AssetFilter,
) -> List[AssetV2]:
    """Takes an asset filter to obtain a list of assets

    Parameters
    ----------
    service : GraphQLService
        Taegis SDK GraphQL service.
    filter : AssetFilter
        Asset filter that specifies what assets to return back.

    Returns
    -------
    List[AssetV2]
        Returns a list of AssetV2 objects.
    """
    all_assets = []

    response = service.assets2.query.assets_v2(
        filter_=filter,
    )

    all_assets.extend(response.assets)
    more_pages = response.page_info.has_next_page
    end_cursor = response.page_info.end_cursor

    while more_pages:
        response = service.assets2.query.assets_v2(
            after=end_cursor,
            filter_=filter,
        )
        more_pages = response.page_info.has_next_page
        end_cursor = response.page_info.end_cursor
        all_assets.extend(response.assets)

    return all_assets


def assets_from_list(
    service: GraphQLService,
    asset_list: List[str],
):
    """Takes a list of assets and returns a list of AssetV2 objects.

    Parameters
    ----------
    service : GraphQLService
        Taegis SDK GraphQL service object.
    asset_list : List[str]
        A list of Taegis host_ids to be used in the asset lookup.

    Returns
    -------
    List[AssetV2]
        Returns a list of AssetV2 objects.
    """
    asset_data = get_assets(
        service=service,
        filter=AssetFilter(
            where=AssetWhereInputV2(
                or_=[AssetWhereInputV2(host_id=x) for x in asset_list]
            )
        ),
    )

    return asset_data


def lookup_assets(
    df: pd.DataFrame, env: Optional[str] = None, region: Optional[str] = None
) -> pd.DataFrame:
    """Takes a Taegis pandas dataframe that contains host_ids and tenant_ids columns
    and preforms a assetv2 lookup using the Taegis SDK on the unique host_ids.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame that contains Taegis host_ids and tenant_ids.
    env : Optional[str], deprecated
        Taegis SDK Region/Environment that the asset lookup is for.  Defaults to US1.
    region : Optional[str]
        Taegis SDK Region/Environment that the asset lookup is for.  Defaults to US1.

    Returns
    -------
    pd.DataFrame
        Returns a pandas DataFrame with additional asset information columns.

    Raises
    ------
    ValueError
        If there are no valid host_id columns in the dataframe a value error will be raised.
    """
    if env:
        log.warning("The `env` parameter is deprecated. Please use `region` instead.")
        if not region:
            region = env

    df = df.copy()

    if df.empty:
        return df

    host_id_col = next(
        x for x in df.columns if "host_id" in x and not "asset_info." in x
    )

    if not host_id_col:
        raise ValueError("Dataframe does not contain an host_id column")

    if "tenant_id" in df.columns:
        tenant_identifier = "tenant_id"
    elif "tenant.id" in df.columns:
        tenant_identifier = "tenant.id"
    else:
        raise ValueError("DataFrame does not contain a valid tenant identifier")

    service = get_service(environment=region)

    tenants_series = df[tenant_identifier].apply(get_tenant_id)
    tenants_list = list(tenants_series.dropna().unique())
    assets_df = pd.DataFrame()

    for tenant in tenants_list:
        host_list = list(df[tenants_series == tenant][host_id_col].dropna().unique())

        with service(tenant_id=tenant):
            for host_ids in chunk_list(host_list, 2000):
                asset_results = assets_from_list(
                    service=service,
                    asset_list=host_ids,
                )
                if asset_results:
                    assets_df = pd.concat(
                        [
                            assets_df,
                            to_dataframe(results=[asdict(x) for x in asset_results])
                            .assign(
                                hostname=lambda x: x.hostnames.apply(
                                    lambda x: x[0].get("hostname", "N/A")
                                )
                            )
                            .add_prefix("asset_info."),
                        ]
                    )
                    continue

    return df.merge(
        assets_df,
        how="left",
        left_on=host_id_col,
        right_on="asset_info.host_id",
    )
