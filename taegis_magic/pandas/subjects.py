"""Pandas utility functions for correlating, transforming and pivoting on subjects in Taegis."""

import logging

from typing import Optional, List
import pandas as pd
from dataclasses import asdict

from taegis_magic.core.service import get_service
from taegis_magic.core.utils import to_dataframe
from taegis_magic.pandas.utils import chunk_list

log = logging.getLogger(__name__)


def lookup_users(
    df: pd.DataFrame,
    region: str,
    tenant_id: Optional[str] = None,
    user_id_columns: Optional[List[str]] = None,
    merge_ons: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Look up users and correlate to the input dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe
    region : str
        Taegis Region
    tenant_id : Optional[str], optional
        Tenant ID to use for the lookup, by default None.
    user_id_columns : Optional[List[str]], optional
        List of user id columns to look up, by default None.
        If None, defaults to ['created_by', 'updated_by'].
    merge_ons : Optional[List[str]], optional
        List of user id fields to merge on, by default None.
        If None, defaults to ['user.id', 'user.idp_user_id', 'user.user_id']

    Returns
    -------
    pd.DataFrame
        Dataframe with correlated User.
        New columns will be prefixed with 'user.'
        Overlapping columns will be suffixed with '.lookup_users'

    Raises
    ------
    ValueError
        If the no valid user id column is found.
    """
    if df.empty:
        return df

    if not user_id_columns:
        user_id_columns = ["created_by", "updated_by"]

    if not merge_ons:
        merge_ons = ["user.id", "user.idp_user_id", "user.user_id"]

    for column in user_id_columns.copy():
        if not column in df.columns:
            log.error(f"Column {column} not found in dataframe")
            user_id_columns.remove(column)

    if not user_id_columns:
        log.error("No valid user id columns found in dataframe")
        return df

    if [
        column
        for column in df.columns
        for user_column in user_id_columns
        if column.startswith(f"{user_column}.user.")
        and column != f"{user_column}.user.id"
    ]:
        log.debug("User columns already exist in DataFrame.")
        return df

    service = get_service(tenant_id=tenant_id, environment=region)

    ids = set()
    for column in user_id_columns:
        ids.update(df[column].dropna().unique().tolist())
    ids = list(ids)

    results = []
    for chunk in chunk_list(ids, 100):
        log.debug(f"Correlating users: {chunk}")
        # need to access deprecated user_id field
        with service(
            output="""
            error
            user { 
                id idp_user_id user_id 
                created_at updated_at created_by 
                updated_by last_login last_ip invited_date 
                registered_date deactivated_date status 
                status_localized email email_normalized 
                family_name given_name phone_number 
                phone_extension secondary_phone_number 
                secondary_phone_extension environments 
                timezone tenant_status tenant_status_localized 
                entitlement_channel allowed_entitlement_channels 
                masked community_role is_scwx is_partner 
                pre_verified 
                accessible_tenants { 
                    id name name_normalized enabled allow_response_actions 
                    actions_approver expires_at is_partner is_organization partner 
                    parent 
                    environments { name enabled } 
                    labels { id tenant_id name value owner_partner_tenant_id } 
                    services { id name description } 
                } 
                role_assignments { 
                    id tenant_id role_id deactivated role_name role_display_name 
                    expires_at created_at updated_at created_by updated_by allowed_environments 
                } 
                eula { date version } 
                preferred_language 
            }
        """
        ):
            result = service.users.query.search_tdrusers_by_ids(user_ids=chunk)
        results.extend(result)

    users_df = to_dataframe([asdict(result) for result in results])

    for column in user_id_columns:
        dfs = []
        column_dataframe = df.copy()
        column_users = users_df.copy().add_prefix(f"{column}.")

        for merge_on in merge_ons:
            log.debug(f"Merge users for column: {column} with right_on: {merge_on}")

            merge_dataframe = pd.merge(
                column_dataframe,
                column_users,
                left_on=column,
                right_on=[f"{column}.{merge_on}"],
                how="left",
                suffixes=(None, ".lookup_users"),
            )

            # filter matched rows from merge
            filtered_dataframe = merge_dataframe[
                ~merge_dataframe[f"{column}.{merge_on}"].isna()
            ]
            if not filtered_dataframe.empty:
                dfs.append(filtered_dataframe)
                # filter out matched rows from df
                column_dataframe = column_dataframe[
                    ~column_dataframe[column].isin(
                        filtered_dataframe[f"{column}.{merge_on}"]
                    )
                ]

        if not column_dataframe.empty:
            log.debug(f"Adding column dataframe: {column_dataframe['id']}")
            dfs.append(column_dataframe)

        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return df
