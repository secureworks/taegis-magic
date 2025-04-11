"""Pandas functions for Alerts DataFrames."""

import logging
import re
import time
from typing import Dict, List, Optional

import pandas as pd
from numpy import add
from taegis_magic.commands.alerts import (
    AlertsResultsNormalizer,
    alerts_service_search_with_events,
)
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_magic.pandas.utils import chunk_list, coalesce_columns

from taegis_sdk_python.services.alerts.types import (
    ResolutionStatus,
    SearchRequestInput,
    TimestampInput,
    UpdateResolutionRequestInput,
)

log = logging.getLogger(__name__)


def convert_alert_timestamps(
    df: pd.DataFrame, format_: str = "%Y-%m-%dT%H:%M:%SZ"
) -> pd.DataFrame:
    """Takes an Alerts dataframe and converts all the metadata time columns
    into a readable time format. All columns with .nanos will be dropped to reduce
    the amount of columns in the dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Alerts Dataframe
    format : str, optional
        Datetime string format to be used, by default "%Y-%m-%dT%H:%M:%SZ"

    Returns
    -------
    pd.DataFrame
        Returns Alerts Dataframe with added taegis_magic timestamp columns
    """

    if df.empty:
        return df

    df = df.copy()

    for column in [
        column
        for column in df.columns
        if column.endswith(".seconds") and not column.startswith("taegis_magic.")
    ]:
        try:
            df[f"taegis_magic.{column}"] = pd.to_datetime(
                df[column], errors="ignore", unit="s"
            ).dt.strftime(format_)
        except Exception as exc:
            log.error(exc)
            continue

        df[f"taegis_magic.{column}"] = df[f"taegis_magic.{column}"].fillna("N/A")

    return df


def inflate_raw_events(df: pd.DataFrame) -> pd.DataFrame:
    """Takes a DataFrame containing Alert (V2) results
    and concats the nested JSON blob containing the
    source event data as columns. Returns a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Alerts (V2) DataFrame

    Returns
    -------
    pd.DataFrame
        Returns an Alerts (V2) Dataframe with exploded events data

    Raises
    ------
    ValueError
        Dataframe does not contain an event_ids column
    """

    if df.empty:
        return df

    if "event_ids" not in df.columns:
        raise ValueError("Dataframe does not contain an event_ids column")

    if not any(df.columns.str.startswith("event_data.")):
        df = df.explode("event_ids").reset_index(drop=True)

        event_df = pd.json_normalize(df["event_ids"]).dropna(axis=1, how="all")

        no_prefix = [col for col in event_df.columns if "event_data." not in col]

        if no_prefix:
            for column in no_prefix:
                event_df = event_df.rename({column: f"event_data.{column}"}, axis=1)

        return pd.concat(
            [
                df,
                event_df,
            ],
            axis=1,
        )

    return df


def get_alerts_from_aggregation(
    df: pd.DataFrame,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
    earliest: str = "-7d",
    latest: Optional[str] = None,
    limit: int = 100,
    additional_operators: Optional[List[str]] = None,
) -> pd.DataFrame:
    """This function takes a DataFrame of aggregate alerts and retrieves
    corresponding non-aggregate data for the same alerts. The main use-case
    is reviewing the aggregate alert data in an interactive table widget
    (such as IPyDataGrid), then use this function to pull down the
    non-aggregate alerts to look more closely at their contents.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame of Alerts Aggregation
    region : str, optional
        Taegis environment, by default "charlie", options: "charlie", "delta", "echo"
    tenant : str, optional
        Which tenant_id to use for the alerts query
    earliest : str, optional
        Earliest timeframe in the alert query, defaults to -7d.
    latest : str, optional
        Latest timeframe in the alert query, defaults to None.
    limit : int, optional
        Alert results limit, defaults to 100.
    additional_operators : Optional[List[str]], optional
        Additional operators to add to the query, by default None.

    Returns
    -------
    pd.DataFrame
        Alerts Dataframe

    Raises
    ------
    ValueError
        If the Dataframe contained no data to populate sub-queries for the aggregate alerts query
    """

    cols = [
        column
        for column in df.columns
        if column not in ["count"] and re.match("^[^0-9]", column) is not None
    ]

    sub_query = []
    single_quote = "'"
    replacement = "\\'"
    for _, row in df.iterrows():
        row_query = [
            (
                f"{col} = '{row[col]}'"
                if not str(row[col]).find("'") > -1
                else f"{col} = e'{str(row[col]).replace(single_quote, replacement)}'"
            )
            for col in cols
            if col in row
        ]

        sub_query.append("(" + " AND ".join(row_query) + ")")

    if not sub_query:
        raise ValueError(
            "No sub-queries in the alerts query WHERE statement. Please look to see if your dataframe has aggregate alert data."
        )

    unique_sub_queries = []

    for x in sub_query:
        if x not in unique_sub_queries:
            unique_sub_queries.append(x)

    sub_query_string = " OR \n".join(x for x in unique_sub_queries)

    additional_operators_string = ""
    if additional_operators:
        additional_operators_string = " AND \n ".join(
            [f"({operator})" for operator in additional_operators]
        )
        additional_operators_string = f"AND \n{additional_operators_string}"

        for op in additional_operators:
            for col in cols:
                if col in op:
                    log.warning(
                        f"Column {col} is already in the sub-query. Please check your additional operators."
                    )

    query = f"""
    FROM alert
    WHERE ({sub_query_string})
    {additional_operators_string}
    {f"EARLIEST={earliest}" if earliest else ""}
    {f"LATEST={latest}" if latest else ""}
    """

    service = get_service(environment=region, tenant_id=tenant)

    try:
        response = alerts_service_search_with_events(
            service,
            SearchRequestInput(
                cql_query=query,
                offset=0,
                limit=limit,
            ),
        )
    except Exception as exc:
        log.error(exc)
        return pd.DataFrame()

    normalized_results = AlertsResultsNormalizer(
        raw_results=[response],
        service="alerts",
        tenant_id=tenant,
        region=region,
        arguments={
            "query": query,
            "tenant": tenant,
            "region": region,
            "earliest": earliest,
            "latest": latest,
        },
    )

    return pd.json_normalize(
        normalized_results.results,
        max_level=3,
    )


def inflate_third_party_details(df: pd.DataFrame) -> pd.DataFrame:
    """Expands `third_party_details` column of a DataFrame and returns
    a new DataFrame where the content of `third_party_details` is
    represented as columns.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing an `third_party_details` column
    Returns
    -------
    pd.DataFrame
        DataFrame with `third_party_details` columns appended

    Example
    -------
    Example::

        %%xdr ql alerts get --assign third_party_alerts --cache
        FROM alert
        WHERE third_party_details IS NOT NULL
        EARLIEST=-1d | head 25

        third_party_alerts_inflated = third_party_alerts.pipe(inflate_third_party_details)

    """

    def third_party_details_to_dict(third_party_details: List[Dict[str, str]]) -> Dict:
        parsed_third_party_details = []
        for detail in third_party_details:
            kv_pairs = detail.get("generic", {}).get("generic", {}).get("record", [])
            for kv_pair in kv_pairs:
                parsed_third_party_details.append(tuple(kv_pair.values()))
        return dict(parsed_third_party_details)

    if "third_party_details" in df.columns:
        if df["third_party_details"].any():
            return pd.concat(
                [
                    df,
                    df["third_party_details"]
                    .apply(third_party_details_to_dict)
                    .apply(pd.Series)
                    .add_prefix("third_party_details."),
                ],
                axis=1,
            )

        log.warning("third_party_details column contains no data to be inflated.")
        return df

    log.warning("Dataframe did not contain a third_party_details column.")
    return df


def severity_rounded_and_category(
    df: pd.DataFrame, severity_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Converts the Taegis alert severity and creates two new taegis_magic. columns.
    One column will convert the severity to a numeric value, and round it to two decimal places.
    The second column will then apply a severity category such as Informational,
    Critical, High, Medium, and Low.

    Parameters
    ----------
    df : pd.DataFrame
        Taegis Alerts Dataframe that contains a severity column.

    Returns
    -------
    pd.DataFrame
        Returns a dataframe containing two taegis_magic. columns if the passed in dataframe contains a
        severity column. If not, the original dataframe is returned.
    """
    if df.empty:
        return df

    df = df.copy()

    def bucket_severity_scores(severity):
        if severity >= 0.8:
            return "Critical"
        elif severity < 0.8 and severity >= 0.6:
            return "High"
        elif severity < 0.6 and severity >= 0.4:
            return "Medium"
        elif severity < 0.4 and severity >= 0.2:
            return "Low"
        elif severity < 0.2:
            return "Informational"
        else:
            raise ValueError

    if severity_columns is None:
        # sorted by priority of field
        severity_columns = ["metadata.severity", "severity"]

    log.debug("Checking severity identifier columns...")
    valid_severity_columns = []

    for severity_col in severity_columns:
        if severity_col in df.columns:
            valid_severity_columns.append(severity_col)
            log.debug(f"Found severity identifier column: {severity_col}...")

    if not valid_severity_columns:
        raise ValueError(
            f"DataFrame does not contain a vaild severity column: {valid_severity_columns}"
        )
    df["taegis_magic.severity"] = (
        coalesce_columns(df, valid_severity_columns).apply(pd.to_numeric).round(2)
    )
    df["taegis_magic.severity_category"] = df["taegis_magic.severity"].apply(
        bucket_severity_scores
    )
    return df


def provide_feedback(
    df: pd.DataFrame,
    environment: str,
    status: ResolutionStatus,
    reason: str,
) -> pd.DataFrame:
    """
    Resolve a DataFrame of Alerts.

    Parameters
    ----------
    df : pd.DataFrame
        Alerts DataFrame
    status : ResolutionStatus
        Status to resolve alerts
    reason : str, optional
        Reason for resolution status

    Returns
    -------
    pd.DataFrame
        Alerts DataFrame
    """

    service = get_service(environment=environment)

    if "tenant_id" in df.columns:
        tenant_identifier = "tenant_id"
    elif "tenant.id" in df.columns:
        tenant_identifier = "tenant.id"
    else:
        raise ValueError("DataFrame does not contain a valid tenant identifier")

    tenants = set(df[tenant_identifier].to_list())

    for tenant in tenants:
        alert_ids = df[df[tenant_identifier] == tenant]["id"].unique()

        with service(tenant_id=tenant):
            for chunk in chunk_list(alert_ids, 250):
                service.alerts.mutation.alerts_service_update_resolution_info(
                    UpdateResolutionRequestInput(
                        alert_ids=chunk,
                        resolution_status=status,
                        reason=reason,
                        requested_at=TimestampInput(seconds=int(time.time())),
                    )
                )

    return df


def normalize_creator_name(
    df: pd.DataFrame, columns: Optional[List[str]] = None, region: Optional[str] = None
) -> pd.DataFrame:
    """
    Normalize creator ids to pretty names.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with `creator` or `metadata.creator.detector.detector_id` fields
    columns : Optional[List[str]], optional
        User provided columns, by default None
    region : Optional[str], optional
        Region to query, by default None

    Returns
    -------
    pd.DataFrame
        DataFrame with pretty names.

    Raises
    ------
    KeyError
        No valid columns to correlate creator names.
    """
    df = df.copy()

    if df.empty:
        return df

    if not columns:
        columns = ["creator", "metadata.creator.detector.detector_id"]

    column = None
    for c in columns:
        if c in df.columns:
            column = c
            break

    if not column:
        raise KeyError("No valid columns to correlate creator names.")

    service = get_service(environment=region)

    detector_results = service.detector_registry.query.detectors()

    df["taegis_magic.creator.display_name"] = df.apply(
        lambda x: next(
            iter(
                [
                    detector.display_name
                    for detector in detector_results
                    if detector.creator_name == x[column]
                ]
                or [x[column]]
            )
        ),
        axis=1,
    )

    return df
