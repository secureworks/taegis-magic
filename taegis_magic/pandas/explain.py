"""Functions for getting LLM generated explanations of specific data"""

import logging
from dataclasses import asdict
from typing import Optional

import pandas as pd
from taegis_sdk_python.services.context_summarizer.types import (
    CommandLineExplanationInput,
    EventExplanationInput,
)

from taegis_magic.core.service import get_service
from taegis_magic.core.utils import to_dataframe

log = logging.getLogger(__name__)


def get_command_line_explanation(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    event_id_column: Optional[str] = "resource_id",
) -> pd.DataFrame:
    """
    Takes in a DataFrame where a column (the event_id_column) contains event_ids. This function will then send those event_ids
    to an API to get a LLM generated explanation of those commmandline commands. A new DataFrame will be returned that adds
    3 additional columns to the input DataFrame: command (the command being explained), explanation (explanation of command), and
    event (the event_ids actually passed in). If invalid event_ids are passed in then the returned DataFrame will still contain
    the same 3 columns, but will have a message indicating that no explanation could be found.

    An empty DataFrame will be returned upon pipe failure.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing a column with event_ids
    region : str
        Taegis Region.
    tenant_id : str
        Tenant ID to use for the correlation.
    event_id_column : Optional[str]
        Name of column in input df that contains event_ids. Defaults to resource_id.

    Returns
    -------
    pd.DataFrame
        A DataFrame that contains info regarding the commands, command explanations, and associate event_ids.
    """

    if df.empty:
        return pd.DataFrame()

    if event_id_column not in df:
        log.warning(f"{event_id_column} not found in dataframe")

        if f'event_data.{event_id_column}' in df:
            log.info(f"Found '{event_id_column}' column with 'event_data.' prefix. Using that column for event IDs.")
            event_id_column = f'event_data.{event_id_column}'
        else:
            return pd.DataFrame()

    service = get_service(environment=region, tenant_id=tenant_id)
    event_ids = [
        event_id for event_id in df[event_id_column].tolist() if event_id.startswith("event://priv:scwx.process:")
    ]
    
    if not event_ids:
        log.error(f"No valid event IDs found in column '{event_id_column}'. Ensure that the column contains valid event IDs starting with 'event://priv:scwx.process:'.")
        return pd.DataFrame()

    explanation_input = CommandLineExplanationInput(events=event_ids)

    explanations = service.context_summarizer.query.explain_command_lines(
        explanation_input
    )

    if not explanations:
        log.warning("No explanations were returned from the service. Please check if the event IDs are valid and if the service is functioning correctly.")
        return pd.DataFrame()

    explanations_df = to_dataframe([asdict(explanation) for explanation in explanations])

    explanations_df.replace("", pd.NA, inplace=True)

    if 'event' not in explanations_df.columns or 'explanation' not in explanations_df.columns:
        log.error("The expected columns 'event' and 'explanation' were not found in the explanations DataFrame. Please check the service response and ensure it returns the correct data.")
        return pd.DataFrame()

    if (
        explanations_df["command"].isna().all()
        and explanations_df["explanation"].isna().all()
    ):
        log.error(
            "Could not generate explanation for commands. Please make sure supplied DataFrame contains valid events."
        )
        return pd.DataFrame()

    ret_df = pd.merge(
        left=df,
        right=explanations_df,
        left_on=event_id_column,
        right_on="event",
        suffixes=(None, ".explain"),
    )

    return ret_df


def get_event_explanation(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    event_id_column: Optional[str] = "resource_id",
) -> pd.DataFrame:
    """
    Takes in a DataFrame where a column (the event_id_column) contains event_ids. This function will then send those event_ids
    to an API to get a LLM generated explanation of the event contents. A new DataFrame will be returned that contains
    3 additional columns: error (the error the AI explaination or API encountered), explanation (explanation of command), and event (the event_ids actually
    passed in). If invalid event_ids are passed in then the returned DataFrame will contain the same 3 columns, but will
    have a message indicating that no explanation could be found.

    An empty DataFrame will be returned upon pipe failure.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing a column with event_ids
    region : str
        Taegis Region.
    tenant_id : str
        Tenant ID to use for the correlation.
    event_id_column : Optional[str]
        Name of column in input df that contains event_ids. Defaults to resource_id.

    Returns
    -------
    pd.DataFrame
        A DataFrame that contains info regarding the commands, command explanations, and associate event_ids.
    """

    if df.empty:
        return pd.DataFrame()

    if event_id_column not in df:
        log.error(f"{event_id_column} not found in dataframe")

        if f'event_data.{event_id_column}' in df:
            log.info(f"Found '{event_id_column}' column with 'event_data.' prefix. Using that column for event IDs.")
            event_id_column = f'event_data.{event_id_column}'
        else:
            return pd.DataFrame()

    service = get_service(environment=region, tenant_id=tenant_id)
    event_ids = df[event_id_column].tolist()

    if not event_ids:
        log.error(f"No valid event IDs found in column '{event_id_column}'.")
        return pd.DataFrame()

    explanation_input = EventExplanationInput(events=event_ids)

    explanations = service.context_summarizer.query.generative_ai_event_explain(
        explanation_input
    )

    if not explanations:
        log.error("No explanations were returned from the service. Please check if the event IDs are valid and if the service is functioning correctly.")
        return pd.DataFrame()

    explanations_df = to_dataframe([asdict(explanation) for explanation in explanations])

    explanations_df.replace("", pd.NA, inplace=True)

    if 'event' not in explanations_df.columns or 'explanation' not in explanations_df.columns:
        log.error("The expected columns 'event' and 'explanation' were not found in the explanations DataFrame. Please check the service response and ensure it returns the correct data.")
        return pd.DataFrame()

    if explanations_df["event"].isna().all() and explanations_df["explanation"].isna().all():
        log.error(
            "Could not generate explanation for events. Please make sure supplied DataFrame contains valid events."
        )
        return pd.DataFrame()

    ret_df = pd.merge(
        df,
        explanations_df,
        how="left",
        left_on=event_id_column,
        right_on="event",
        suffixes=(None, ".explain"),
    )

    return ret_df
