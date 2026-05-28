"""Functions for getting LLM generated explanations of specific data"""

import logging

import pandas as pd
from taegis_magic.core.service import get_service
from typing import Optional

from taegis_sdk_python.services.context_summarizer.types import CommandLineExplanationInput

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
    to an API to get a LLM generated explanation of those commmandline commands. A new DataFrame will be returned that contains
    3 columns: command (the command being explanation), explanation (explanation of command), and event (the event_ids actually
    passed in). If invalid event_ids are passed in then the returned DataFrame will contain the same 3 columns, but will
    have a message indicating that no explanation could be found. 

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
        return df
    
    if event_id_column not in df:
        log.warning(f"{event_id_column} not found in dataframe")
        return df

    service = get_service(environment=region, tenant_id=tenant_id)
    event_ids = df[event_id_column].tolist()
    explanation_input = CommandLineExplanationInput(events=event_ids)
    
    explanations = service.context_summarizer.query.explain_command_lines(explanation_input)
    
    ret_df = pd.DataFrame(explanations)

    ret_df.replace("", pd.NA, inplace=True)
    
    if ret_df["command"].isna().all() and ret_df["explanation"].isna().all():
        log.warning("Could not generate explanation for commands. Please make sure supplied DataFrame contains valid events.")

    return ret_df