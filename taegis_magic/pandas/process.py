import pandas as pd
import logging
from typing import List, Optional
from taegis_magic.core.service import get_service
from taegis_magic.pandas.utils import chunk_df

from taegis_sdk_python.services.events.types import (
    Event,
    EventQueryOptions,
    EventQueryResults,
)
from taegis_sdk_python.config import get_config
from taegis_magic.commands.configure import QUERIES_SECTION
from taegis_magic.commands.events import get_next_page

log = logging.getLogger(__name__)


CONFIG = get_config()
if not CONFIG.has_section(QUERIES_SECTION):
    CONFIG.add_section(QUERIES_SECTION)

def process_correlate_netflow(
    df: pd.DataFrame,
    region: str,
    tenant_id: Optional[str] = None,
    process_columns: Optional[List[str]] = None,
    merge_ons: Optional[List[str]] = None,
):
    """Correlate process data with netflow information.

    Parameters
    ----------
    df : pd.DataFrame   
        Dataframe containing process data
    region : str
        Taegis Region
    tenant_id : Optional[str], optional
        Tenant ID to use for the correlation, by default None
    process_columns : Optional[List[str]], optional
        List of process columns to lookup in input DataFrame, by default None
    merge_ons : Optional[List[str]], optional
        List of netflow columns to merge on, by default None

    Returns
    -------
    pd.DataFrame
        Dataframe with correlated netflow data
    """

    if df.empty:
        return df
    
    if not process_columns:
        process_columns = ["sensor_id","process_id"]
    
    ## Ensure columns in process_columns argument exist in df
    for pc in process_columns:
        if pc not in df.columns:
            log.error(f"Column {pc} not found in dataframe")
            process_columns.remove(pc)
    
    if not process_columns:
        log.error("No valid user id columns found in dataframe")
        return df
    
    if not merge_ons:
        merge_ons = ["sensor_id","processcorrelationid.pid"]

    service = get_service(tenant_id=tenant_id, environment=region)

    process_unique_values = df[process_columns].drop_duplicates()

    results = []

    options = EventQueryOptions(
        timestamp_ascending=True,
        page_size=1000,
        max_rows=100000,
        aggregation_off=False,
    )
    
    base_query = f"FROM netflow where "
        
    # Retrieve netflow data that correlates with process data in batches. 
    for chunk in chunk_df(process_unique_values, 100):

        query = base_query + build_where_clause(chunk)
        result = results.append(
            service.events.subscription.event_query(
            query,
            options=options,
            metadata={
                "callerName": CONFIG[QUERIES_SECTION].get(
                    "callername", fallback="Taegis Magic"
                    ),
                },
            )
        )

        results.extend(result)
        next_page = get_next_page(result)

        while next_page:
            result = service.events.subscription.event_page(next_page)
            results.extend(result)
            next_page = get_next_page(result)

    return None



def build_where_clause(chunk: pd.DataFrame) -> str:
    """Build a WHERE clause from DataFrame rows."""
    conditions = []
    
    for _, row in chunk.iterrows():
        # Build AND conditions for each column in the row
        row_conditions = [f"{col}='{row[col]}'" for col in chunk.columns]
        # Join with AND and wrap in parentheses
        conditions.append(f"({' and '.join(row_conditions)})")
    
    # Join all row conditions with OR
    return ' or '.join(conditions)