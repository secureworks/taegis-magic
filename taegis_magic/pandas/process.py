import pandas as pd
import logging
from typing import List, Optional
from dataclasses import asdict
from taegis_magic.core.service import get_service
from taegis_magic.pandas.utils import chunk_list
from dataclasses import dataclass
from taegis_magic.core.utils import to_dataframe

from jinja2 import Environment, PackageLoader

from taegis_sdk_python.services.events.types import (
    EventQueryOptions,
)
from taegis_sdk_python.config import get_config
from taegis_magic.commands.configure import QUERIES_SECTION
from taegis_magic.commands.events import get_next_page

log = logging.getLogger(__name__)

jinja_env = Environment(loader=PackageLoader("taegis_magic","templates/process"))
NETFLOW_TEMPLATE = "netflow_correlation_id.jinja"

NETFLOW = "netflow"

CONFIG = get_config()
if not CONFIG.has_section(QUERIES_SECTION):
    CONFIG.add_section(QUERIES_SECTION)


@dataclass
class NetflowCorrelationId:
    host_id: str
    pid: str
    time_window: str

    def __str__(self):
        # PID can be in the form of `pid` only OR `pid:timewindow`
        return f"(host_id='{self.host_id}' AND ((processcorrelationid.pid='{self.pid+':'+self.time_window}') OR (processcorrelationid.pid='{self.pid}' AND processcorrelationid.timewindow='{self.time_window}'))) "


def process_correlate_netflow(
    df: pd.DataFrame,
    region: str,
    tenant_id: Optional[str] = None,
    process_column: Optional[List[str]] = None,
):
    """Correlate process data with netflow information.

    Input DataFrame is expected to have a column whose row values contain process_correlation_ids
    that have the format of {host_id}:{process_id}:{time_window}.`

    Parameters
    ----------
    df : pd.DataFrame   
        Dataframe containing process data.
    region : str
        Taegis Region.
    tenant_id : Optional[str], optional
        Tenant ID to use for the correlation, by default None.
    process_column : Optional[str], optional
        Process column to lookup in input DataFrame, by default process_correlation_id

    Returns
    -------
    pd.DataFrame
        A new Dataframe with correlated netflow data. New columns will be prepended with 'netflow'

    Example
    -------
    >>> import pandas as pd
    >>> df = pd.DataFrame({"process_correlation_id": ["host123:1234:56789", 1, "host123:1234:56789"]})
    >>> df
       process_correlation_id
    0      host123:1234:56789
    1                       1
    2      host123:1234:56789
    >>> result = process_correlate_netflow(df, region="us1")
    >>> result
       process_correlation_id  netflow.host_id  netflow.processcorrelationid.pid  netflow.processcorrelationid.timewindow  netflow.process_correlation_id  ...
    0      host123:1234:56789          host123                        1234:56789                                      NaN              host123:1234:56789  ...
    1                       1              NaN                               NaN                                      NaN                             NaN  ...
    2      host123:1234:56789          host123                              1234                                    56789              host123:1234:56789  ...
    """

    if df.empty:
        return df
    
    if not process_column:
        process_column = "process_correlation_id"
    
    if process_column not in df.columns:
        log.error(f"Column {process_column} not found in dataframe")
        return df
    
    merge_on = "process_correlation_id"
    
    if f"{NETFLOW}.{merge_on}" in df.columns:
        log.debug(f"Netflow columns already exist in DataFrame")
        return df

    service = get_service(tenant_id=tenant_id, environment=region)

    pids = set()
    pids.update(df[process_column].dropna().unique().tolist())
    pids = list(pids)

    results = []

    options = EventQueryOptions(
        timestamp_ascending=True,
        page_size=1000,
        max_rows=100000,
        aggregation_off=False,
    )
            
    # Retrieve netflow data that correlates with process data in batches. 
    template = jinja_env.get_template(NETFLOW_TEMPLATE)
    for chunk in chunk_list(pids, 100):
        netflow_correlation_ids = [NetflowCorrelationId(part[0], part[1], part[2]) for part in (pid.split(":") for pid in chunk)]
        
        query = template.render(netflow_correlation=netflow_correlation_ids)

        query_result = service.events.subscription.event_query(
            query=query,
            options=options,
            metadata={
                "callerName": CONFIG[QUERIES_SECTION].get(
                    "callername", fallback="Taegis Magic"
                    ),
                },
            )
        
        if not query_result:
            continue
        
        results.extend(query_result)
        next_page = get_next_page(query_result)

        while next_page:
            query_result = service.events.subscription.event_page(next_page)
            results.extend(query_result)
            next_page = get_next_page(query_result)
    
    if not results:
        log.debug("No results were returned from query.")
        return df

    netflow_df = to_dataframe(
        row
        for r in results
        if r.result and r.result.rows
        for row in r.result.rows
    )

    # Create a new column for full process_correlation_id to merge on
    has_colon = netflow_df['processcorrelationid.pid'].str.contains(':', na=False)
    netflow_df[f'{merge_on}'] = netflow_df['host_id'] + ":" + netflow_df['processcorrelationid.pid']
    netflow_df.loc[~has_colon, f'{merge_on}'] = (
        netflow_df['host_id'] + ":" + netflow_df['processcorrelationid.pid'] + ":" + netflow_df['processcorrelationid.timewindow']
    )

    netflow_df_with_new_col = netflow_df.add_prefix(f"{NETFLOW}.")
        
    merge_df = pd.merge(
        left=df,
        right=netflow_df_with_new_col,        
        left_on=process_column,
        right_on=f"{NETFLOW}.{merge_on}",
        how="left",
        suffixes=(None, ".correlate_netflow")
    )

    return merge_df

