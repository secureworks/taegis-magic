import pandas as pd
import logging
from typing import Optional
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
NETFLOW_TEMPLATE = "process_netflow_pipe.jinja"
NETFLOW_PIVOT_COLUMNS = ["host_id", "sensor_id", "sensor_type", "sensor_tenant",  "tenant_id"]

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
    *,
    region: str,
    tenant_id: str,
    process_column: Optional[str] = "process_correlation_id",
    earliest: Optional[str] = "1d"
):
    """Correlate process data with netflow information.

    Input DataFrame is expected to have a column whose row values contain process_correlation_ids
    that have the format of {host_id}:{process_id}:{time_window}.

    All parameters are keyword-only.

    Parameters
    ----------
    df : pd.DataFrame   
        Dataframe containing process data.
    region : str
        Taegis Region.
    tenant_id : str
        Tenant ID to use for the correlation.
    process_column : Optional[str], optional
        Process column to lookup in input DataFrame, by default "process_correlation_id".
    earliest : Optional[str], default "1d"
        Date filter to apply when querying against netflow events to correlate with process data. Based on Taegis Query language. A "-" will be prepended to whatever value is provided. 

    Returns
    -------
    pd.DataFrame
        A new Dataframe with correlated netflow data. New columns will be prepended with 'netflow'.

    Example
    -------
    >>> import pandas as pd
    >>> df = pd.DataFrame({"process_correlation_id": ["host123:1234:56789", 1, "host123:1234:56789"]})
    >>> df
       process_correlation_id
    0      host123:1234:56789
    1                       1
    2      host123:1234:56789
    >>> result = process_correlate_netflow(df=df, region="us1", tenant_id="12345")
    >>> result
       process_correlation_id  netflow.host_id  netflow.processcorrelationid.pid  netflow.processcorrelationid.timewindow  netflow.process_correlation_id  ...
    0      host123:1234:56789          host123                        1234:56789                                      NaN              host123:1234:56789  ...
    1                       1              NaN                               NaN                                      NaN                             NaN  ...
    2      host123:1234:56789          host123                              1234                                    56789              host123:1234:56789  ...
    """

    if df.empty:
        return df
    
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
        
        query = template.render(netflow_correlation=netflow_correlation_ids,earliest=f"-{earliest}")

        query_result = service.events.subscription.event_query(
            query=query,
            options=options,
            metadata={
                "callerName": CONFIG[QUERIES_SECTION].get(
                    "callername", fallback="Taegis Magic"
                    ),
                },
            )
        
        # query_result is non-empty even if no rows are returned, so can't just do `if not query_result`
        if not query_result[0].result.rows:
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


def process_pivot_netflow(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
    """Pivot aggregate process data into non-aggregate netflow event rows.

    The input DataFrame is expected to contain aggregate process data. Columns present in the 
    DataFrame and a static pivot column list are used to build per-row sub-query filters,
    which are then combined with OR logic and issued as a single query against netflow

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing aggregate process data.
    region : str,
        Taegis region.
    tenant_id : str,
        Tenant ID to query against.
    earliest : str, optional
        Date filter to apply when querying against netflow events. Based on Taegis Query language. A "-" will be prepended to whatever value is provided. 

    Returns
    -------
    pd.DataFrame
        DataFrame of raw netflow event rows matching the aggregate filters.

    Example
    -------
    Input DataFrame with aggregate process info include columns that are not in the static pivot list (``NETFLOW_PIVOT_COLUMNS``).
    Only intersecting columns are turned into ``WHERE`` filters, other columns are ignored. 

    >>> import pandas as pd
    >>> input_df = pd.DataFrame({
    ...     "host_id": [
    ...         "550e8400-e29b-41d4-a716-446655440001",
    ...         "550e8400-e29b-41d4-a716-446655440002",
    ...         "550e8400-e29b-41d4-a716-446655440003",
    ...     ],
    ...     "sensor_type": ["ENDPOINT_SOPHOS", "ENDPOINT_TAEGIS", "FIREWALL"],
    ...     "non_matching_column": ["alpha", "beta", "gamma"],
    ...     "count": [100, 200, 50],
    ... })
    >>> input_df
                                    host_id      sensor_type non_matching_column  count
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS               alpha    100
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS                beta    200
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL               gamma     50
    >>> # Calling the function
    >>> result = process_pivot_netflow(input_df, region="us1", tenant_id="12345")
    >>> # Calling the function via pipe
    >>> result = input_df.pipe(process_pivot_netflow, region="us1", tenant_id="12345")
    >>> # Raw netflow rows include many fields; a subset might look like:
    >>> result[
    ...     [
    ...         "host_id",
    ...         "sensor_type",
    ...         "source_address",
    ...         "destination_address",
    ...         "destination_port",
    ...         "source_port",
    ...         "direction",
    ...         "protocol",
    ...     ]
    ... ]
                                    host_id      sensor_type source_address destination_address  destination_port  source_port  direction  protocol
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS   172.16.16.10       20.189.173.18               443         52773  OUTBOUND         6
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS   172.16.16.10       20.189.173.18               443         52773  OUTBOUND         6
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL   192.168.0.50       93.184.216.34                80         49152   INBOUND         6

    -------------------------------------------------------------------------------------------------------
    For the dataframe above, the generated query would look like:

        FROM netflow
        WHERE
            (host_id = '550e8400-e29b-41d4-a716-446655440001' AND sensor_type = 'ENDPOINT_SOPHOS') or 
            (host_id = '550e8400-e29b-41d4-a716-446655440002' AND sensor_type = 'ENDPOINT_TAEGIS') or 
            (host_id = '550e8400-e29b-41d4-a716-446655440003' AND sensor_type = 'FIREWALL')
        EARLIEST=-1d

    Notice how the ``non_matching_column`` column is not part of the WHERE clause. 

    """

    if df.empty:
        return df

    cols = [col for col in df.columns if col in NETFLOW_PIVOT_COLUMNS]

    if not cols:
        log.error(
            f"DataFrame contains none of the expected pivot columns: {NETFLOW_PIVOT_COLUMNS}"
        )
        return df

    single_quote = "'"
    replacement = "\\'"

    sub_queries = []
    for _, row in df.iterrows():
        row_filters = [
            (
                f"{col} = '{row[col]}'"
                if not str(row[col]).find("'") > -1
                else f"{col} = e'{str(row[col]).replace(single_quote, replacement)}'"
            )
            for col in cols
            if col in row and pd.notna(row[col])
        ]
        if row_filters:
            sub_queries.append( "(" + " AND ".join(row_filters) + ")" )

    unique_sub_queries = list(dict.fromkeys(sub_queries))

    if not unique_sub_queries:
        raise ValueError(
            "No sub-queries could be built from the DataFrame. "
            "Ensure the DataFrame contains non-null values in one or more of the following columns: "
            f"{NETFLOW_PIVOT_COLUMNS}"
        )

    template = jinja_env.get_template(NETFLOW_TEMPLATE)
    service = get_service(environment=region, tenant_id=tenant_id)
    query_options = EventQueryOptions(
        timestamp_ascending=True,
        page_size=1000,
        max_rows=100000,
        aggregation_off=True,
    )

    results = []

    for chunk in chunk_list(unique_sub_queries, 100):

        query = template.render(netflow_correlation=chunk, earliest=f"-{earliest}")
        log.debug(query)
    
        query_result = service.events.subscription.event_query(
            query=query,
            options=query_options,
            metadata={
                "callerName": CONFIG[QUERIES_SECTION].get(
                    "callername", fallback="Taegis Magic"
                ),
            },
        )

        if not query_result[0].result.rows:
            log.debug("No results were returned from process_pivot_netflow query.")
            return df

        results.extend(query_result)
        next_page = get_next_page(query_result)

        while next_page:
            query_result = service.events.subscription.event_page(next_page)
            results.extend(query_result)
            next_page = get_next_page(query_result)

    if not results:
        log.info("No results were returned from query.")
        return df

    return to_dataframe(
        row
        for r in results
        if r.result and r.result.rows
        for row in r.result.rows
    )

