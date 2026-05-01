import pandas as pd
import logging
from typing import Mapping, Optional
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
PROCESS_PIPE_TEMPLATE = "process_pipe_template.jinja"

NETFLOW_PIVOT_COLUMNS = ["host_id", "sensor_id", "sensor_tenant", "sensor_type", "tenant_id"]
HTTP_PIVOT_COLUMNS = ["host_id", "process_correlation_id", "sensor_id", "sensor_type", "tenant_id"]
AUTH_PIVOT_COLUMNS = ["host_id", "process_correlation_id", "sensor_id", "sensor_type", "tenant_id"]
DETECTIONFINDING_PIVOT_COLUMNS = ["host_id", "sensor_id", "sensor_tenant", "sensor_type", "tenant_id"]

NETFLOW = "netflow"
HTTP = "HTTP"
AUTH = "AUTH"
DETECTIONFINDING = "detectionfinding"

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
) -> pd.DataFrame:
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
    template = jinja_env.get_template(PROCESS_PIPE_TEMPLATE)
    for chunk in chunk_list(pids, 40):
        netflow_correlation_ids = [NetflowCorrelationId(part[0], part[1], part[2]) for part in (pid.split(":") for pid in chunk)]
        
        query = template.render(table=NETFLOW, filters=netflow_correlation_ids, earliest=f"-{earliest}")

        log.trace(query)

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
        print("No results were returned from query.")
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
    pivot_map: Optional[Mapping[str, str]] = None,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
    """Pivot aggregate process data into non-aggregate netflow event rows."""

    return _process_pivot_with_map(df, region, tenant_id, PROCESS_PIPE_TEMPLATE, NETFLOW, NETFLOW_PIVOT_COLUMNS, pivot_map, earliest)


def process_pivot_http(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    pivot_map: Optional[Mapping[str, str]] = None,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
    """Pivot aggregate process data into non-aggregate http event rows."""

    return _process_pivot_with_map(df, region, tenant_id, PROCESS_PIPE_TEMPLATE, HTTP, HTTP_PIVOT_COLUMNS, pivot_map, earliest)


def process_pivot_auth(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    pivot_map: Optional[Mapping[str, str]] = None,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
    """Pivot aggregate process data into non-aggregate http event rows."""

    return _process_pivot_with_map(df, region, tenant_id, PROCESS_PIPE_TEMPLATE, AUTH, AUTH_PIVOT_COLUMNS, pivot_map, earliest)


def process_pivot_detectionfinding(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    pivot_map: Optional[Mapping[str, str]] = None,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
    """Pivot aggregate process data into non-aggregate detectionfinding event rows."""

    return _process_pivot_with_map(df, region, tenant_id, PROCESS_PIPE_TEMPLATE, DETECTIONFINDING, DETECTIONFINDING_PIVOT_COLUMNS, pivot_map, earliest)


def _execute_pivot_subqueries(
    df: pd.DataFrame,
    unique_sub_queries: list[str],
    query_template: str,
    table: str,
    region: str,
    tenant_id: str,
    earliest: str,
) -> pd.DataFrame:
    """Helper function to execute queries for pivot functions"""

    template = jinja_env.get_template(query_template)
    service = get_service(environment=region, tenant_id=tenant_id)
    query_options = EventQueryOptions(
        timestamp_ascending=True,
        page_size=1000,
        max_rows=100000,
        aggregation_off=True,
    )

    results = []

    for chunk in chunk_list(unique_sub_queries, 100):

        query = template.render(table=table, filters=chunk, earliest=f"-{earliest}")

        log.trace(query)

        try:
            query_result = service.events.subscription.event_query(
                query=query,
                options=query_options,
                metadata={
                    "callerName": CONFIG[QUERIES_SECTION].get(
                        "callername", fallback="Taegis Magic"
                    ),
                },
            )
        except Exception as e:
            log.error(f"Encountered error when trying to execute query {query}. Error is {e}")
            return df

        if not query_result[0].result.rows:
            log.debug("No results were returned from query.")
            continue

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


def _process_pivot_base_func(
    df: pd.DataFrame,
    region: str,
    tenant_id: str,
    query_template: str,
    table: str,
    pivot_columns: list[str],
    earliest: str
) -> pd.DataFrame:
    """Base function template for process pivot functions.

    Due to the common nature of pivot functions, whose purpose is to take an input DataFrame with aggregate data and then query another table
    with where clauses based on the columns of the input DataFrame, this base function was created. 

    In this particular case, the input DataFrame contains aggregate process data. The function parses the input DataFrame and creates a list
    of WHERE clauses based on columns that exist in both the input DataFrame and the `pivot_columns` parameter. It will then execute a query
    against the table to `pivot` to (i.e. the `table` parameter) and the function will return whatever is returned by that query. So if a 
    pivot function is for process -> netflow, the input DataFrame will contains aggregate process data and what is returned is raw non-aggregate
    data from the netflow table based on the WHERE clauses. 

    Please note that the values in the pivot_columns list are columns that must exist in both the process table and the table to pivot to.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing aggregate process data.
    region : str,
        Taegis region.
    tenant_id : str,
        Tenant ID to query against.
    query_template: str,
        Name of Jinja template for querying desired table.
    table: str,
        Table to generate a query against, to be injected into Jinja query template. 
    pivot_columns: str,
        A list of columns that are found in both the `process` table and the `table` to execute queries against. This list will determine 
        the WHERE clauses that will get generated when executing the query against the `table parameter`.
    earliest : str,
        Date filter to apply when querying against netflow events. Based on Taegis Query language. A "-" will be prepended to whatever value is provided. 
    
    Returns
    -------
    pd.DataFrame
        DataFrame of raw netflow event rows matching the aggregate filters.


    Example
    -------

    The following example is for a process -> netflow pivot function. The example demonstrates what the input DataFrame is expected to look like,
    what `pivot_columns` parameter may look like, and then shows what kind of query the function will execute based on the contents of the input DataFrame. 

    >>> input_df
                                    host_id      sensor_type non_matching_column  count
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS               alpha    100
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS                beta    200
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL               gamma     50
    >>> # Example value of `pivot_columns` parameter
    >>> print(pivot_columns)

    ["host_id", "sensor_type"]

    -------------------------------------------------------------------------------------------------------
    For the `input_df` dataframe above, the generated query would look like:

        FROM netflow
        WHERE
            (host_id = '550e8400-e29b-41d4-a716-446655440001' AND sensor_type = 'ENDPOINT_SOPHOS') or 
            (host_id = '550e8400-e29b-41d4-a716-446655440002' AND sensor_type = 'ENDPOINT_TAEGIS') or 
            (host_id = '550e8400-e29b-41d4-a716-446655440003' AND sensor_type = 'FIREWALL')
        EARLIEST=-1d

    Notice how the `non_matching_column` column is not part of the WHERE clause. The result of this query is what is returned by calling the function. 

    """
    if df.empty:
        return df

    cols = [col for col in df.columns if col in pivot_columns]

    if not cols:
        log.error(
            f"DataFrame contains none of the expected pivot columns: {pivot_columns}"
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

    return _execute_pivot_subqueries(
        df,
        unique_sub_queries,
        query_template,
        table,
        region,
        tenant_id,
        earliest,
    )


def _process_pivot_with_map(
    df: pd.DataFrame,
    region: str,
    tenant_id: str,
    query_template: str,
    table: str,
    pivot_columns: list[str],
    pivot_map: Optional[Mapping[str, str]],
    earliest: str
) -> pd.DataFrame:
    """Works similarly to `_process_pivot_base_func`, but optional `pivot_map` contains keys that are column names in the input
    DataFrame and the corresponding values are column names in the table to pivot to. Therefore, the pivot_map remaps DataFrame
    DataFrame column names to target table column names in WHERE clauses.

    When `pivot_map` is None, `_process_pivot_base_func` is called.

    When `pivot_map` is provided, `pivot_columns` is ignored. Every key in `pivot_map` must exist as a column in the input DataFrame.
    Each key's corresponding value should be a column that exists in the table to pivot to, i.e. the `table` parameter. Whereas in 
    `_process_pivot_base_func` the where clauses are built using the column names and their row values directly from the input DataFrame,
    this function builds where clauses using the column names from the `pivot_map` and the row values are still from the input DataFrame.

    This function is being created to allow for additional "customization" to overcome the "limitations" of `_process_pivot_base_func`.
    `_process_pivot_base_func` only works with a static list of columns that must exist both in the `process` table and the table to 
    pivot to. If there is schema evolution, a mismatch in column names between `process` and target table with similar data, custom 
    column names etc. then the `_process_pivot_base_func` is not as useful. This function helps overcome these limitations 
    without adding new code. 


    Example
    -------

    The following example demonstrates what the input DataFrame is expected to look like, what `pivot_map` parameter may look like,
    and then shows what kind of query the function will execute based on the contents of the input DataFrame and `pivot_map`. 
    This is just meant for demonstration purposes, the column names may or may not be real. 

    >>> # Remember, input_df contains aggregate process info. 
    >>> input_df
                                    host_id      sensor_type                 env  count
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS               alpha    100
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS                beta    200
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL               gamma     50
    >>> # Example value of `pivot_map` parameter
    >>> print(pivot_map)
    {
        "sensor_type": "sensor_type",
        "env": "region"
    }

    -------------------------------------------------------------------------------------------------------
    For the `input_df` dataframe above, the generated query would look like:

        FROM netflow
        WHERE
            (sensor_type = 'ENDPOINT_SOPHOS' AND region = 'alpha') or 
            (sensor_type = 'ENDPOINT_TAEGIS' AND region = 'beta') or 
            (sensor_type = 'FIREWALL' AND region = 'gamma')
        EARLIEST=-1d

    Notice how the `pivot_map` did not include `host_id` and therefore `host_id` was not included in the query. In addition, notice how
    the values for `env` column got remapped to `region` in the query.  


    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing aggregate process data.
    region : str,
        Taegis region.
    tenant_id : str,
        Tenant ID to query against.
    query_template: str,
        Name of Jinja template for querying desired table.
    table: str,
        Table to generate a query against, to be injected into Jinja query template. 
    pivot_columns: str,
        A list of columns that are found in both the `process` table and the `table` to execute queries against. This list will determine 
        the WHERE clauses that will get generated when executing the query against the `table parameter`.
    earliest : str,
        Date filter to apply when querying against netflow events. Based on Taegis Query language. A "-" will be prepended to whatever value is provided. 
    pivot_map : Mapping[str, str] | None
        Maps input DataFrame column names to target table column names for query filters.
    """

    if pivot_map is None:
        return _process_pivot_base_func(
            df, region, tenant_id, query_template, table, pivot_columns, earliest
        )

    if df.empty:
        return df

    cols = list(pivot_map.keys())
    if not cols:
        log.error("pivot_map is empty; no columns to pivot on")
        return df

    missing_keys = [k for k in cols if k not in df.columns]
    if missing_keys:
        log.error(
            f"DataFrame is missing columns required by pivot_map keys: {missing_keys}"
        )
        return df

    single_quote = "'"
    replacement = "\\'"

    sub_queries = []
    for _, row in df.iterrows():
        row_filters = []
        for col in cols:
            if col not in row or pd.isna(row[col]):
                continue
            table_col = pivot_map[col]
            val = row[col]
            if not str(val).find("'") > -1:
                row_filters.append(f"{table_col} = '{val}'")
            else:
                row_filters.append(
                    f"{table_col} = e'{str(val).replace(single_quote, replacement)}'"
                )
        if row_filters:
            sub_queries.append("(" + " AND ".join(row_filters) + ")")

    unique_sub_queries = list(dict.fromkeys(sub_queries))

    if not unique_sub_queries:
        raise ValueError(
            "No sub-queries could be built from the DataFrame. "
            "Ensure the DataFrame contains non-null values in one or more of the following columns: "
            f"{cols}"
        )

    return _execute_pivot_subqueries(
        df,
        unique_sub_queries,
        query_template,
        table,
        region,
        tenant_id,
        earliest,
    )

