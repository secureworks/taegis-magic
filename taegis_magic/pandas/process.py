import pandas as pd
import logging
from typing import List, Optional
from dataclasses import asdict
from taegis_magic.core.service import get_service
from taegis_magic.pandas.utils import chunk_df
from dataclasses import dataclass
from taegis_magic.core.utils import to_dataframe

from jinja2 import Template, Environment, PackageLoader

from taegis_sdk_python.services.events.types import (
    Event,
    EventQueryOptions,
    EventQueryResults,
)
from taegis_sdk_python.config import get_config
from taegis_magic.commands.configure import QUERIES_SECTION
from taegis_magic.commands.events import get_next_page

log = logging.getLogger(__name__)
jinja_env = Environment(loader=PackageLoader("taegis-magic","templates/process"))
NETFLOW_TEMPLATE = "NetflowCorrelationID.jinja"

@dataclass
class NetflowCorrelationId:
    host_id: str
    pid: str

    def __str__(self):
        return f"(processcorrelationid.pid = {self.pid} AND host_id = {self.host_id})"


CONFIG = get_config()
if not CONFIG.has_section(QUERIES_SECTION):
    CONFIG.add_section(QUERIES_SECTION)

def process_correlate_netflow(
    df: pd.DataFrame,
    region: str,
    tenant_id: Optional[str] = None,
    process_column: Optional[List[str]] = None,
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
    process_column : Optional[str], optional
        Process column to lookup in input DataFrame, by default process_correlation_id

    Returns
    -------
    pd.DataFrame
        Dataframe with correlated netflow data
    """

    if df.empty:
        return df
    
    if not process_column:
        process_column = "process_correlation_id"
    
    for pc in process_column:
        if pc not in df.columns:
            log.error(f"Column {pc} not found in dataframe")
            process_column.remove(pc)
    
    if not process_column:
        log.error("No valid process columns found in dataframe")
        return df
    
    merge_on = "process_correlation_id"

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
    for chunk in chunk_df(pids, 100):
        netflow_correlation_ids = [NetflowCorrelationId(part[0], f"{part[1]}:{part[2]}") for pid in chunk for part in pid.split(':')]
        
        query = template.render(netflow_correlation_ids)

        result = results.append(
            service.events.subscription.event_query(
            query=query,
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
    
    netflow_df = to_dataframe([asdict(p) for p in results])

    if netflow_df.empty:
        log.warning("No netflow events found for process data.")

    # Create a new column for full process_correlation_id to merge on
    netflow_df[f'{merge_on}'] = netflow_df['host_id'] + ":" + netflow_df['processcorrelationid.pid']

    df_copy = df.copy()
    pid_df_copy = netflow_df.copy().add_prefix(f"{process_column}")

    merge_df = pd.merge(
        left=df_copy,
        right=pid_df_copy,        
        left_on=process_column,
        right_on=merge_on,
        how="left",
        suffixes=(None, ".correlate_netflow")
    )

    return merge_df

