import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from typing import Callable, List, Optional

import pandas as pd
from taegis_magic.core.log import tracing
from taegis_magic.core.utils import get_first_tenant_id
from taegis_magic.pandas.tenants import lookup_first_environment

from taegis_sdk_python import GraphQLService
from taegis_sdk_python.services.multi_tenant_ioc.types import (
    EventAggregationArguments,
    EventCountResult,
    LogicalType,
    LogicalTypeFilter,
    Operator,
    TenantsInput,
)

log = logging.getLogger(__name__)


@tracing
def normalize_event_count_results(input_: List[EventCountResult]) -> pd.DataFrame:
    """Normalize event count results into a DataFrame.

    Parameters
    ----------
    input_ : List[EventCountResult]

    Returns
    -------
    pd.DataFrame
    """
    df = pd.json_normalize(
        [
            asdict(result)
            for event_count_result in input_ or []
            for result in event_count_result.results or []
        ],
    )
    if "counts_by_tenant" in df.columns:
        df = df.explode("counts_by_tenant")
        df["counts_by_tenant.tenant_id"] = df["counts_by_tenant"].str.get("tenant_id")
        df["counts_by_tenant.count"] = df["counts_by_tenant"].str.get("count")
    else:
        log.warning('"No counts_by_tenant in results, skipping explode operation.')
    return df


@tracing
def is_domain(value: str) -> bool:
    """Check for a valid domain name.

    Parameters
    ----------
    value : str
        Value to validate.

    Returns
    -------
    bool
    """
    domain_regex = r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)\.[A-Za-z]{2,}$"
    return re.match(domain_regex, value) is not None


@tracing
def is_hash(value: str) -> bool:
    """Check for a valid hash value.

    Parameters
    ----------
    value : str
        Value to validate.

    Returns
    -------
    bool
    """

    def is_md5_hash(value: str) -> bool:
        return re.match(r"^[A-Fa-f0-9]{32}$", value) is not None

    def is_sha1_hash(value: str) -> bool:
        return re.match(r"^[A-Fa-f0-9]{40}$", value) is not None

    def is_sha256_hash(value: str) -> bool:
        return re.match(r"^[A-Fa-f0-9]{64}$", value) is not None

    return is_md5_hash(value) or is_sha1_hash(value) or is_sha256_hash(value)


@tracing
def is_ip_address(value: str) -> bool:
    """Check for valid IP address.

    Parameters
    ----------
    value : str
        Value to validate.

    Returns
    -------
    bool
    """
    try:
        ip_address(value)
        return True
    except ValueError:
        return False


@tracing
def event_count_by_logical_type_paginated(
    service: GraphQLService,
    *,
    arguments: EventAggregationArguments,
) -> List[EventCountResult]:
    """Get paginated event count by logical type.

    Parameters
    ----------
    service : GraphQLService
        The GraphQL service instance.
    arguments : EventAggregationArguments
        Arguments for the event count query.

    Returns
    -------
    List[EventCountResult]
        List of event count results.
    """
    pages = []
    results = service.multi_tenant_ioc.query.event_count_by_logical_type(
        arguments=arguments
    )
    pages.append(results)
    while results.next:
        results = service.multi_tenant_ioc.query.event_count_page(
            next_token=results.next
        )
        pages.append(results)

    return pages


@tracing
def search_ioc_domains(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
) -> Optional[List[EventCountResult]]:
    """Search for domain IOCs.

    Parameters
    ----------
    service : GraphQLService
    iocs : List[str]
        List of IOCs.
    days : int, optional
        Days back to search, by default 30
    tenant_ids : Optional[List[str]], optional
        Taegis Tenants IDs, by default None

    Returns
    -------
    Optional[List[EventCountResult]]
    """
    days += 1  # add 1 to days due to search failure on current day

    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    values = [ioc for ioc in iocs if is_domain(ioc)]

    if not values:
        log.error("No valid domains found in the provided IOCs.")
        return None

    arguments = EventAggregationArguments(
        logical_type_filter=LogicalTypeFilter(
            logical_type=LogicalType.DOMAIN, values=values, operator=Operator.EQUALS
        ),
        tenants_context=TenantsInput(tenant_ids=tenant_ids),
        earliest=(datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d"),
        latest=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
    )
    results = event_count_by_logical_type_paginated(
        service=service,
        arguments=arguments,
    )

    return results


@tracing
def search_ioc_file_hashes(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
) -> Optional[EventCountResult]:
    """Search for File Hash IOCs.

    Parameters
    ----------
    service : GraphQLService
    iocs : List[str]
        List of IOCs.
    days : int, optional
        Days back to search, by default 30
    tenant_ids : Optional[List[str]], optional
        Taegis Tenants IDs, by default None

    Returns
    -------
    Optional[List[EventCountResult]]
    """
    days += 1  # add 1 to days due to search failure on current day

    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    values = [ioc for ioc in iocs if is_hash(ioc)]

    if not values:
        log.error("No valid hashes found in the provided IOCs.")
        return None

    arguments = EventAggregationArguments(
        logical_type_filter=LogicalTypeFilter(
            logical_type=LogicalType.HASH, values=values, operator=Operator.EQUALS
        ),
        tenants_context=TenantsInput(tenant_ids=tenant_ids),
        earliest=(datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d"),
        latest=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
    )
    results = event_count_by_logical_type_paginated(
        service=service,
        arguments=arguments,
    )

    return results


@tracing
def search_ioc_ip_addresses(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
) -> Optional[EventCountResult]:
    """Search for IP Address IOCs.

    Parameters
    ----------
    service : GraphQLService
    iocs : List[str]
        List of IOCs.
    days : int, optional
        Days back to search, by default 30
    tenant_ids : Optional[List[str]], optional
        Taegis Tenants IDs, by default None

    Returns
    -------
    Optional[List[EventCountResult]]
    """
    days += 1  # add 1 to days due to search failure on current day

    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    values = [ioc for ioc in iocs if is_ip_address(ioc)]

    if not values:
        log.error("No valid IP addresses found in the provided IOCs.")
        return None

    arguments = EventAggregationArguments(
        logical_type_filter=LogicalTypeFilter(
            logical_type=LogicalType.IP, values=values, operator=Operator.EQUALS
        ),
        tenants_context=TenantsInput(tenant_ids=tenant_ids),
        earliest=(datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d"),
        latest=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
    )
    results = event_count_by_logical_type_paginated(
        service=service,
        arguments=arguments,
    )

    return results


@tracing
def multi_tenant_ioc_search(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
    search_functions: Optional[Callable] = None,
) -> List[EventCountResult]:
    """Search for IOCs across multiple tenants.

    Parameters
    ----------
    service : GraphQLService
        The GraphQL service instance.
    iocs : List[str]
        List of IOCs to search for.
    days : int, optional
        Number of days to look back, by default 30.
    tenant_ids : Optional[List[str]], optional
        List of tenant IDs to search in, by default None.

    Returns
    -------
    Optional[List[EventCountResult]]
        List of event count results or None if no valid IOCs are found.
    """
    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    if not iocs:
        log.error("No IOCs provided for search.")
        return None

    results = []

    if not search_functions:
        search_functions = [
            search_ioc_domains,
            search_ioc_file_hashes,
            search_ioc_ip_addresses,
        ]

    for search_function in search_functions:
        if not callable(search_function):
            log.error(f"Search function {search_function} is not callable.")
            continue

        try:
            search_results = search_function(
                service=service, iocs=iocs, days=days, tenant_ids=tenant_ids
            )
            if search_results:
                results.extend(search_results)
        except Exception as e:
            log.error(f"Error during IOC search with {search_function.__name__}: {e}")

    return results


@tracing
def regioned_multi_tenant_ioc_search(
    service: GraphQLService,
    *,
    region: str,
    tenants: pd.DataFrame,
    iocs: List[str],
    days: int = 30,
    search_functions: Optional[Callable] = None,
) -> List[EventCountResult]:
    """Search for IOCs across multiple tenants by region.

    Parameters
    ----------
    service : GraphQLService
        The GraphQL service instance.
    region : str
        The Taegis region to search in.
    iocs : List[str]
        List of IOCs to search for.
    days : int, optional
        Number of days to look back, by default 30.
    tenant_ids : Optional[List[str]], optional
        List of tenant IDs to search in, by default None.

    Returns
    -------
    Optional[List[EventCountResult]]
        List of event count results or None if no valid IOCs are found.
    """
    if not "first_environment" in tenants.columns:
        log.error(
            "Tenants DataFrame must contain 'first_environment' column to filter by region."
        )
        return []

    with service(environment=region):
        tenant_ids = tenants[tenants["first_environment"] == region]["id"].tolist()

        return multi_tenant_ioc_search(
            service=service,
            iocs=iocs,
            days=days,
            tenant_ids=tenant_ids,
            search_functions=search_functions,
        )


@tracing
def threaded_multi_tenant_ioc_search(
    service: GraphQLService,
    *,
    tenants: pd.DataFrame,
    iocs: List[str],
    days: int = 30,
    search_functions: Optional[Callable] = None,
    **kwargs,
) -> pd.DataFrame:
    """Perform a multi-tenant IOC search using threading.

    Parameters
    ----------
    service : GraphQLService
        The GraphQL service instance.
    tenants : pd.DataFrame
        DataFrame containing tenant information.
    iocs : List[str]
        List of IOCs to search for.
    days : int, optional
        Number of days to look back, by default 30.
    search_functions : Optional[Callable], optional
        Custom search functions to use, by default None.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the IOC search.
    """
    if tenants.empty:
        log.error("No tenants provided for search.")
        return pd.DataFrame()

    if "first_environment" in tenants.columns:
        regions = tenants["first_environment"].unique().tolist()
    elif "environment" in tenants.columns:
        tenants["first_environment"] = tenants.pipe(lookup_first_environment)
        regions = tenants["first_environment"].unique().tolist()
    else:
        log.error(
            "Tenants DataFrame must contain 'first_environment' or 'environment' column."
        )
        return pd.DataFrame()

    if service.use_universal_authentication is False:
        for region in regions:
            with service(environment=region):
                service.access_token
    else:
        service.access_token

    future_results = {}

    with ThreadPoolExecutor(**kwargs) as executor:
        futures = {
            executor.submit(
                regioned_multi_tenant_ioc_search,
                service=service,
                region=region,
                tenants=tenants,
                iocs=iocs,
                days=days,
                search_functions=search_functions,
            ): region
            for region in regions
        }

        log.debug("Waiting for futures to complete...")
        for future in as_completed(futures):
            # stitch results per region
            region = futures[future]
            future_results[region] = future.result()
        log.debug("All futures completed.")

    log.debug("Normalizing results...")
    normalized_results = []
    for region, results in future_results.items():
        normalized_result = normalize_event_count_results(results)
        normalized_result["region"] = region
        normalized_results.append(normalized_result)
    log.debug("Results normalized.")

    if not normalized_results:
        log.error("No results found after normalization.")
        return pd.DataFrame()

    stiched_results = pd.concat(
        normalized_results,
        axis=1,
    ).reset_index(drop=True)

    return stiched_results
