import logging

from ipaddress import ip_address
import re

from typing import List, Optional

from datetime import datetime, timedelta, timezone

from taegis_sdk_python import GraphQLService
from taegis_sdk_python.services.multi_tenant_ioc.types import (
    EventAggregationArguments,
    LogicalTypeFilter,
    LogicalType,
    TenantsInput,
    Operator,
    EventCountResult,
)

from taegis_magic.core.utils import get_first_tenant_id

log = logging.getLogger(__name__)


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


def search_ioc_domains(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
) -> Optional[List[EventCountResult]]:
    days += 1  # add 1 to days due to search failure on current day

    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    values = [ioc for ioc in iocs if is_domain(ioc)]

    if not values:
        log.error("No valid domains found in the provided IOCs.")
        return None

    pages = []

    arguments = EventAggregationArguments(
        logical_type_filter=LogicalTypeFilter(
            logical_type=LogicalType.DOMAIN, values=values, operator=Operator.EQUALS
        ),
        tenants_context=TenantsInput(tenant_ids=tenant_ids),
        earliest=(datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d"),
        latest=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
    )
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


def search_ioc_file_hashes(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
) -> Optional[EventCountResult]:
    days += 1  # add 1 to days due to search failure on current day

    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    values = [ioc for ioc in iocs if is_hash(ioc)]

    if not values:
        log.error("No valid hashes found in the provided IOCs.")
        return None

    pages = []

    arguments = EventAggregationArguments(
        logical_type_filter=LogicalTypeFilter(
            logical_type=LogicalType.HASH, values=values, operator=Operator.EQUALS
        ),
        tenants_context=TenantsInput(tenant_ids=tenant_ids),
        earliest=(datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d"),
        latest=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
    )
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


def search_ioc_ip_addresses(
    service: GraphQLService,
    *,
    iocs: List[str],
    days: int = 30,
    tenant_ids: Optional[List[str]] = None,
) -> Optional[EventCountResult]:
    days += 1  # add 1 to days due to search failure on current day

    if not tenant_ids:
        tenant_ids = [get_first_tenant_id(service)]

    values = [ioc for ioc in iocs if is_ip_address(ioc)]

    if not values:
        log.error("No valid IP addresses found in the provided IOCs.")
        return None

    pages = []

    arguments = EventAggregationArguments(
        logical_type_filter=LogicalTypeFilter(
            logical_type=LogicalType.IP, values=values, operator=Operator.EQUALS
        ),
        tenants_context=TenantsInput(tenant_ids=tenant_ids),
        earliest=(datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d"),
        latest=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
    )
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
